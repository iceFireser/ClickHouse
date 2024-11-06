#include <algorithm>
#include <ranges>
#include <cmath>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/ReverseTransform.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageValues.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <base/getThreadId.h>
#include <base/range.h>
#include <boost/range/algorithm_ext/erase.hpp>
#include <Common/CurrentMetrics.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/HashTable/HashMap.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>


namespace ProfileEvents
{
    extern const Event StorageBufferFlush;
    extern const Event StorageBufferErrorOnFlush;
    extern const Event StorageBufferPassedAllMinThresholds;
    extern const Event StorageBufferPassedTimeMaxThreshold;
    extern const Event StorageBufferPassedRowsMaxThreshold;
    extern const Event StorageBufferPassedBytesMaxThreshold;
    extern const Event StorageBufferPassedTimeFlushThreshold;
    extern const Event StorageBufferPassedRowsFlushThreshold;
    extern const Event StorageBufferPassedBytesFlushThreshold;
    extern const Event StorageBufferLayerLockReadersWaitMilliseconds;
    extern const Event StorageBufferLayerLockWritersWaitMilliseconds;
}

namespace CurrentMetrics
{
    extern const Metric StorageBufferRows;
    extern const Metric StorageBufferBytes;
    extern const Metric StorageBufferFlushThreads;
    extern const Metric StorageBufferFlushThreadsActive;
    extern const Metric StorageBufferFlushThreadsScheduled;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int INFINITE_LOOP;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int INCORRECT_FILE_NAME;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_ENOUGH_SPACE;
    extern const int TOO_MANY_PARTS;
    extern const int TOO_SLOW;
}


std::unique_lock<std::mutex> StorageBuffer::Buffer::lockForReading() const
{
    return lockImpl(/* read= */true);
}
std::unique_lock<std::mutex> StorageBuffer::Buffer::lockForWriting() const
{
    return lockImpl(/* read= */false);
}
std::unique_lock<std::mutex> StorageBuffer::Buffer::tryLock() const
{
    std::unique_lock lock(mutex, std::try_to_lock);
    return lock;
}
std::unique_lock<std::mutex> StorageBuffer::Buffer::lockImpl(bool read) const
{
    std::unique_lock lock(mutex, std::defer_lock);

    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    lock.lock();
    UInt64 elapsed = watch.elapsedMilliseconds();

    if (read)
        ProfileEvents::increment(ProfileEvents::StorageBufferLayerLockReadersWaitMilliseconds, elapsed);
    else
        ProfileEvents::increment(ProfileEvents::StorageBufferLayerLockWritersWaitMilliseconds, elapsed);

    return lock;
}

size_t StorageBuffer::Buffer::bufferRows() const
{
    size_t rows = 0;
    for (auto & [partitition_id, block_and_offset] : data_map)
        rows += block_and_offset.block.rows();

    return rows;
}

size_t StorageBuffer::Buffer::bufferBytes() const
{
    size_t bytes = 0;
    for (auto & [partitition_id, block_and_offset] : data_map)
        bytes += block_and_offset.block.bytes();

    return bytes;
}

size_t StorageBuffer::Buffer::bufferAllocatedBytes() const
{
    size_t bytes = 0;
    for (auto & [partitition_id, block_and_offset] : data_map)
        bytes += block_and_offset.block.allocatedBytes();

    return bytes;
}


StoragePtr StorageBuffer::getDestinationTable() const
{
    if (!destination_id)
        return {};

    auto destination = DatabaseCatalog::instance().tryGetTable(destination_id, getContext());
    if (destination.get() == this)
        throw Exception(ErrorCodes::INFINITE_LOOP, "Destination table is myself. Will lead to infinite loop.");

    return destination;
}


StorageBuffer::StorageBuffer(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    size_t num_shards_,
    const Thresholds & min_thresholds_,
    const Thresholds & max_thresholds_,
    const Thresholds & flush_thresholds_,
    const StorageID & destination_id_,
    bool allow_materialized_,
    const String & relative_data_path_,
    StoragePolicyPtr storage_policy_,
    const BufferSettings & buffer_settings_)
    : IStorage(table_id_)
    , WithContext(context_->getBufferContext())
    , num_shards(num_shards_)
    , buffers(num_shards_)
    , min_thresholds(min_thresholds_)
    , max_thresholds(max_thresholds_)
    , flush_thresholds(flush_thresholds_)
    , destination_id(destination_id_)
    , allow_materialized(allow_materialized_)
    , storage_policy(std::move(storage_policy_))
    , buffer_settings(buffer_settings_)
    , log(getLogger("StorageBuffer (" + table_id_.getFullTableName() + ")"))
    , bg_pool(getContext()->getBufferFlushSchedulePool())
{
    initializeDirectoriesAndFormatVersion(relative_data_path_, false, true);
    load_buffer_wal();

    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        auto dest_table = DatabaseCatalog::instance().getTable(destination_id, context_);
        storage_metadata.setColumns(dest_table->getInMemoryMetadataPtr()->getColumns());
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    if (num_shards > 1)
    {
        flush_pool = std::make_unique<ThreadPool>(
            CurrentMetrics::StorageBufferFlushThreads,
            CurrentMetrics::StorageBufferFlushThreadsActive,
            CurrentMetrics::StorageBufferFlushThreadsScheduled,
            num_shards,
            0,
            num_shards);
    }
    flush_handle = bg_pool.createTask(log->name() + "/Bg", [this] { backgroundFlush(); });
}

static void appendBlock(const LoggerPtr & log, const Block & from, const size_t from_start_rows, Block & to)
{
    if (from.rows() <= from_start_rows)
        return;

    const size_t rows = from.rows() - from_start_rows;
    const size_t old_rows = to.rows();

    if (!to)
        to = from.cloneEmpty();

    assertBlocksHaveEqualStructure(from, to, "Buffer");

    from.checkNumberOfRows();
    to.checkNumberOfRows();

    MutableColumnPtr last_col;
    try
    {
        MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

        for (size_t column_no = 0, columns = to.columns(); column_no < columns; ++column_no)
        {
            const IColumn & col_from = *from.getByPosition(column_no).column.get();
            {
                /// Usually IColumn::mutate() here will simply move pointers,
                /// however in case of parallel reading from it via SELECT, it
                /// is possible for the full IColumn::clone() here, and in this
                /// case it may fail due to MEMORY_LIMIT_EXCEEDED, and this
                /// breaks the rollback, since the column got lost, it is
                /// neither in last_col nor in "to" block.
                ///
                /// The safest option here, is to do a full clone every time,
                /// however, it is overhead. And it looks like the only
                /// exception that is possible here is MEMORY_LIMIT_EXCEEDED,
                /// and it is better to simply suppress it, to avoid overhead
                /// for every INSERT into Buffer (Anyway we have a
                /// LOGICAL_ERROR in rollback that will bail if something else
                /// will happens here).
                LockMemoryExceptionInThread temporarily_ignore_any_memory_limits(VariableContext::Global);
                last_col = IColumn::mutate(std::move(to.getByPosition(column_no).column));
            }

            /// In case of ColumnAggregateFunction aggregate states will
            /// be allocated from the query context but can be destroyed from the
            /// server context (in case of background flush), and thus memory
            /// will be leaked from the query, but only tracked memory, not
            /// memory itself.
            ///
            /// To avoid this, prohibit sharing the aggregate states.
            last_col->ensureOwnership();
            last_col->insertRangeFrom(col_from, from_start_rows, rows);

            {
                DENY_ALLOCATIONS_IN_SCOPE;
                to.getByPosition(column_no).column = std::move(last_col);
            }
        }
    }
    catch (...)
    {
        /// Rollback changes.

        /// In case of rollback, it is better to ignore memory limits instead of abnormal server termination.
        /// So ignore any memory limits, even global (since memory tracking has drift).
        LockMemoryExceptionInThread temporarily_ignore_any_memory_limits(VariableContext::Global);

        /// But first log exception to get more details in case of LOGICAL_ERROR
        tryLogCurrentException(log, "Caught exception while adding data to buffer, rolling back...");

        try
        {
            for (size_t column_no = 0, columns = to.columns(); column_no < columns; ++column_no)
            {
                ColumnPtr & col_to = to.getByPosition(column_no).column;
                /// If there is no column, then the exception was thrown in the middle of append, in the insertRangeFrom()
                if (!col_to)
                {
                    col_to = std::move(last_col);
                    /// Suppress clang-tidy [bugprone-use-after-move]
                    last_col = {};
                }
                /// But if there is still nothing, abort
                if (!col_to)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "No column to rollback");
                if (col_to->size() != old_rows)
                    col_to = col_to->cut(0, old_rows);
            }
        }
        catch (...)
        {
            /// In case when we cannot rollback, do not leave incorrect state in memory.
            std::terminate();
        }

        throw;
    }
}

/// Reads from one buffer (from one block) under its mutex.
class BufferSource : public ISource
{
public:
    BufferSource(const Names & column_names_, StorageBuffer::Buffer & buffer_, const StorageSnapshotPtr & storage_snapshot, StorageBuffer & storage_)
        : ISource(storage_snapshot->getSampleBlockForColumns(column_names_))
        , column_names_and_types(
              storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), column_names_))
        , buffer(buffer_)
        , metadata_version(storage_snapshot->metadata->metadata_version)
        , storage(storage_)
    {
    }

    String getName() const override { return "Buffer"; }

protected:
    Chunk generate() override
    {
        Chunk res;

        if (has_been_read)
            return res;
        has_been_read = true;

        std::unique_lock lock(buffer.lockForReading());

        if (!buffer.bufferRows() || buffer.metadata_version != metadata_version)
            return res;

        Columns columns;
        columns.reserve(column_names_and_types.size());

        Block block_to_read;
        for (auto & [partition_id, block_and_offset] : buffer.data_map)
            appendBlock(storage.log, block_and_offset.block, 0, block_to_read);

        /// TODO: Reduce the query data based on the partitions in the where condition
        for (const auto & elem : column_names_and_types)
            columns.emplace_back(getColumnFromBlock(block_to_read, elem));

        const UInt64 size = columns.at(0)->size();
        res.setColumns(std::move(columns), size);

        return res;
    }

private:
    NamesAndTypesList column_names_and_types;
    StorageBuffer::Buffer & buffer;
    int32_t metadata_version;
    bool has_been_read = false;
    StorageBuffer & storage;
};


QueryProcessingStage::Enum StorageBuffer::getQueryProcessingStage(
    ContextPtr local_context, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr &, SelectQueryInfo & query_info) const
{
    if (auto destination = getDestinationTable())
    {
        const auto & destination_metadata = destination->getInMemoryMetadataPtr();
        return destination->getQueryProcessingStage(
            local_context, to_stage, destination->getStorageSnapshot(destination_metadata, local_context), query_info);
    }

    return QueryProcessingStage::FetchColumns;
}

void StorageBuffer::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    const auto & metadata_snapshot = storage_snapshot->metadata;

    if (local_context->getSettingsRef().select_union_destination)
    {
        if (auto destination = getDestinationTable())
        {
            auto destination_lock
                = destination->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);

            auto destination_metadata_snapshot = destination->getInMemoryMetadataPtr();
            auto destination_snapshot = destination->getStorageSnapshot(destination_metadata_snapshot, local_context);

            const bool dst_has_same_structure = std::all_of(column_names.begin(), column_names.end(), [metadata_snapshot, destination_metadata_snapshot](const String& column_name)
            {
                const auto & dest_columns = destination_metadata_snapshot->getColumns();
                const auto & our_columns = metadata_snapshot->getColumns();
                auto dest_columm = dest_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name);
                return dest_columm && dest_columm->type->equals(*our_columns.getColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name).type);
            });

            if (dst_has_same_structure)
            {
                if (query_info.order_optimizer)
                    query_info.input_order_info = query_info.order_optimizer->getInputOrder(destination_metadata_snapshot, local_context);

                /// The destination table has the same structure of the requested columns and we can simply read blocks from there.
                destination->read(
                    query_plan, column_names, destination_snapshot, query_info,
                    local_context, processed_stage, max_block_size, num_streams);
            }
            else
            {
                /// There is a struct mismatch and we need to convert read blocks from the destination table.
                const Block header = metadata_snapshot->getSampleBlock();
                Names columns_intersection = column_names;
                Block header_after_adding_defaults = header;
                const auto & dest_columns = destination_metadata_snapshot->getColumns();
                const auto & our_columns = metadata_snapshot->getColumns();
                for (const String & column_name : column_names)
                {
                    if (!dest_columns.hasPhysical(column_name))
                    {
                        LOG_WARNING(log, "Destination table {} doesn't have column {}. The default values are used.", destination_id.getNameForLogs(), backQuoteIfNeed(column_name));
                        std::erase(columns_intersection, column_name);
                        continue;
                    }
                    const auto & dst_col = dest_columns.getPhysical(column_name);
                    const auto & col = our_columns.getPhysical(column_name);
                    if (!dst_col.type->equals(*col.type))
                    {
                        LOG_WARNING(log, "Destination table {} has different type of column {} ({} != {}). Data from destination table are converted.", destination_id.getNameForLogs(), backQuoteIfNeed(column_name), dst_col.type->getName(), col.type->getName());
                        header_after_adding_defaults.getByName(column_name) = ColumnWithTypeAndName(dst_col.type, column_name);
                    }
                }

                if (columns_intersection.empty())
                {
                    LOG_WARNING(log, "Destination table {} has no common columns with block in buffer. Block of data is skipped.", destination_id.getNameForLogs());
                }
                else
                {
                    auto src_table_query_info = query_info;
                    if (src_table_query_info.prewhere_info)
                    {
                        src_table_query_info.prewhere_info = src_table_query_info.prewhere_info->clone();

                        auto actions_dag = ActionsDAG::makeConvertingActions(
                                header_after_adding_defaults.getColumnsWithTypeAndName(),
                                header.getColumnsWithTypeAndName(),
                                ActionsDAG::MatchColumnsMode::Name);

                        if (src_table_query_info.prewhere_info->row_level_filter)
                        {
                            src_table_query_info.prewhere_info->row_level_filter = ActionsDAG::merge(
                                actions_dag.clone(),
                                std::move(*src_table_query_info.prewhere_info->row_level_filter));

                            src_table_query_info.prewhere_info->row_level_filter->removeUnusedActions();
                        }

                        {
                            src_table_query_info.prewhere_info->prewhere_actions = ActionsDAG::merge(
                                actions_dag.clone(),
                                std::move(src_table_query_info.prewhere_info->prewhere_actions));

                            src_table_query_info.prewhere_info->prewhere_actions.removeUnusedActions();
                        }
                    }

                    destination->read(
                            query_plan, columns_intersection, destination_snapshot, src_table_query_info,
                            local_context, processed_stage, max_block_size, num_streams);

                    if (query_plan.isInitialized())
                    {

                        auto actions = addMissingDefaults(
                                query_plan.getCurrentDataStream().header,
                                header_after_adding_defaults.getNamesAndTypesList(),
                                metadata_snapshot->getColumns(),
                                local_context);

                        auto adding_missed = std::make_unique<ExpressionStep>(
                                query_plan.getCurrentDataStream(),
                                std::move(actions));

                        adding_missed->setStepDescription("Add columns missing in destination table");
                        query_plan.addStep(std::move(adding_missed));

                        auto actions_dag = ActionsDAG::makeConvertingActions(
                                query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
                                header.getColumnsWithTypeAndName(),
                                ActionsDAG::MatchColumnsMode::Name);

                        auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), std::move(actions_dag));

                        converting->setStepDescription("Convert destination table columns to Buffer table structure");
                        query_plan.addStep(std::move(converting));
                    }
                }
            }

            if (query_plan.isInitialized())
            {
                query_plan.addStorageHolder(destination);
                query_plan.addTableLock(std::move(destination_lock));
            }
        }
    }


    Pipe pipe_from_buffers;
    {
        Pipes pipes_from_buffers;
        pipes_from_buffers.reserve(num_shards);
        for (auto & buf : buffers)
            pipes_from_buffers.emplace_back(std::make_shared<BufferSource>(column_names, buf, storage_snapshot));

        pipe_from_buffers = Pipe::unitePipes(std::move(pipes_from_buffers));
        if (query_info.input_order_info)
        {
            /// Each buffer has one block, and it not guaranteed that rows in each block are sorted by order keys
            pipe_from_buffers.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<PartialSortingTransform>(header, query_info.input_order_info->sort_description_for_merging, 0);
            });
        }
    }

    if (pipe_from_buffers.empty())
        return;

    QueryPlan buffers_plan;

    /** If the sources from the table were processed before some non-initial stage of query execution,
      * then sources from the buffers must also be wrapped in the processing pipeline before the same stage.
      */
    /// TODO: Find a way to support projections for StorageBuffer
    if (processed_stage > QueryProcessingStage::FetchColumns)
    {
        if (local_context->getSettingsRef().allow_experimental_analyzer)
        {
            auto storage = std::make_shared<StorageValues>(
                    getStorageID(),
                    storage_snapshot->getAllColumnsDescription(),
                    std::move(pipe_from_buffers),
                    *getVirtualsPtr());

            auto interpreter = InterpreterSelectQueryAnalyzer(
                    query_info.query, local_context, storage,
                    SelectQueryOptions(processed_stage));
            interpreter.addStorageLimits(*query_info.storage_limits);
            buffers_plan = std::move(interpreter).extractQueryPlan();
        }
        else
        {
            auto interpreter = InterpreterSelectQuery(
                    query_info.query, local_context, std::move(pipe_from_buffers),
                    SelectQueryOptions(processed_stage));
            interpreter.addStorageLimits(*query_info.storage_limits);
            interpreter.buildQueryPlan(buffers_plan);
        }
    }
    else
    {
        if (query_info.prewhere_info)
        {
            auto actions_settings = ExpressionActionsSettings::fromContext(local_context);

            if (query_info.prewhere_info->row_level_filter)
            {
                auto actions = std::make_shared<ExpressionActions>(query_info.prewhere_info->row_level_filter->clone(), actions_settings);
                pipe_from_buffers.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<FilterTransform>(
                            header,
                            actions,
                            query_info.prewhere_info->row_level_column_name,
                            false);
                });
            }

            auto actions = std::make_shared<ExpressionActions>(query_info.prewhere_info->prewhere_actions.clone(), actions_settings);
            pipe_from_buffers.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<FilterTransform>(
                        header,
                        actions,
                        query_info.prewhere_info->prewhere_column_name,
                        query_info.prewhere_info->remove_prewhere_column);
            });
        }

        for (const auto & processor : pipe_from_buffers.getProcessors())
            processor->setStorageLimits(query_info.storage_limits);

        auto read_from_buffers = std::make_unique<ReadFromPreparedSource>(std::move(pipe_from_buffers));
        read_from_buffers->setStepDescription("Read from buffers of Buffer table");
        buffers_plan.addStep(std::move(read_from_buffers));
    }

    if (!query_plan.isInitialized())
    {
        query_plan = std::move(buffers_plan);
        return;
    }

    auto result_header = buffers_plan.getCurrentDataStream().header;

    /// Convert structure from table to structure from buffer.
    if (!blocksHaveEqualStructure(query_plan.getCurrentDataStream().header, result_header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
                result_header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);

        auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), std::move(convert_actions_dag));
        query_plan.addStep(std::move(converting));
    }

    DataStreams input_streams;
    input_streams.emplace_back(query_plan.getCurrentDataStream());
    input_streams.emplace_back(buffers_plan.getCurrentDataStream());

    std::vector<std::unique_ptr<QueryPlan>> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(buffers_plan)));
    query_plan = QueryPlan();

    auto union_step = std::make_unique<UnionStep>(std::move(input_streams));
    union_step->setStepDescription("Unite sources from Buffer table");
    query_plan.unitePlans(std::move(union_step), std::move(plans));
}

void StorageBuffer::BlockAndOffsetWithPartition::appendBlockAndOffset(const LoggerPtr & log_, BlockAndOffsetWithPartition & block_and_offset)
{
    const Block & from = block_and_offset.block;
    Block & to = this->block;

    appendBlock(log_, from, 0, to);

    std::ranges::copy(block_and_offset.offsets, std::back_inserter(this->offsets));
    std::ranges::copy(block_and_offset.wal_pos, std::back_inserter(this->wal_pos));
}

void StorageBuffer::BlockAndOffsetWithPartition::appendOffset(const LoggerPtr & /* log */, BlockAndOffsetWithPartition & block_and_offset)
{
    std::ranges::copy(block_and_offset.offsets, std::back_inserter(this->offsets));
    std::ranges::copy(block_and_offset.wal_pos, std::back_inserter(this->wal_pos));
}

void StorageBuffer::BlockAndOffsetWithPartition::removeBlockAndOffset(const LoggerPtr & log_, BlockAndOffsetWithPartition & block_and_offset)
{
    size_t start_rows = 0;
    start_rows = std::accumulate(block_and_offset.offsets.begin(), block_and_offset.offsets.end(), start_rows);

    if (start_rows > this->block.rows() || block_and_offset.offsets.size() > this->offsets.size()
        || block_and_offset.wal_pos.size() > this->wal_pos.size() || block_and_offset.offsets.size() != block_and_offset.wal_pos.size()
        || this->offsets.size() != this->wal_pos.size() || start_rows != block_and_offset.block.rows())
    {
        LOG_FATAL(
            log_,
            "Error data detected when removing block and offset from buffer start rows:{} remove block rows: {} remove offset size:{} remove wal_pos "
            "size: {} this block rows: {} this offset size:{} this wal_pos size:{}.",
            start_rows,
            block_and_offset.block.rows(),
            block_and_offset.offsets.size(),
            block_and_offset.wal_pos.size(),
            this->block.rows(),
            this->offsets.size(),
            this->wal_pos.size());

        /// In case when we cannot rollback, do not leave incorrect state in memory.
        std::terminate();
    }

    Block tmp_block = this->block.cloneEmpty();
    std::vector<size_t> tmp_offsets(this->offsets.begin() + block_and_offset.offsets.size(), this->offsets.end());
    std::vector<size_t> tmp_wal_pos(this->wal_pos.begin() + block_and_offset.wal_pos.size(), this->wal_pos.end());

    appendBlock(log_, this->block, start_rows, tmp_block);

    this->block.swap(tmp_block);
    this->offsets.swap(tmp_offsets);
    this->wal_pos.swap(tmp_wal_pos);
}

void StorageBuffer::BlockAndOffsetWithPartition::checkBLockAndOffset(const LoggerPtr & log_) const
{
    size_t total_rows = 0;
    total_rows = std::accumulate(offsets.begin(), offsets.end(), total_rows);

    if (total_rows != this->block.rows() || offsets.size() != wal_pos.size())
    {
        LOG_FATAL(
            log_,
            "Error data detected when check block and offset, offset total rows:{} block rows: {} offset size:{}  wal_pos:{} ",
            total_rows,
            block.rows(),
            offsets.size(),
            wal_pos.size());

        /// In case when we cannot rollback, do not leave incorrect state in memory.
        std::terminate();
    }
}

class BufferSink : public SinkToStorage
{
public:
    explicit BufferSink(StorageBuffer & storage_, const StorageMetadataPtr & metadata_snapshot_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock()), storage(storage_), metadata_snapshot(metadata_snapshot_)
    {
        // Check table structure.
        metadata_snapshot->check(getHeader(), true);
    }

    String getName() const override { return "BufferSink"; }

    bool tryAppendToBuffer(Block & block) const
    {
        bool append = false;
        /// We distribute the load on the shards by the stream number.
        const auto start_shard_num = getThreadId() % storage.num_shards;

        /// We loop through the buffers, trying to lock mutex. No more than one lap.
        auto shard_num = start_shard_num;

        StorageBuffer::Buffer * least_busy_buffer = nullptr;
        std::unique_lock<std::mutex> least_busy_lock;
        size_t least_busy_shard_rows = 0;

        for (size_t try_no = 0; try_no < storage.num_shards; ++try_no)
        {
            std::unique_lock lock(storage.buffers[shard_num].tryLock());

            if (lock.owns_lock())
            {
                size_t num_rows = storage.buffers[shard_num].bufferRows();
                if (!least_busy_buffer || num_rows < least_busy_shard_rows)
                {
                    least_busy_buffer = &storage.buffers[shard_num];
                    least_busy_lock = std::move(lock);
                    least_busy_shard_rows = num_rows;
                }
            }

            shard_num = (shard_num + 1) % storage.num_shards;
        }

        /// If you still can not lock anything at once, then we'll wait on mutex.
        if (!least_busy_buffer)
        {
            least_busy_buffer = &storage.buffers[start_shard_num];
            LOG_TRACE(storage.log, "Can not try lock any buffer, so wait on random buffer:{}", static_cast<void *>(least_busy_buffer));
            least_busy_lock = least_busy_buffer->lockForWriting();
            LOG_TRACE(storage.log, "Wait on random buffer:{} finish", static_cast<void *>(least_busy_buffer));
        }

        append = insertIntoBuffer(block, *least_busy_buffer, metadata_snapshot->metadata_version);
        least_busy_lock.unlock();

        return append;
    }

    void consume(Chunk chunk) override
    {
        size_t rows = chunk.getNumRows();
        if (!rows)
            return;

        auto block = getHeader().cloneWithColumns(chunk.getColumns());

        StoragePtr destination = storage.getDestinationTable();
        if (destination)
        {
            if (destination.get() == &storage)
                throw Exception(ErrorCodes::INFINITE_LOOP, "Destination table is myself. Write will cause infinite loop.");
        }

        size_t bytes = block.bytes();

        storage.lifetime_writes.rows += rows;
        storage.lifetime_writes.bytes += bytes;

        /// If the block already exceeds the maximum limit, then we skip the buffer.
        if (rows > storage.max_thresholds.rows || bytes > storage.max_thresholds.bytes)
        {
            if (destination)
            {
                LOG_DEBUG(storage.log, "Writing block with {} rows, {} bytes directly.", rows, bytes);
                storage.writeBlockToDestination(block, destination);
            }
            return;
        }

        /// default 3
        UInt64 append_to_buffer_max_try_count = storage.getContext()->getSettings().append_to_buffer_max_try_count;
        /// default 1000 ms
        auto interval =  storage.getContext()->getSettings().append_to_buffer_try_interval_ms.totalMilliseconds();
        bool append_to_buffer = false;
        for (UInt64 i = 0; (i < append_to_buffer_max_try_count || !append_to_buffer_max_try_count); i++)
        {
            if (tryAppendToBuffer(block))
            {
                append_to_buffer = true;
                break;
            }

            storage.schedule();

            LOG_TRACE(storage.log, "Buffer sink consumer start wait iterval:{} ms for flush try count:{}", interval, i + 1);
            bool wait_pred = storage.waitForFlush(interval);
            LOG_TRACE(storage.log, "Buffer sink consumer wait for flush result:{} try count:{}", wait_pred ? "(received notidy)" : "(timeout)", i + 1);
        }

        if (!append_to_buffer)
            throw Exception(ErrorCodes::TOO_SLOW, "Flushing to the destination table is too slow, please retry later.");

        storage.reschedule();
    }

private:
    StorageBuffer & storage;
    StorageMetadataPtr metadata_snapshot;

    bool insertIntoBuffer(const Block & block, StorageBuffer::Buffer & buffer, int32_t metadata_version) const
    {
        const time_t current_time = time(nullptr);

        /// Sort the columns in the block. This is necessary to make it easier to concatenate the blocks later.
        Block sorted_block = block.sortColumns();


        /// TODO: process metadata change
        /// drop all data when metadata change
        if (buffer.metadata_version != metadata_version)
        {
            buffer.data_map.clear();

            if (storage.enableWriteAheadLog())
                storage.rebuildPartLog(buffer, true);

            buffer.metadata_version = metadata_version;

            storage.total_writes.rows = buffer.bufferRows();
            storage.total_writes.bytes = buffer.bufferAllocatedBytes();
        }

        if (!buffer.data_map.empty()
            && storage.checkThresholds(buffer, current_time))
        {
            /** If, after inserting the buffer, the constraints are exceeded, then we will reset the buffer.
              * This also protects against unlimited consumption of RAM, since if it is impossible to write to the table,
              *  an exception will be thrown, and new data will not be added to the buffer.
              */

            /// will wait for backgroup thread flushBuffer finish
            return false;
        }

        if (!buffer.first_write_time)
            buffer.first_write_time = current_time;

        const StoragePtr destination = storage.getDestinationTable();

        /// split block with partition
        auto part_blocks = storage.splitBlockIntoParts(std::move(sorted_block), buffer, destination);

        /// add to data map
        for (auto & block_and_offset : part_blocks)
        {
            String partition_id = storage.getPartitionIdFromRow(block_and_offset.partition, destination);

            if (auto it = buffer.data_map.find(partition_id); it != buffer.data_map.end())
            {
                it->second.appendBlockAndOffset(storage.log, block_and_offset);
                it->second.checkBLockAndOffset(storage.log);
            }
            else
            {
                block_and_offset.checkBLockAndOffset(storage.log);
                buffer.data_map.emplace(partition_id, block_and_offset);
            }

            CurrentMetrics::add(CurrentMetrics::StorageBufferRows, block_and_offset.block.rows());
            CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, block_and_offset.block.bytes());
        }

        if (storage.enableWriteAheadLog())
            for (auto & block_and_offset : part_blocks)
            {
                buffer.write_ahead_log->addBlock(block_and_offset.wal_pos.front(), block_and_offset.block);
                LOG_TRACE(storage.log, "Add buffer:{} WAL Block partition:{} wal_pos:{} rows:{} bytes:{}",
                    static_cast<void *>(&buffer),
                    storage.getPartitionIdFromRow(block_and_offset.partition, destination),
                    block_and_offset.wal_pos.front(),
                    block_and_offset.block.rows(),
                    block_and_offset.block.bytes());
            }

        storage.total_writes.rows = buffer.bufferRows();
        storage.total_writes.bytes = buffer.bufferAllocatedBytes();
        return true;
    }
};

SinkToStoragePtr
StorageBuffer::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/, bool /*async_insert*/)
{
    return std::make_shared<BufferSink>(*this, metadata_snapshot);
}


void StorageBuffer::startup()
{
    if (getContext()->getSettingsRef().readonly)
    {
        LOG_WARNING(
            log,
            "Storage {} is run with readonly settings, it will not be able to insert data. Set appropriate buffer_profile to fix this.",
            getName());
    }

    flush_handle->activateAndSchedule();
}


void StorageBuffer::flushAndPrepareForShutdown()
{
    if (!flush_handle)
        return;

    flush_handle->deactivate();

    try
    {
        optimize(
            nullptr /*query*/,
            getInMemoryMetadataPtr(),
            {} /*partition*/,
            false /*final*/,
            false /*deduplicate*/,
            {},
            false /*cleanup*/,
            getContext());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


/** NOTE If you do OPTIMIZE after insertion,
  * it does not guarantee, that all data will be in destination table at the time of next SELECT just after OPTIMIZE.
  *
  * Because in case if there was already running flushBuffer method,
  *  then call to flushBuffer inside OPTIMIZE will see empty buffer and return quickly,
  *  but at the same time, the already running flushBuffer method possibly is not finished,
  *  so next SELECT will observe missing data.
  *
  * This kind of race condition make very hard to implement proper tests.
  */
bool StorageBuffer::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & /* deduplicate_by_columns */,
    bool cleanup,
    ContextPtr /*context*/)
{
    if (partition)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Partition cannot be specified when optimizing table of type Buffer");

    if (final)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "FINAL cannot be specified when optimizing table of type Buffer");

    if (deduplicate)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DEDUPLICATE cannot be specified when optimizing table of type Buffer");

    if (cleanup)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CLEANUP cannot be specified when optimizing table of type Buffer");

    flushAllBuffers(false);
    return true;
}

bool StorageBuffer::supportsPrewhere() const
{
    if (auto destination = getDestinationTable())
        return destination->supportsPrewhere();
    return false;
}

bool StorageBuffer::checkThresholds(
    const Buffer & buffer, time_t current_time) const
{
    time_t time_passed = 0;
    if (buffer.first_write_time)
        time_passed = current_time - buffer.first_write_time;

    size_t rows = buffer.bufferRows();
    size_t bytes = buffer.bufferBytes();

    if (time_passed > max_thresholds.time)
        return true;

    if ((rows * 100 / 150) > max_thresholds.rows)
        return true;

    if ((bytes * 100 / 150) > max_thresholds.bytes)
        return true;

    return false;
}


bool StorageBuffer::checkThresholdsImpl(bool direct, size_t rows, size_t bytes, time_t time_passed) const
{
    if (time_passed > min_thresholds.time && rows > min_thresholds.rows && bytes > min_thresholds.bytes)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferPassedAllMinThresholds);
        return true;
    }

    if (time_passed > max_thresholds.time)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferPassedTimeMaxThreshold);
        return true;
    }

    if (rows > max_thresholds.rows)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferPassedRowsMaxThreshold);
        return true;
    }

    if (bytes > max_thresholds.bytes)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferPassedBytesMaxThreshold);
        return true;
    }

    if (!direct)
    {
        if (flush_thresholds.time && time_passed > flush_thresholds.time)
        {
            ProfileEvents::increment(ProfileEvents::StorageBufferPassedTimeFlushThreshold);
            return true;
        }

        if (flush_thresholds.rows && rows > flush_thresholds.rows)
        {
            ProfileEvents::increment(ProfileEvents::StorageBufferPassedRowsFlushThreshold);
            return true;
        }

        if (flush_thresholds.bytes && bytes > flush_thresholds.bytes)
        {
            ProfileEvents::increment(ProfileEvents::StorageBufferPassedBytesFlushThreshold);
            return true;
        }
    }

    return false;
}

void StorageBuffer::flushAllBuffers(bool check_thresholds)
{
    for (auto & buf : buffers)
    {
        if (flush_pool)
        {
            scheduleFromThreadPool<void>(
                [&]() { check_thresholds ? flushSomeBuffer(buf) : flushAllBuffer(buf); }, *flush_pool, "BufferFlush");
        }
        else
        {
            check_thresholds ? flushSomeBuffer(buf) : flushAllBuffer(buf);
        }
    }

    if (flush_pool)
        flush_pool->wait();

    notifyAllWaitForFlush();
    LOG_TRACE(log, "Check thresholds:{} and flush buffer, notify buffer sink can append data continue.", check_thresholds);
}

bool StorageBuffer::flushSomeBuffer(Buffer & buffer)
{
    time_t time_passed = 0;
    time_t current_time = time(nullptr);
    if (buffer.first_write_time)
        time_passed = current_time - buffer.first_write_time;

    BufferBlockAndOffsetWithPartitionMap map;
    {
        std::unique_lock<std::mutex> lock(buffer.lockForReading());
        /// select which partition can be flush
        if (!enumeratePartitionBuffer(buffer, time_passed, map))
            return false;
    }

    buffer.first_write_time = 0;

    size_t block_rows = 0;
    size_t block_bytes = 0;

    for (auto & [partition_id, block_and_offset] : map)
    {
        block_rows += block_and_offset.block.rows();
        block_bytes += block_and_offset.block.bytes();
    }

    CurrentMetrics::sub(CurrentMetrics::StorageBufferRows, block_rows);
    CurrentMetrics::sub(CurrentMetrics::StorageBufferBytes, block_bytes);

    ProfileEvents::increment(ProfileEvents::StorageBufferFlush);

    if (!destination_id)
    {
        {
            std::unique_lock<std::mutex> lock(buffer.lockForWriting());

            /// TODO: lock write for buffer and delete flush data in buffer
            removeFlushDataInBuffer(buffer, map);

            if (enableWriteAheadLog())
            {
                /// In order to be compatible with the old version of the log format,
                /// the log needs to be forced to be rewritten after the first refresh after reading out the old data.
                bool force_rebuild = false;
                if (!buffer.has_flushed_once)
                {
                    force_rebuild = true;
                    buffer.has_flushed_once = true;
                }

                /// try rebuild wal, if not rebuild then write drop part log
                if (!rebuildPartLog(buffer, force_rebuild))
                    writeDropPartLog(buffer, map);
            }
        }

        total_writes.rows = buffer.bufferRows();
        total_writes.bytes = buffer.bufferAllocatedBytes();

        LOG_DEBUG(
            log,
            "Flushing buffer:{}'s some partition with {} rows (discarded), {} bytes, age {} seconds {}.",
            static_cast<void *>(&buffer),
            block_rows,
            block_bytes,
            time_passed,
            "(bg)");
        return true;
    }

    /** For simplicity, buffer is locked during write.
        * We could unlock buffer temporary, but it would lead to too many difficulties:
        * - data, that is written, will not be visible for SELECTs;
        * - new data could be appended to buffer, and in case of exception, we must merge it with old data, that has not been written;
        * - this could lead to infinite memory growth.
        */

    Stopwatch watch;
    try
    {
        Block block_to_write;

        for (auto & [partition_id, block_and_offset] : map)
            appendBlock(log, block_and_offset.block, 0, block_to_write);

        writeBlockToDestination(block_to_write, getDestinationTable());
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferErrorOnFlush);

        /// Return the block to its place in the buffer.

        CurrentMetrics::add(CurrentMetrics::StorageBufferRows, block_rows);
        CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, block_bytes);


        if (!buffer.first_write_time)
            buffer.first_write_time = current_time;

        /// After a while, the next write attempt will happen.
        throw;
    }

    {
        std::unique_lock<std::mutex> lock(buffer.lockForWriting());
        /// lock write for buffer and delete flush data in buffer
        removeFlushDataInBuffer(buffer, map);

        if (enableWriteAheadLog())
        {
            /// In order to be compatible with the old version of the log format,
            /// the log needs to be forced to be rewritten after the first refresh after reading out the old data.
            bool force_rebuild = false;
            if (!buffer.has_flushed_once)
            {
                force_rebuild = true;
                buffer.has_flushed_once = true;
            }

            /// try rebuild wal, if not rebuild then write drop part log
            if (!rebuildPartLog(buffer, force_rebuild))
            {
                writeDropPartLog(buffer, map);
            }
        }
    }

    if (!buffer.first_write_time)
        buffer.first_write_time = current_time;

    total_writes.rows = buffer.bufferRows();
    total_writes.bytes = buffer.bufferAllocatedBytes();

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(
        log,
        "Flushing buffer:{}'s some partition with {} rows, {} bytes, age {} seconds, took {} ms {}.",
        static_cast<void *>(&buffer),
        block_rows,
        block_bytes,
        time_passed,
        milliseconds,
        "(bg)");
    return true;
}

bool StorageBuffer::flushAllBuffer(Buffer & buffer)
{
    time_t time_passed = 0;
    time_t current_time = time(nullptr);
    if (buffer.first_write_time)
        time_passed = current_time - buffer.first_write_time;

    size_t block_rows = buffer.bufferRows();
    size_t block_bytes = buffer.bufferBytes();
    buffer.first_write_time = 0;

    CurrentMetrics::sub(CurrentMetrics::StorageBufferRows, block_rows);
    CurrentMetrics::sub(CurrentMetrics::StorageBufferBytes, block_bytes);

    ProfileEvents::increment(ProfileEvents::StorageBufferFlush);

    if (!destination_id)
    {
        {
            std::unique_lock<std::mutex> lock(buffer.lockForWriting());

            buffer.data_map.clear();

            if (enableWriteAheadLog())
            {
                if (!buffer.has_flushed_once)
                    buffer.has_flushed_once = true;
                /// rebuild wal
                rebuildPartLog(buffer, true);
            }
        }

        total_writes.rows = buffer.bufferRows();
        total_writes.bytes = buffer.bufferAllocatedBytes();

        LOG_DEBUG(
            log,
            "Flushing buffer:{}'s all partition with {} rows (discarded), {} bytes, age {} seconds {}.",
            static_cast<void *>(&buffer),
            block_rows,
            block_bytes,
            time_passed,
            "(bg)");
        return true;
    }

    Stopwatch watch;
    {
        std::unique_lock<std::mutex> lock(buffer.lockForWriting());

        try
        {
            Block block_to_write;
            for (auto & [partition_id, block_and_offset] : buffer.data_map)
                appendBlock(log, block_and_offset.block, 0, block_to_write);

            writeBlockToDestination(block_to_write, getDestinationTable());

            buffer.data_map.clear();
            if (enableWriteAheadLog())
            {
                /// try rebuild wal, if not rebuild then write drop part log
                rebuildPartLog(buffer, true);
            }

        }
        catch (...)
        {
            ProfileEvents::increment(ProfileEvents::StorageBufferErrorOnFlush);

            /// Return the block to its place in the buffer.

            CurrentMetrics::add(CurrentMetrics::StorageBufferRows, block_rows);
            CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, block_bytes);


            if (!buffer.first_write_time)
                buffer.first_write_time = current_time;

            /// After a while, the next write attempt will happen.
            throw;
        }
    }

    total_writes.rows = buffer.bufferRows();
    total_writes.bytes = buffer.bufferAllocatedBytes();

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(
        log,
        "Flushing buffer:{}'s all partition with {} rows, {} bytes, age {} seconds, took {} ms {}.",
        static_cast<void *>(&buffer),
        block_rows,
        block_bytes,
        time_passed,
        milliseconds,
        "(bg)");

    return true;
}

void StorageBuffer::writeBlockToDestination(const Block & block, StoragePtr table) const
{
    if (!destination_id || !block)
        return;

    if (!table)
    {
        LOG_ERROR(log, "Destination table {} doesn't exist. Block of data is discarded.", destination_id.getNameForLogs());
        return;
    }
    auto destination_metadata_snapshot = table->getInMemoryMetadataPtr();

    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = destination_id;

    /** We will insert columns that are the intersection set of columns of the buffer table and the subordinate table.
      * This will support some of the cases (but not all) when the table structure does not match.
      */
    Block structure_of_destination_table = allow_materialized ? destination_metadata_snapshot->getSampleBlock()
                                                              : destination_metadata_snapshot->getSampleBlockNonMaterialized();
    Block block_to_write;
    for (size_t i : collections::range(0, structure_of_destination_table.columns()))
    {
        auto dst_col = structure_of_destination_table.getByPosition(i);
        if (block.has(dst_col.name))
        {
            auto column = block.getByName(dst_col.name);
            if (!column.type->equals(*dst_col.type))
            {
                LOG_WARNING(
                    log,
                    "Destination table {} have different type of column {} ({} != {}). Block of data is converted.",
                    destination_id.getNameForLogs(),
                    backQuoteIfNeed(column.name),
                    dst_col.type->getName(),
                    column.type->getName());
                column.column = castColumn(column, dst_col.type);
                column.type = dst_col.type;
            }

            block_to_write.insert(column);
        }
    }

    if (block_to_write.columns() == 0)
    {
        LOG_ERROR(
            log,
            "Destination table {} have no common columns with block in buffer. Block of data is discarded.",
            destination_id.getNameForLogs());
        return;
    }

    if (block_to_write.columns() != block.columns())
        LOG_WARNING(
            log,
            "Not all columns from block in buffer exist in destination table {}. Some columns are discarded.",
            destination_id.getNameForLogs());

    auto list_of_columns = std::make_shared<ASTExpressionList>();
    insert->columns = list_of_columns;
    list_of_columns->children.reserve(block_to_write.columns());
    for (const auto & column : block_to_write)
        list_of_columns->children.push_back(std::make_shared<ASTIdentifier>(column.name));

    auto insert_context = Context::createCopy(getContext());
    insert_context->makeQueryContext();

    InterpreterInsertQuery interpreter{insert, insert_context, allow_materialized};

    auto block_io = interpreter.execute();
    PushingPipelineExecutor executor(block_io.pipeline);
    executor.start();
    executor.push(std::move(block_to_write));
    executor.finish();
}


void StorageBuffer::backgroundFlush()
{
    try
    {
        flushAllBuffers(true);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    reschedule();
}

void StorageBuffer::reschedule()
{
    time_t min_first_write_time = std::numeric_limits<time_t>::max();
    time_t rows = 0;

    for (auto & buffer : buffers)
    {
        /// try_to_lock here to avoid waiting for other layers flushing to be finished,
        /// since the buffer table may:
        /// - push to Distributed table, that may take too much time,
        /// - push to table with materialized views attached,
        ///   this is also may take some time.
        ///
        /// try_to_lock is also ok for background flush, since if there is
        /// INSERT contended, then the reschedule will be done after
        /// INSERT will be done.
        std::unique_lock lock(buffer.tryLock());
        if (lock.owns_lock())
        {
            if (!buffer.data_map.empty())
            {
                min_first_write_time = std::min(min_first_write_time, buffer.first_write_time);
                rows += buffer.bufferRows();
            }
        }
    }

    /// will be rescheduled via INSERT
    if (!rows)
        return;

    time_t current_time = time(nullptr);
    time_t time_passed = current_time - min_first_write_time;

    size_t min = std::max<ssize_t>(min_thresholds.time - time_passed, 1);
    size_t max = std::max<ssize_t>(max_thresholds.time - time_passed, 1);
    size_t min_schedule_seconds = std::min(min, max);
    size_t min_schedule_milliseconds = std::min(min_schedule_seconds, static_cast<size_t>(4)) * 1000;
    if (flush_thresholds.time)
    {
        size_t flush = std::max<ssize_t>(flush_thresholds.time - time_passed, 1);
        min_schedule_milliseconds = std::min(min_schedule_milliseconds, static_cast<size_t>(flush * 1000));
    }

    flush_handle->scheduleAfter(min_schedule_milliseconds);
}

void StorageBuffer::schedule()
{
    flush_handle->schedule();
}

void StorageBuffer::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    std::optional<NameDependencies> name_deps{};
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::COMMENT_COLUMN
            && command.type != AlterCommand::Type::COMMENT_TABLE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());

        if (command.type == AlterCommand::Type::DROP_COLUMN && !command.clear)
        {
            if (!name_deps)
                name_deps = getDependentViewsByColumn(local_context);
            const auto & deps_mv = name_deps.value()[command.column_name];
            if (!deps_mv.empty())
            {
                throw Exception(
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER DROP column {} which is referenced by materialized view {}",
                    backQuoteIfNeed(command.column_name),
                    toString(deps_mv));
            }
        }
    }
}

std::optional<UInt64> StorageBuffer::totalRows(const Settings & settings) const
{
    std::optional<UInt64> underlying_rows;
    if (settings.select_union_destination)
    {
        if (auto destination = getDestinationTable())
            underlying_rows = destination->totalRows(settings);
    }

    return total_writes.rows + underlying_rows.value_or(0);
}

std::optional<UInt64> StorageBuffer::totalBytes(const Settings & settings) const
{
    std::optional<UInt64> underlying_bytes;
    if (settings.select_union_destination)
    {
        if (auto destination = getDestinationTable())
            underlying_bytes = destination->totalBytes(settings);
    }

    return total_writes.bytes + underlying_bytes.value_or(0);
}

void StorageBuffer::alter(const AlterCommands & params, ContextPtr local_context, AlterLockHolder &)
{
    auto table_id = getStorageID();
    checkAlterIsPossible(params, local_context);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    bool try_fix_optimize_exception = false;
    try
    {
        /// Flush buffers to the storage because BufferSource skips buffers with old metadata_version.
        optimize(
            {} /*query*/,
            metadata_snapshot,
            {} /*partition_id*/,
            false /*final*/,
            false /*deduplicate*/,
            {},
            false /*cleanup*/,
            local_context);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        try_fix_optimize_exception = true;
    }

    if (try_fix_optimize_exception)
    {
        LOG_WARNING(log, "Buffer storages try fix optimize exception");
        clearBuffer();
        LOG_INFO(log, "Buffer storages has fixed optimize exception");
    }

    StorageInMemoryMetadata new_metadata = *metadata_snapshot;
    params.apply(new_metadata, local_context);
    new_metadata.metadata_version += 1;
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}
void StorageBuffer::initializeDirectoriesAndFormatVersion(
    const std::string & relative_data_path_, [[maybe_unused]] bool attach, bool need_create_directories)
{
    relative_data_path = relative_data_path_;

    if (relative_data_path.empty())
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Buffer storages with WAL require data path");

    Disks disks = storage_policy->getDisks();

    for (const auto & disk : storage_policy->getDisks())
    {
        if (disk->isBroken())
            continue;

        if (disk->isReadOnly())
        {
            LOG_WARNING(log, "Buffer storages with WAL start with readonly disk:{}.", disk->getName());
            continue;
        }

        if (disk->isWriteOnce())
        {
            LOG_WARNING(log, "Buffer storages with WAL start with write once disk:{}.", disk->getName());
            continue;
        }

        if (need_create_directories)
        {
            disk->createDirectories(relative_data_path);
            disk->createDirectories(fs::path(relative_data_path) / MergeTreeData::DETACHED_DIR_NAME);
        }
    }
}

void StorageBuffer::load_buffer_wal()
{
    if (!enableWriteAheadLog())
        return;

    std::set<String> wal_sets;
    for (size_t i = 0; i < buffers.size(); i++)
    {
        String wal_name = fmt::format("wal_{}.bin", i);
        wal_sets.emplace(wal_name);
    }

    WriteAheadLogDiskMap wal_maps;
    /*
     * for-each disk to find all wal file
     */
    for (const auto & disk : storage_policy->getDisks())
    {
        if (disk->isBroken() || disk->isReadOnly() || disk->isWriteOnce())
            continue;

        for (const auto it = disk->iterateDirectory(relative_data_path); it->isValid(); it->next())
        {
            if (!startsWith(it->name(), BufferWriteAheadLog::WAL_FILE_NAME))
                continue;

            if (!wal_sets.contains(it->name()))
                throw Exception(
                    ErrorCodes::INCORRECT_FILE_NAME, "Exist incorrect WAL file on disk: {} name: {}", disk->getName(), it->name());

            wal_maps.emplace(it->name(), disk);
        }
    }

    if (!wal_maps.empty() && wal_maps.size() != buffers.size())
    {
        LOG_WARNING(
            log,
            "Some WAL file lost, correct number: {} current number: {} , will recreate those lost WAL file.",
            buffers.size(),
            wal_maps.size());
    }

    const StoragePtr destination = getDestinationTable();

    for (size_t i = 0; i < buffers.size(); i++)
    {
        Block block;
        Buffer & buffer = buffers[i];
        buffer.write_ahead_log = getWriteAheadLog(i, wal_maps);

        UInt64 block_number;
        buffer.write_ahead_log->restore(block, block_number);
        buffer.increment.set(block_number);


        /// split block with partition
        auto part_blocks = splitBlockIntoParts(std::move(block), buffer, destination);

        /// add to data map
        for (const auto & block_and_offset : part_blocks)
        {
            String partition_id = getPartitionIdFromRow(block_and_offset.partition, destination);
            block_and_offset.checkBLockAndOffset(log);
            buffer.data_map.emplace(partition_id, block_and_offset);
        }

        size_t restore_rows = buffer.bufferRows();
        size_t restore_bytes = buffer.bufferBytes();
        size_t restore_allocted_bytes = buffer.bufferAllocatedBytes();

        if (restore_rows > 0 || restore_bytes > 0)
        {
            time_t current_time = time(nullptr);
            if (!buffer.first_write_time)
                buffer.first_write_time = current_time;
        }

        CurrentMetrics::add(CurrentMetrics::StorageBufferRows, restore_rows);
        CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, restore_bytes);

        total_writes.rows += restore_rows;
        total_writes.bytes += restore_allocted_bytes;
    }
}


void StorageBuffer::truncate(
    const ASTPtr & /*query*/, const StorageMetadataPtr & /* metadata_snapshot */, ContextPtr /* context */, TableExclusiveLockHolder &)
{
    clearBuffer();
}

void StorageBuffer::clearBuffer()
{
    for (auto & buffer : buffers)
    {
        std::unique_lock<std::mutex> buffer_lock = buffer.lockForWriting();

        size_t block_rows = buffer.bufferRows();
        size_t block_bytes = buffer.bufferBytes();
        buffer.data_map.clear();

        CurrentMetrics::sub(CurrentMetrics::StorageBufferRows, block_rows);
        CurrentMetrics::sub(CurrentMetrics::StorageBufferBytes, block_bytes);

        total_writes.rows = 0;
        total_writes.bytes = 0;

        if (enableWriteAheadLog())
            buffer.write_ahead_log->truncate(0);

        buffer_lock.unlock();
    }

    notifyAllWaitForFlush();
}


StorageBuffer::WriteAheadLogPtr StorageBuffer::getWriteAheadLog(size_t index, const WriteAheadLogDiskMap & wal_map)
{
    String wal_name = fmt::format("wal_{}.bin", index);

    auto alloc_buffer_log = [this, &wal_name]
    {
        auto reservation = storage_policy->reserve(BufferWriteAheadLog::MAX_WAL_BYTES);
        if (!reservation)
            throw Exception(
                ErrorCodes::NOT_ENOUGH_SPACE, "Can not alloc space:{} for WAL ", ReadableSize(BufferWriteAheadLog::MAX_WAL_BYTES));

        DiskPtr disk = reservation->getDisk();

        LOG_INFO(log, "It will allocate a new WAL file on disk:{} name:{}", disk->getName(), wal_name);

        return std::make_shared<BufferWriteAheadLog>(*this, disk, wal_name);
    };

    if (wal_map.empty())
        return alloc_buffer_log();

    auto it = wal_map.find(wal_name);
    if (it == wal_map.end())
    {
        LOG_WARNING(log, "Loss {} in WAL map ", wal_name);
        return alloc_buffer_log();
    }

    DiskPtr disk = it->second;

    /// TODO: add settings write_ahead_log_max_bytes
    if (!disk->reserve(BufferWriteAheadLog::MAX_WAL_BYTES))
        LOG_TRACE(log, "Could not reserve {} from default disk for index {}", ReadableSize(BufferWriteAheadLog::MAX_WAL_BYTES), index);

    LOG_INFO(log, "It will load a WAL file on disk:{} name:{}", disk->getName(), wal_name);

    return std::make_shared<BufferWriteAheadLog>(*this, disk, wal_name);
}

StorageBuffer::BufferBlocksAndOffsetWithPartition
StorageBuffer::splitBlockIntoParts(Block && block, Buffer & buffer, const StoragePtr & destination)
{
    BufferBlocksAndOffsetWithPartition result;
    if (!block || !block.rows())
        return result;

    auto return_empty_partition_block = [&block, &result, &buffer]() -> StorageBuffer::BufferBlocksAndOffsetWithPartition
    {
        const size_t offset = block.rows();
        result.emplace_back(Block(block), Row{});
        result[0].offsets.push_back(offset);
        result[0].wal_pos.push_back(buffer.increment.get());

        return result;
    };

    if (!getContext()->getSettings().allow_split_buffer_block_with_destination_partition || !destination)
        return return_empty_partition_block();


    auto destination_metadata_snapshot = destination->getInMemoryMetadataPtr();
    if (!destination_metadata_snapshot->hasPartitionKey())
        return return_empty_partition_block();

    Block block_copy = block;

    /// After expression execution partition key columns will be added to block_copy with names regarding partition function.
    auto partition_key_names_and_types
        = MergeTreePartition::executePartitionByExpression(destination_metadata_snapshot, block_copy, getContext());

    ColumnRawPtrs partition_columns;
    partition_columns.reserve(partition_key_names_and_types.size());
    for (const auto & element : partition_key_names_and_types)
        partition_columns.emplace_back(block_copy.getByName(element.name).column.get());

    size_t max_parts = getContext()->getSettingsRef().max_partitions_per_insert_block;
    PODArray<size_t> partition_num_to_first_row;
    IColumn::Selector selector;
    buildScatterSelector(partition_columns, partition_num_to_first_row, selector, max_parts, getContext());

    size_t partitions_count = partition_num_to_first_row.size();
    result.reserve(partitions_count);

    auto get_partition = [&](size_t num)
    {
        Row partition(partition_columns.size());
        for (size_t i = 0; i < partition_columns.size(); ++i)
            partition[i] = (*partition_columns[i])[partition_num_to_first_row[num]];
        return partition;
    };

    if (partitions_count == 1)
    {
        const size_t offset = block.rows();
        result.emplace_back(Block(block), get_partition(0));
        result[0].offsets.push_back(offset);
        result[0].wal_pos.push_back(buffer.increment.get());

        return result;
    }

    for (size_t i = 0; i < partitions_count; ++i)
        result.emplace_back(block.cloneEmpty(), get_partition(i));

    /// scatter block
    for (size_t col = 0; col < block.columns(); ++col)
    {
        MutableColumns scattered = block.getByPosition(col).column->scatter(partitions_count, selector);
        for (size_t i = 0; i < partitions_count; ++i)
            result[i].block.getByPosition(col).column = std::move(scattered[i]);
    }

    /// scatter offset
    std::vector<Int64> last_row_for_partition(partitions_count, -1);
    for (size_t i = 0; i < selector.size(); ++i)
    {
        ++last_row_for_partition[selector[i]];
        for (size_t part_id = 0; part_id < last_row_for_partition.size(); ++part_id)
        {
            const Int64 last_row = last_row_for_partition[part_id];
            if (-1 == last_row)
                continue;
            const size_t offset = static_cast<size_t>(last_row + 1);

            if (result[part_id].offsets.empty())
            {
                result[part_id].offsets.push_back(offset);
                result[part_id].wal_pos.push_back(buffer.increment.get());
            }
            else if (offset > *result[part_id].offsets.rbegin())
            {
                result[part_id].offsets.front() = offset;
            }
        }
    }

    return result;
}

void StorageBuffer::buildScatterSelector(
    const ColumnRawPtrs & columns,
    PODArray<size_t> & partition_num_to_first_row,
    IColumn::Selector & selector,
    size_t max_parts,
    ContextPtr context_) const
{
    /// Use generic hashed variant since partitioning is unlikely to be a bottleneck.
    using Data = HashMap<UInt128, size_t, UInt128TrivialHash>;
    Data partitions_map;

    size_t num_rows = columns[0]->size();
    size_t partitions_count = 0;
    size_t throw_on_limit = context_->getSettingsRef().throw_on_max_partitions_per_insert_block;

    for (size_t i = 0; i < num_rows; ++i)
    {
        Data::key_type key = hash128(i, columns.size(), columns);
        typename Data::LookupResult it;
        bool inserted;
        partitions_map.emplace(key, it, inserted);

        if (inserted)
        {
            if (max_parts && partitions_count >= max_parts && throw_on_limit)
            {
                throw Exception(
                    ErrorCodes::TOO_MANY_PARTS,
                    "Too many partitions for single INSERT block (more than {}). "
                    "The limit is controlled by 'max_partitions_per_insert_block' setting. "
                    "Large number of partitions is a common misconception. "
                    "It will lead to severe negative performance impact, including slow server startup, "
                    "slow INSERT queries and slow SELECT queries. Recommended total number of partitions "
                    "for a table is under 1000..10000. Please note, that partitioning is not intended "
                    "to speed up SELECT queries (ORDER BY key is sufficient to make range queries fast). "
                    "Partitions are intended for data manipulation (DROP PARTITION, etc).",
                    max_parts);
            }

            partition_num_to_first_row.push_back(i);
            it->getMapped() = partitions_count;

            ++partitions_count;

            /// Optimization for common case when there is only one partition - defer selector initialization.
            if (partitions_count == 2)
            {
                selector = IColumn::Selector(num_rows);
                std::fill(selector.begin(), selector.begin() + i, 0);
            }
        }

        if (partitions_count > 1)
            selector[i] = it->getMapped();
    }
    // Checking partitions per insert block again here outside the loop above
    // so we can log the total number of partitions that would have parts created
    if (max_parts && partitions_count >= max_parts && !throw_on_limit)
    {
        const auto & client_info = context_->getClientInfo();

        LOG_WARNING(
            log,
            "INSERT query from initial_user {} (query ID: {}) inserted a block "
            "that created parts in {} partitions. This is being logged "
            "rather than throwing an exception as throw_on_max_partitions_per_insert_block=false.",
            client_info.initial_user,
            client_info.initial_query_id,
            partitions_count);
    }
}

bool StorageBuffer::enableWriteAheadLog() const
{
    if (getContext()->getSettingsRef().allow_experimental_buffer_wal && getBufferSettingsRef().enable_buffer_wal)
        return true;

    return false;
}

size_t StorageBuffer::caculate_flush_partition_count(const double max_avg_ratio, const double max_total_ratio,
        const double max_total_threshold, const size_t max_partition_count)
{
    if (max_avg_ratio < 1)
        return 2;

    if (max_total_ratio < 0)
        return 2;

    if (max_avg_ratio == 1)
        return max_partition_count;

    if (max_total_ratio > max_total_threshold)
        return 2;

    double ratio1 = 10 * (std::log(max_avg_ratio - 1) / std::log(0.5) + 5);
    constexpr  double max_ratio = 100;
    constexpr double min_ratio = 0;
    ratio1 = std::min(ratio1, max_ratio);
    ratio1 = std::max(ratio1, min_ratio);

    double ratio2 = 0;
    if (max_total_threshold <= 0)
        ratio2 = max_ratio;
    else
        ratio2 = -(max_ratio / max_total_threshold) * max_total_ratio  + 100;

    ratio2 = std::min(ratio2, max_ratio);
    ratio2 = std::max(ratio2, min_ratio);

    const double ratio = ratio1 * 0.5 + ratio2 * 0.5;
    const double flush_partition_count_double = static_cast<double>(max_partition_count) * ratio / 100;
    size_t flush_partition_count_int = static_cast<size_t>(flush_partition_count_double);

    flush_partition_count_int = std::max<size_t>(flush_partition_count_int, 2);
    flush_partition_count_int = std::min(flush_partition_count_int, max_partition_count);

    return flush_partition_count_int;
}

bool StorageBuffer::enumeratePartitionBuffer(Buffer & buffer, time_t time_passed, BufferBlockAndOffsetWithPartitionMap & map) const
{
    size_t rows = buffer.bufferRows();
    size_t bytes = buffer.bufferBytes();

    if (!checkThresholdsImpl(/* direct= */ false, rows, bytes, time_passed))
        return false;

    BufferBytesMap bytes_to_partition_id_map;
    size_t max_major_partition_bytes = 0;
    std::optional<String> select_major_partition = std::nullopt;
    for (auto & [partition_id, block_and_offset] : buffer.data_map)
    {
        if (block_and_offset.block.bytes() > max_major_partition_bytes)
        {
            max_major_partition_bytes = block_and_offset.block.bytes();
            select_major_partition = partition_id;
        }

        bytes_to_partition_id_map.emplace(block_and_offset.block.bytes(), partition_id);
    }

    if (!select_major_partition)
        return false;

    size_t partition_count = buffer.data_map.size();

    const double max_avg_ratio =  (max_major_partition_bytes * 1.0) / ((bytes + 1) * 1.0 / partition_count);
    const double max_total_ratio = (max_major_partition_bytes * 1.0) / (bytes + 1);
    double max_total_threshold = getContext()->getSettingsRef().min_memory_ratio_for_max_partition_to_total * 100;
    max_total_threshold = std::min(max_total_threshold, 100.0);
    max_total_threshold = std::max(max_total_threshold, 0.0);

    size_t throw_on_limit = getContext()->getSettingsRef().throw_on_max_partitions_per_insert_block;
    size_t partition_limit = getContext()->getSettingsRef().max_partitions_to_destination;

    size_t max_partition_count = partition_count / 3;
    max_partition_count = std::min(max_partition_count, throw_on_limit);
    if (partition_limit > 0)
        max_partition_count = std::min(max_partition_count, partition_limit);

    size_t select_count = caculate_flush_partition_count(max_avg_ratio, max_total_ratio, max_total_threshold, max_partition_count);

    LOG_TRACE(log, "Caculate flush partition max_avg_ratio:{} max_total_ratio:{} max_total_threshold:{} max_partition_count:{} result:{}",
        max_avg_ratio, max_total_ratio, max_total_threshold, max_partition_count, select_count);

    if (select_count > 10)
        LOG_INFO(log, "The caculated flush partition's result begin to increace, max_avg_ratio:{} "
                      "max_total_ratio:{} max_total_threshold:{} max_partition_count:{} result:{}",
        max_avg_ratio, max_total_ratio, max_total_threshold, max_partition_count, select_count);

    if (max_major_partition_bytes * 2 < bytes)
        LOG_WARNING(
            log,
            "Max major partition bytes:{} is too small total bytes:{} partiton size:{} avg bytes:{}",
            max_major_partition_bytes,
            bytes,
            buffer.data_map.size(),
            bytes / buffer.data_map.size());


    size_t top_count = select_count / 2;
    size_t bottom_count = select_count - (select_count / 2);

    BufferPartitionSet buffer_partition_set;
    buffer_partition_set.emplace(select_major_partition.value());
    size_t top_index = 0;
    for (auto & [block_bytes, partition_id] : bytes_to_partition_id_map)
    {
        if (top_index == 0)
        {
            top_index++;
            continue;
        }

        if (top_index >= top_count)
            break;

        buffer_partition_set.emplace(partition_id);
        top_index++;
    }


    if ((buffer.history_minor_partition_index + 1) >= static_cast<Int64>(buffer.data_map.size()))
        buffer.history_minor_partition_index = -1;

    Int64 minor_partition_index = 0;
    size_t select_minor_count = 0;
    for (auto & [partition_id, block_and_offset] : buffer.data_map)
    {
        if (minor_partition_index > buffer.history_minor_partition_index)
        {
            if (select_minor_count >= bottom_count)
                break;

            if (!buffer_partition_set.contains(partition_id))
            {
                buffer_partition_set.emplace(partition_id);
                buffer.history_minor_partition_index = minor_partition_index;
                select_minor_count++;
            }
        }

        minor_partition_index++;
    }

    for (const auto & partition_id : buffer_partition_set)
        map.emplace(partition_id, buffer.data_map.at(partition_id));

    return true;
}

void StorageBuffer::writeDropPartLog(Buffer & buffer, BufferBlockAndOffsetWithPartitionMap & map) const
{
    /// write drop part log
    for (auto & [partition_id, block_and_offset] : map)
        for (const auto wal_pos : block_and_offset.wal_pos)
        {
            buffer.write_ahead_log->dropBlock(wal_pos);
            LOG_TRACE(log, "Drop buffer:{}'s WAL block partition:{} wal_pos:{}",
                    static_cast<void *>(&buffer), partition_id, wal_pos);
        }

}

bool StorageBuffer::rebuildPartLog(Buffer & buffer, bool force_rebuild) const
{
    if (buffer.data_map.empty())
    {
        buffer.write_ahead_log->truncate(0);
        LOG_TRACE(log, "Rebuild buffer:{}'s WAL all partition wal_pos:0 rows:0", static_cast<void *>(&buffer));
        return true;
    }

    if (force_rebuild || buffer.write_ahead_log->file_size() > (BufferWriteAheadLog::MAX_WAL_BYTES / 2)
        || (buffer.write_ahead_log->file_size() > (BufferWriteAheadLog::MAX_WAL_BYTES / 10)
            && (buffer.bufferBytes() * 10) < max_thresholds.bytes))
    {
        buffer.write_ahead_log->truncate(0);
        for (auto & [partition_id, block_and_offset] : buffer.data_map)
        {
            /// merge multi data part to one and select first wal_pos
            std::vector<size_t> tmp_offsets;
            size_t start_rows = 0;
            start_rows = std::accumulate(block_and_offset.offsets.begin(), block_and_offset.offsets.end(), start_rows);
            tmp_offsets.push_back(start_rows);
            block_and_offset.offsets.swap(tmp_offsets);
            block_and_offset.wal_pos.erase(block_and_offset.wal_pos.begin() + 1, block_and_offset.wal_pos.end());
            /// flush to wal
            buffer.write_ahead_log->addBlock(block_and_offset.wal_pos.front(), block_and_offset.block);

            LOG_TRACE(log, "Rebuild buffer:{}'s WAL partition:{} wal_pos:{} rows:{} bytes:{}",
                    static_cast<void *>(&buffer), partition_id, block_and_offset.wal_pos.front(), block_and_offset.block.rows(), block_and_offset.block.bytes());
        }
        return true;
    }
    return false;
}

void StorageBuffer::removeFlushDataInBuffer(Buffer & buffer, BufferBlockAndOffsetWithPartitionMap & map)
{
    for (auto & [partition_id, block_and_offset] : map)
    {
        if (auto it = buffer.data_map.find(partition_id); it != buffer.data_map.end())
        {
            if (block_and_offset.offsets.size() >= it->second.offsets.size())
            {
                buffer.data_map.erase(it);
                continue;
            }

            it->second.removeBlockAndOffset(log, block_and_offset);
            it->second.checkBLockAndOffset(log);
        }
    }
}

bool StorageBuffer::waitForFlush(Int64 milliseconds)
{
    std::unique_lock<std::mutex> lock(flush_condition_lock);
    wait_number++;
    return flush_condition.wait_for(lock, std::chrono::milliseconds(milliseconds), [this] { return wait_number == 0; });
}

void StorageBuffer::notifyAllWaitForFlush()
{
    std::unique_lock<std::mutex> lock(flush_condition_lock);
    wait_number = 0;
    lock.unlock();

    flush_condition.notify_all();
}

String StorageBuffer::getPartitionIdFromRow(const Row & partition_row, const StoragePtr & destination) const
{
    String partition_id;

    if (!getContext()->getSettings().allow_split_buffer_block_with_destination_partition)
        return partition_id;

    if (partition_row.empty())
        return partition_id;

    if (!destination)
        return partition_id;

    const StorageMetadataPtr destination_metadata_snapshot = destination->getInMemoryMetadataPtr();

    if (!destination_metadata_snapshot || !destination_metadata_snapshot->hasPartitionKey())
        return partition_id;

    const MergeTreePartition partition(partition_row);
    partition_id = partition.getID(destination_metadata_snapshot->getPartitionKey().sample_block);
    return partition_id;
}

void StorageBuffer::drop()
{
    for (const auto & disk : storage_policy->getDisks())
        BufferWriteAheadLog::dropAllWriteAheadLogs(disk, relative_data_path);
}


void registerStorageBuffer(StorageFactory & factory)
{
    /** Buffer(db, table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
      *
      * db, table - in which table to put data from buffer.
      * num_buckets - level of parallelism.
      * min_time, max_time, min_rows, max_rows, min_bytes, max_bytes - conditions for flushing the buffer,
      * flush_time, flush_rows, flush_bytes - conditions for flushing.
      */

    factory.registerStorage(
        "Buffer",
        [](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;

            if (engine_args.size() < 9 || engine_args.size() > 12)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Storage Buffer requires from 9 to 12 parameters: "
                    " destination_database, destination_table, num_buckets, min_time, max_time, min_rows, "
                    "max_rows, min_bytes, max_bytes[, flush_time, flush_rows, flush_bytes].");

            // Table and database name arguments accept expressions, evaluate them.
            engine_args[0] = evaluateConstantExpressionForDatabaseName(engine_args[0], args.getLocalContext());
            engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.getLocalContext());

            // After we evaluated all expressions, check that all arguments are
            // literals.
            for (size_t i = 0; i < engine_args.size(); ++i)
            {
                if (!typeid_cast<ASTLiteral *>(engine_args[i].get()))
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Storage Buffer expects a literal as an argument #{}, got '{}'"
                        " instead",
                        i,
                        engine_args[i]->formatForErrorMessage());
                }
            }

            size_t i = 0;

            String destination_database = checkAndGetLiteralArgument<String>(engine_args[i++], "destination_database");
            String destination_table = checkAndGetLiteralArgument<String>(engine_args[i++], "destination_table");

            UInt64 num_buckets = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);

            StorageBuffer::Thresholds min;
            StorageBuffer::Thresholds max;
            StorageBuffer::Thresholds flush;

            min.time = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
            max.time = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
            min.rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
            max.rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
            min.bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
            max.bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
            if (engine_args.size() > i)
                flush.time = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
            if (engine_args.size() > i)
                flush.rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
            if (engine_args.size() > i)
                flush.bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);

            /// If destination_id is not set, do not write data from the buffer, but simply empty the buffer.
            StorageID destination_id = StorageID::createEmpty();
            if (!destination_table.empty())
            {
                destination_id.database_name = args.getContext()->resolveDatabase(destination_database);
                destination_id.table_name = destination_table;
            }

            /// todo support storage policy setting
            DiskPtr disk = args.getContext()->getDisk("default");
            StoragePolicyPtr storage_policy = args.getContext()->getStoragePolicy("default");

            BufferSettings buffer_settings;
            if (args.storage_def->settings)
                buffer_settings.loadFromQuery(*args.storage_def);

            return std::make_shared<StorageBuffer>(
                args.table_id,
                args.columns,
                args.constraints,
                args.comment,
                args.getContext(),
                num_buckets,
                min,
                max,
                flush,
                destination_id,
                static_cast<bool>(args.getLocalContext()->getSettingsRef().insert_allow_materialized_columns),
                args.relative_data_path,
                storage_policy,
                buffer_settings);
        },
        {
            .supports_settings = true,
            .supports_parallel_insert = true,
            .supports_schema_inference = true,
        });
}

}
