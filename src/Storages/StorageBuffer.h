#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <Common/ThreadPool.h>

#include <Poco/Event.h>

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>

#include <Disks/StoragePolicy.h>

#include <Storages/BufferSettings.h>
#include <Storages/BufferWriteAheadLog.h>
#include <Common/SimpleIncrement.h>

namespace Poco
{
class Logger;
}


namespace DB
{


/** During insertion, buffers the data in the RAM until certain thresholds are exceeded.
  * When thresholds are exceeded, flushes the data to another table.
  * When reading, it reads both from its buffers and from the subordinate table.
  *
  * The buffer is a set of num_shards blocks.
  * When writing, select the block number by the remainder of the `ThreadNumber` division by `num_shards` (or one of the others),
  *  and add rows to the corresponding block.
  * When using a block, it is locked by some mutex. If during write the corresponding block is already occupied
  *  - try to lock the next block in a round-robin fashion, and so no more than `num_shards` times (then wait for lock).
  * Thresholds are checked on insertion, and, periodically, in the background thread (to implement time thresholds).
  * Thresholds act independently for each shard. Each shard can be flushed independently of the others.
  * If a block is inserted into the table, which itself exceeds the max-thresholds, it is written directly to the subordinate table without buffering.
  * Thresholds can be exceeded. For example, if max_rows = 1 000 000, the buffer already had 500 000 rows,
  *  and a part of 800 000 rows is added, then there will be 1 300 000 rows in the buffer, and then such a block will be written to the subordinate table.
  *
  * There are also separate thresholds for flush, those thresholds are checked only for non-direct flush.
  * This maybe useful if you do not want to add extra latency for INSERT queries,
  * so you can set max_rows=1e6 and flush_rows=500e3, then each 500e3 rows buffer will be flushed in background only.
  *
  * When you destroy a Buffer table, all remaining data is flushed to the subordinate table.
  * The data in the buffer is not replicated, not logged to disk, not indexed. With a rough restart of the server, the data is lost.
  */
class StorageBuffer final : public IStorage, public WithContext
{
friend class BufferSource;
friend class BufferSink;

public:
    struct Thresholds
    {
        time_t time = 0;  /// The number of seconds from the insertion of the first row into the block.
        size_t rows = 0;  /// The number of rows in the block.
        size_t bytes = 0; /// The number of (uncompressed) bytes in the block.
    };

    /** num_shards - the level of internal parallelism (the number of independent buffers)
      * The buffer is flushed if all minimum thresholds or at least one of the maximum thresholds are exceeded.
      */
    StorageBuffer(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        size_t num_shards_,
        const Thresholds & min_thresholds_,
        const Thresholds & max_thresholds_,
        const Thresholds & flush_thresholds_,
        const StorageID & destination_id,
        bool allow_materialized_,
        const String & relative_data_path_,
        StoragePolicyPtr storage_policy_,
        const BufferSettings & buffer_settings_);

    std::string getName() const override { return "Buffer"; }

    bool storesDataOnDisk() const override { return true; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool supportsParallelInsert() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool /*async_insert*/) override;

    void startup() override;
    /// Flush all buffers into the subordinate table and stop background thread.
    void flushAndPrepareForShutdown() override;
    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr context) override;

    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override;
    bool supportsFinal() const override { return true; }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;

    /// The structure of the subordinate table is not checked and does not change.
    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & table_lock_holder) override;

    std::optional<UInt64> totalRows(const Settings & settings) const override;
    std::optional<UInt64> totalBytes(const Settings & settings) const override;

    std::optional<UInt64> lifetimeRows() const override { return lifetime_writes.rows; }
    std::optional<UInt64> lifetimeBytes() const override { return lifetime_writes.bytes; }

    String getLogName() const { return log->name(); }

    String getRelativeDataPath() const { return relative_data_path; }

    void drop() override;

    const BufferSettings & getBufferSettingsRef() const { return buffer_settings; }

    void truncate(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /* metadata_snapshot */,
        ContextPtr /* context */,
        TableExclusiveLockHolder &) override;

private:
    using WriteAheadLogPtr = std::shared_ptr<BufferWriteAheadLog>;
    using WriteAheadLogDiskMap = std::map<String, DiskPtr>;
    using BufferBytesMap = std::map<size_t, String, std::greater<>>;
    using BufferPartitionSet = std::set<String>;

    struct BlockAndOffsetWithPartition
    {
        Block block;
        Row partition;
        /// for row's offset
        std::vector<size_t> offsets;
        /// for wal data part metadata
        std::vector<size_t> wal_pos;

        BlockAndOffsetWithPartition(Block && block_, Row && partition_) : block(block_), partition(std::move(partition_)) { }

        void appendBlockAndOffset(const LoggerPtr & log_, BlockAndOffsetWithPartition & block_and_offset);
        void appendOffset(const LoggerPtr & log_, BlockAndOffsetWithPartition & block_and_offset);
        void removeBlockAndOffset(const LoggerPtr & log_, BlockAndOffsetWithPartition & block_and_offset);
        void checkBLockAndOffset(const LoggerPtr & log_) const;
    };

    /// key is partition_id
    using BufferBlockAndOffsetWithPartitionMap = std::map<String, BlockAndOffsetWithPartition>;
    using BufferBlocksAndOffsetWithPartition = std::vector<BlockAndOffsetWithPartition>;

    struct Buffer
    {
        time_t first_write_time = 0;
        /// Block data;
        BufferBlockAndOffsetWithPartitionMap data_map;
        /// for low bytes partition block flush round robin
        Int64 history_minor_partition_index = -1;

        WriteAheadLogPtr write_ahead_log;

        /// Schema version, checked to avoid mixing blocks with different sets of columns, from
        /// before and after an ALTER. There are some remaining mild problems if an ALTER happens
        /// in the middle of a long-running INSERT:
        ///  * The data produced by the INSERT after the ALTER is not visible to SELECTs until flushed.
        ///    That's because BufferSource skips buffers with old metadata_version instead of converting
        ///    them to the latest schema, for simplicity.
        ///  * If there are concurrent INSERTs, some of which started before the ALTER and some started
        ///    after, then the buffer's metadata_version will oscillate back and forth between the two
        ///    schemas, flushing the buffer each time. This is probably fine because long-running INSERTs
        ///    usually don't produce lots of small blocks.
        int32_t metadata_version = 0;

        /// For wal data part numbers.
        SimpleIncrement increment;

        bool has_flushed_once = false;

        std::unique_lock<std::mutex> lockForReading() const;
        std::unique_lock<std::mutex> lockForWriting() const;
        std::unique_lock<std::mutex> tryLock() const;

        size_t bufferRows() const;
        size_t bufferBytes() const;
        size_t bufferAllocatedBytes() const;

    private:
        mutable std::mutex mutex;

        std::unique_lock<std::mutex> lockImpl(bool read) const;
    };

    /// There are `num_shards` of independent buffers.
    const size_t num_shards;
    std::unique_ptr<ThreadPool> flush_pool;
    std::vector<Buffer> buffers;

    const Thresholds min_thresholds;
    const Thresholds max_thresholds;
    const Thresholds flush_thresholds;

    StorageID destination_id;
    bool allow_materialized;
    /// Relative path data, changes during rename for ordinary databases use
    /// under lockForShare if rename is possible.
    String relative_data_path;
    const StoragePolicyPtr storage_policy;

    BufferSettings buffer_settings;

    struct Writes
    {
        std::atomic<size_t> rows = 0;
        std::atomic<size_t> bytes = 0;
    };
    Writes lifetime_writes;
    Writes total_writes;

    LoggerPtr log;

    void flushAllBuffers(bool check_thresholds = true);
    bool flushSomeBuffer(Buffer & buffer);
    bool flushAllBuffer(Buffer & buffer);
    bool
    checkThresholds(const Buffer & buffer, time_t current_time) const;
    bool checkThresholdsImpl(bool direct, size_t rows, size_t bytes, time_t time_passed) const;

    /// `table` argument is passed, as it is sometimes evaluated beforehand. It must match the `destination`.
    void writeBlockToDestination(const Block & block, StoragePtr table) const;

    void backgroundFlush();
    void reschedule();
    void schedule();

    StoragePtr getDestinationTable() const;

    BackgroundSchedulePool & bg_pool;
    BackgroundSchedulePoolTaskHolder flush_handle;


    void initializeDirectoriesAndFormatVersion(const std::string & relative_data_path_, bool attach, bool need_create_directories);

    void load_buffer_wal();

    WriteAheadLogPtr getWriteAheadLog(size_t index, const WriteAheadLogDiskMap & wal_map);

    void clearBuffer();

    BufferBlocksAndOffsetWithPartition splitBlockIntoParts(Block && block, Buffer & buffer, const StoragePtr & destination);

    void buildScatterSelector(
        const ColumnRawPtrs & columns,
        PODArray<size_t> & partition_num_to_first_row,
        IColumn::Selector & selector,
        size_t max_parts,
        ContextPtr context_) const;

    bool enableWriteAheadLog() const;

    static size_t caculate_flush_partition_count(double max_avg_ratio, double max_total_ratio,
        double max_total_threshold, size_t max_partition_count);

    bool enumeratePartitionBuffer(Buffer & buffer, time_t time_passed, BufferBlockAndOffsetWithPartitionMap & map) const;

    void writeDropPartLog(Buffer & buffer, BufferBlockAndOffsetWithPartitionMap & map) const;

    bool rebuildPartLog(Buffer & buffer, bool force_rebuild) const;

    void removeFlushDataInBuffer(Buffer & buffer, BufferBlockAndOffsetWithPartitionMap & map);

    std::condition_variable flush_condition;
    std::mutex flush_condition_lock;
    size_t wait_number;

    bool waitForFlush(Int64 milliseconds);
    void notifyAllWaitForFlush();

    String getPartitionIdFromRow(const Row & partition_row, const StoragePtr & destination) const;
};

}
