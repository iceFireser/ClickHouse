//
// Created by root on 23-12-18.
//

#include <vector>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Storages/BufferWriteAheadLog.h>
#include <Storages/StorageBuffer.h>
#include <sys/time.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Common/logger_useful.h>

namespace DB
{


namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT_VERSION;
extern const int CANNOT_READ_ALL_DATA;
extern const int CORRUPTED_DATA;
}

String BufferWriteAheadLog::ActionMetadata::toJSON() const
{
    Poco::JSON::Object json;


    json.set(JSON_KEY_NUMBER, toString(number));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    json.stringify(oss);

    return oss.str();
}

void BufferWriteAheadLog::ActionMetadata::fromJSON(const String & buf)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(buf).extract<Poco::JSON::Object::Ptr>();

    if (json->has(JSON_KEY_NUMBER))
        number = parseFromString<UInt64>(json->getValue<std::string>(JSON_KEY_NUMBER));
}

void BufferWriteAheadLog::ActionMetadata::read(ReadBuffer & meta_in)
{
    readIntBinary(min_compatible_version, meta_in);
    if (min_compatible_version > WAL_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "WAL metadata version {} is not compatible with this ClickHouse version",
            toString(min_compatible_version));

    size_t metadata_size;
    readVarUInt(metadata_size, meta_in);

    if (metadata_size == 0)
        return;

    String buf(metadata_size, ' ');
    meta_in.readStrict(buf.data(), metadata_size);

    fromJSON(buf);
}

void BufferWriteAheadLog::ActionMetadata::write(WriteBuffer & meta_out) const
{
    writeIntBinary(min_compatible_version, meta_out);

    String ser_meta = toJSON();

    writeVarUInt(static_cast<UInt32>(ser_meta.length()), meta_out);
    writeString(ser_meta, meta_out);
}


BufferWriteAheadLog::BufferWriteAheadLog(StorageBuffer & storage_, const DiskPtr & disk_, const String & name_)
    : storage(storage_)
    , disk(disk_)
    , name(name_)
    , path(storage.getRelativeDataPath() + name_)
    , pool(storage.getContext()->getSchedulePool())
    , log(&Poco::Logger::get(storage.getLogName() + " (WriteAheadLog)"))
    , block_number(0)
{
    init();

    ///storage.getContext()->getSettings().allow_experimental_hash_functions

    sync_task = pool.createTask(
        "BufferWriteAheadLog::sync",
        [this]
        {
            std::lock_guard lock(write_mutex);
            out->sync();
            sync_scheduled = false;
            sync_cv.notify_all();
        });
}

BufferWriteAheadLog::~BufferWriteAheadLog()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void BufferWriteAheadLog::init()
{
    int flags = (O_APPEND | O_CREAT | O_WRONLY);
    fs::path write_path = fs::path(disk->getPath()) / path;
    out = std::make_unique<WriteBufferFromFile>(write_path, DBMS_DEFAULT_BUFFER_SIZE, flags);

    /// Small hack: in NativeWriter header is used only in `getHeader` method.
    /// To avoid complex logic of changing it during ALTERs we leave it empty.
    block_out = std::make_unique<NativeWriter>(*out, 0, Block{});

    bytes_at_last_sync = 0;
}

void BufferWriteAheadLog::shutdown()
{
    {
        std::unique_lock lock(write_mutex);
        if (shutdown_called)
            return;

        if (sync_scheduled)
            sync_cv.wait(lock, [this] { return !sync_scheduled; });

        shutdown_called = true;
        out->finalize();
        out.reset();
    }

    /// Do it without lock, otherwise inversion between pool lock and write_mutex is possible
    sync_task->deactivate();
}

void BufferWriteAheadLog::sync(std::unique_lock<std::mutex> & lock)
{
    size_t bytes_to_sync = static_cast<size_t>(storage.getBufferSettingsRef().write_ahead_log_bytes_to_fsync);
    time_t time_to_sync = static_cast<size_t>(storage.getBufferSettingsRef().write_ahead_log_interval_ms_to_fsync);
    size_t current_bytes = out->count();

    if (current_bytes - bytes_at_last_sync > bytes_to_sync)
    {
        sync_task->schedule();
        bytes_at_last_sync = current_bytes;
    }
    else if (!sync_scheduled)
    {
        sync_task->scheduleAfter(time_to_sync);
        sync_scheduled = true;
    }

    bool wait_insert_sync = storage.getBufferSettingsRef().wait_insert_sync;
    if (wait_insert_sync)
        sync_cv.wait(lock, [this] { return !sync_scheduled; });
}

UInt64 BufferWriteAheadLog::addBlock(UInt64 wal_pos, const Block & block)
{
    std::unique_lock lock(write_mutex);

    writeIntBinary(WAL_VERSION, *out);

    if (wal_pos <= block_number)
        LOG_WARNING(log, "WAL pos:{} less than or equal block_number: {} ", wal_pos, block_number);

    block_number = wal_pos;
    ActionMetadata metadata{};
    metadata.number = wal_pos;
    metadata.write(*out);

    writeIntBinary(static_cast<UInt8>(ActionType::ADD_BLOCK), *out);
    block_out->write(block);
    block_out->flush();
    sync(lock);

    size_t out_size = static_cast<size_t>(out->size());
    if (out_size > MAX_WAL_BYTES)
        LOG_WARNING(log, "WAL path:{} exceeds maximum length: {} ", path, MAX_WAL_BYTES);

    return wal_pos;
}

void BufferWriteAheadLog::dropBlock(UInt64 block_number_)
{
    std::unique_lock lock(write_mutex);

    ActionMetadata metadata{};
    metadata.number = block_number_;
    metadata.write(*out);

    writeIntBinary(static_cast<UInt8>(ActionType::DROP_BLOCK), *out);
    out->next();
    sync(lock);

    size_t out_size = static_cast<size_t>(out->size());
    if (out_size > MAX_WAL_BYTES)
        LOG_WARNING(log, "WAL path:{} exceeds maximum length: {} ", path, MAX_WAL_BYTES);
}

void BufferWriteAheadLog::restore(Block & out_block, UInt64 & last_block_number_)
{
    std::unique_lock lock(write_mutex);
    auto in = disk->readFile(path);
    NativeReader block_in(*in, 0);
    off_t offset = 0;
    bool stop_read = false;
    std::unordered_map<UInt64, Block> data_part_unordered_map;
    UInt64 last_block_number = 0;

    while (!in->eof() && !stop_read)
    {
        offset = in->getPosition();

        try
        {
            UInt8 version;
            ActionType action_type;
            ActionMetadata metadata;

            readIntBinary(version, *in);
            if (version > 0)
                metadata.read(*in);

            readIntBinary(action_type, *in);

            if (action_type == ActionType::DROP_BLOCK)
            {
                if (auto it = data_part_unordered_map.find(metadata.number); it != data_part_unordered_map.end())
                    data_part_unordered_map.erase(it);
            }
            else if (action_type == ActionType::ADD_BLOCK)
            {
                Block block;
                block = block_in.read();
                if (auto it = data_part_unordered_map.find(metadata.number); it == data_part_unordered_map.end())
                    data_part_unordered_map.emplace(metadata.number, std::move(block));

                if (metadata.number > last_block_number)
                    last_block_number = metadata.number;
            }
            else
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown action type: {}", toString(static_cast<UInt8>(action_type)));
            }
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::CANNOT_READ_ALL_DATA || e.code() == ErrorCodes::UNKNOWN_FORMAT_VERSION
                || e.code() == ErrorCodes::CORRUPTED_DATA)
            {
                LOG_WARNING(log, "WAL file '{}' is broken. {}", path, e.displayText());

                /// If file is broken, stop read and correct it.
                stop_read = true;

                truncate(offset, false);
                break;
            }
            throw;
        }
    }

    for (auto & [number, block] : data_part_unordered_map)
    {
        size_t rows = block.rows();
        size_t old_rows = out_block.rows();
        size_t old_bytes = out_block.bytes();

        if (!out_block)
            out_block = block.cloneEmpty();

        assertBlocksHaveEqualStructure(block, out_block, "Buffer WAL");

        block.checkNumberOfRows();
        out_block.checkNumberOfRows();

        try
        {
            MutableColumnPtr last_col;
            for (size_t column_no = 0, columns = out_block.columns(); column_no < columns; ++column_no)
            {
                const IColumn & col_from = *block.getByPosition(column_no).column.get();

                last_col = IColumn::mutate(std::move(out_block.getByPosition(column_no).column));

                /// In case of ColumnAggregateFunction aggregate states will
                /// be allocated from the query context but can be destroyed from the
                /// server context (in case of background flush), and thus memory
                /// will be leaked from the query, but only tracked memory, not
                /// memory itself.
                ///
                /// To avoid this, prohibit sharing the aggregate states.
                last_col->ensureOwnership();
                last_col->insertRangeFrom(col_from, 0, rows);

                {
                    DENY_ALLOCATIONS_IN_SCOPE;
                    out_block.getByPosition(column_no).column = std::move(last_col);
                }
            }

            LOG_INFO(
                log,
                "Restore path:{} [old rows: {} => rows: {}] [old bytes: {} => bytes: {}]",
                path,
                old_rows,
                out_block.rows(),
                old_bytes,
                out_block.bytes());
        }
        catch (...)
        {
            /// Rollback changes.

            /// But first log exception to get more details in case of LOGICAL_ERROR
            tryLogCurrentException(log, "Caught exception while restore data to buffer..");

            throw;
        }
    }

    last_block_number_ = last_block_number;
}

size_t BufferWriteAheadLog::file_size() const
{
    if (out)
        return out->size();

    return 0;
}

void BufferWriteAheadLog::rotate(const std::unique_lock<std::mutex> &)
{
    max_block_number = block_number;

    String new_name = String(WAL_FILE_NAME) + "_" + toString(min_block_number) + "_" + toString(max_block_number) + WAL_FILE_EXTENSION;

    /// Finalize stream before file rename
    out->finalize();
    disk->replaceFile(path, storage.getRelativeDataPath() + new_name);
    init();

    min_block_number = block_number;
}

void BufferWriteAheadLog::truncate(off_t offset, bool with_lock)
{
    if (with_lock)
    {
        std::unique_lock lock(write_mutex);
        out->truncate(offset);
    }
    else
    {
        out->truncate(offset);
    }
}

void BufferWriteAheadLog::dropAllWriteAheadLogs(DiskPtr disk_to_drop, std::string relative_data_path)
{
    std::vector<std::string> files;
    disk_to_drop->listFiles(relative_data_path, files);
    for (const auto & file : files)
        if (file.starts_with(WAL_FILE_NAME))
            disk_to_drop->removeFile(fs::path(relative_data_path) / file);
}

const Settings & BufferWriteAheadLog::getContextSettings() const
{
    return storage.getContext()->getSettingsRef();
}

} // DB
