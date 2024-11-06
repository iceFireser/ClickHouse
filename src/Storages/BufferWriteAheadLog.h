#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Disks/IDisk.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>

namespace DB
{

class StorageBuffer;
class WriteBufferFromFile;

class BufferWriteAheadLog
{
public:
    enum class ActionType : UInt8
    {
        ADD_BLOCK = 0,
        DROP_BLOCK = 1,
    };

    struct ActionMetadata
    {
        UInt8 min_compatible_version = 0;
        UInt64 number = 0;

        void write(WriteBuffer & meta_out) const;
        void read(ReadBuffer & meta_in);

    private:
        static constexpr auto JSON_KEY_NUMBER = "block_number";

        String toJSON() const;
        void fromJSON(const String & buf);
    };

    constexpr static UInt8 WAL_VERSION = 1;
    constexpr static auto WAL_FILE_NAME = "wal";
    constexpr static auto WAL_FILE_EXTENSION = ".bin";
    constexpr static auto DEFAULT_WAL_FILE_NAME = "wal.bin";

    constexpr static size_t MAX_WAL_BYTES = 1024 * 1024 * 512;

    BufferWriteAheadLog(StorageBuffer & storage_, const DiskPtr & disk_, const String & name_ = DEFAULT_WAL_FILE_NAME);

    ~BufferWriteAheadLog();
    void shutdown();

    UInt64 addBlock(UInt64 wal_pos, const Block & block);
    void dropBlock(UInt64 block_number_);
    void truncate(off_t offset, bool with_lock = true);

    void restore(Block & out_block, UInt64 & last_block_number_);

    size_t file_size() const;

    /// Drop all write ahead logs from disk. Useful during table drop.
    static void dropAllWriteAheadLogs(DiskPtr disk_to_drop, std::string relative_data_path);

private:
    const StorageBuffer & storage;
    DiskPtr disk;
    String name;
    String path;

    std::unique_ptr<WriteBufferFromFile> out;
    std::unique_ptr<NativeWriter> block_out;

    BackgroundSchedulePool & pool;
    BackgroundSchedulePoolTaskHolder sync_task;
    std::condition_variable sync_cv;

    size_t bytes_at_last_sync = 0;
    bool sync_scheduled = false;
    bool shutdown_called = false;

    mutable std::mutex write_mutex;

    Poco::Logger * log;

    UInt64 block_number;
    Int64 min_block_number = std::numeric_limits<Int64>::max();
    Int64 max_block_number = -1;

    void init();
    [[maybe_unused]] void rotate(const std::unique_lock<std::mutex> & lock);
    void sync(std::unique_lock<std::mutex> & lock);

    const Settings & getContextSettings() const;
};

} // DB
