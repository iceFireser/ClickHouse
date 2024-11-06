#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;

#define LIST_OF_BUFFER_SETTINGS(M, ALIAS) \
    M(Bool, enable_buffer_wal, false, "Write ahead log for inserted. Will decreases performance of inserts", 0) \
    M(Bool, wait_insert_sync, false, "If true will wait for fsync of WAL", 0) \
    /** Inserts settings. */ \
    M(UInt64, write_ahead_log_bytes_to_fsync, 10ULL * 1024 * 1024, "Amount of bytes, accumulated in WAL to do fsync.", 0) \
    M(UInt64, write_ahead_log_interval_ms_to_fsync, 100, "Interval in milliseconds after which fsync for WAL is being done.", 0) \

DECLARE_SETTINGS_TRAITS(BufferSettingsTraits, LIST_OF_BUFFER_SETTINGS)


/** Settings for the Distributed family of engines.
  */
struct BufferSettings : public BaseSettings<BufferSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
