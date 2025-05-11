#include <iostream>
#include <boost/program_options.hpp>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/ZooKeeperDataReader.h>
#include <Coordination/KeeperContext.h>
#include <Common/TerminalSize.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include <Disks/DiskLocal.h>

#include <exception>
#include <filesystem>
#include <mutex>
#include <ranges>
#include <Coordination/Changelog.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/Keeper4LWInfo.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Disks/DiskLocal.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ZstdDeflatingAppendableWriteBuffer.h>
#include <base/errnoToString.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/ProfileEvents.h>
#include <libnuraft/log_val_type.hxx>
#include <libnuraft/log_entry.hxx>
#include <libnuraft/raft_server.hxx>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT_VERSION;
extern const int CHECKSUM_DOESNT_MATCH;

}

ChangelogFileDescriptionPtr keeperconvertGetChangelogFileDescription(const std::filesystem::path & path)
{
    // we can have .bin.zstd so we cannot use std::filesystem stem and extension
    std::string filename_with_extension = path.filename();
    std::string_view filename_with_extension_view = filename_with_extension;

    auto first_dot = filename_with_extension.find('.');
    if (first_dot == std::string::npos)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid changelog file {}", path.generic_string());

    Strings filename_parts;
    boost::split(filename_parts, filename_with_extension_view.substr(0, first_dot), boost::is_any_of("_"));
    if (filename_parts.size() < 3)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid changelog {}", path.generic_string());

    auto result = std::make_shared<ChangelogFileDescription>();
    result->prefix = filename_parts[0];
    result->from_log_index = parse<uint64_t>(filename_parts[1]);
    result->to_log_index = parse<uint64_t>(filename_parts[2]);
    result->extension = std::string(filename_with_extension.substr(first_dot + 1));
    result->path = path.generic_string();
    return result;
}

Checksum keeperconvertComputeRecordChecksum(const ChangelogRecord & record)
{
    SipHash hash;
    hash.update(record.header.version);
    hash.update(record.header.index);
    hash.update(record.header.term);
    hash.update(record.header.value_type);
    hash.update(record.header.blob_size);
    if (record.header.blob_size != 0)
        hash.update(reinterpret_cast<char *>(record.blob->data_begin()), record.blob->size());
    return hash.get64();
}

ChangelogRecord keeperconvertReadChangelogRecord(ReadBuffer & read_buf, const std::string & filepath)
{
    /// Read checksum
    Checksum record_checksum;
    readIntBinary(record_checksum, read_buf);

    /// Read header
    ChangelogRecord record;
    readIntBinary(record.header.version, read_buf);
    readIntBinary(record.header.index, read_buf);
    readIntBinary(record.header.term, read_buf);
    readIntBinary(record.header.value_type, read_buf);
    readIntBinary(record.header.blob_size, read_buf);

    if (record.header.version > CURRENT_CHANGELOG_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported changelog version {} on path {}", static_cast<uint8_t>(record.header.version), filepath);

    /// Read data
    if (record.header.blob_size != 0)
    {
        auto buffer = nuraft::buffer::alloc(record.header.blob_size);
        auto * buffer_begin = reinterpret_cast<char *>(buffer->data_begin());
        read_buf.readStrict(buffer_begin, record.header.blob_size);
        record.blob = buffer;
    }
    else
        record.blob = nullptr;

    /// Compare checksums
    Checksum checksum = keeperconvertComputeRecordChecksum(record);
    if (checksum != record_checksum)
    {
        throw Exception(
            ErrorCodes::CHECKSUM_DOESNT_MATCH,
            "Checksums doesn't match for log {} (version {}), index {}, blob_size {}",
            filepath,
            record.header.version,
            record.header.index,
            record.header.blob_size);
    }

    return record;
}

LogEntryPtr keeperconvertLogEntryFromRecord(const ChangelogRecord & record)
{
    return nuraft::cs_new<nuraft::log_entry>(record.header.term, record.blob, static_cast<nuraft::log_val_type>(record.header.value_type));
}

std::shared_ptr<KeeperStorageBase::RequestForSession> keeperconvertParseRequest(nuraft::buffer & data)
{
    ReadBufferFromNuraftBuffer buffer(data);
    auto request_for_session = std::make_shared<KeeperStorageBase::RequestForSession>();
    readIntBinary(request_for_session->session_id, buffer);

    int32_t length;
    Coordination::read(length, buffer);

    int32_t xid;
    Coordination::read(xid, buffer);

    Coordination::OpNum opnum;

    Coordination::read(opnum, buffer);

    request_for_session->request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_for_session->request->xid = xid;
    request_for_session->request->readImpl(buffer);



    if (!buffer.eof())
    {
        readIntBinary(request_for_session->time, buffer);
    }
    else
        request_for_session->time
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    if (!buffer.eof())
    {
        readIntBinary(request_for_session->zxid, buffer);

        chassert(!buffer.eof());

        request_for_session->digest.emplace();
        readIntBinary(request_for_session->digest->version, buffer);
        if (request_for_session->digest->version != KeeperStorageBase::DigestVersion::NO_DIGEST || !buffer.eof())
            readIntBinary(request_for_session->digest->value, buffer);
    }


    return request_for_session;
}

}




int mainEntryClickHouseKeeperConverter(int argc, char ** argv)
{
    using namespace DB;
    namespace po = boost::program_options;

    po::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
    desc.add_options()
        ("help,h", "produce help message")
        ("type", po::value<std::string>(), "Convert type")
        ("keeper-log-input-path", po::value<std::string>(), "Path to keeper log")
        ("keeper-log-output-path", po::value<std::string>(), "Path to keeper log json")
        ("zookeeper-logs-dir", po::value<std::string>(), "Path to directory with ZooKeeper logs")
        ("zookeeper-snapshots-dir", po::value<std::string>(), "Path to directory with ZooKeeper snapshots")
        ("output-dir", po::value<std::string>(), "Directory to place output clickhouse-keeper snapshot")
    ;
    po::variables_map options;
    po::store(po::command_line_parser(argc, argv).options(desc).run(), options);
    Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel);

    LoggerPtr logger = getLogger("KeeperConverter");
    logger->setChannel(console_channel);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " --type zookeeper_snapshot --zookeeper-logs-dir /var/lib/zookeeper/data/version-2 --zookeeper-snapshots-dir /var/lib/zookeeper/data/version-2 --output-dir /var/lib/clickhouse/coordination/snapshots" << std::endl;
        std::cout << "   or  " << argv[0] << " --type keeper_log --keeper-log-input-path /var/lib/keeper/data/log-1 --keeper-log-output-path /var/lib/keeper/data/log-1.json" << std::endl;
        std::cout << desc << std::endl;
        return 0;
    }

    try
    {
        auto type = options["type"].as<std::string>();
        if (type.empty() || type == "zookeeper_snapshot")
        {
            auto keeper_context = std::make_shared<KeeperContext>(true, std::make_shared<CoordinationSettings>());
            keeper_context->setDigestEnabled(true);
            keeper_context->setSnapshotDisk(std::make_shared<DiskLocal>("Keeper-snapshots", options["output-dir"].as<std::string>()));

            /// TODO(hanfei): support rocksdb here
            DB::KeeperMemoryStorage storage(/* tick_time_ms */ 500, /* superdigest */ "", keeper_context, /* initialize_system_nodes */ false);

            DB::deserializeKeeperStorageFromSnapshotsDir(storage, options["zookeeper-snapshots-dir"].as<std::string>(), logger);
            storage.initializeSystemNodes();

            DB::deserializeLogsAndApplyToStorage(storage, options["zookeeper-logs-dir"].as<std::string>(), logger);
            DB::SnapshotMetadataPtr snapshot_meta = std::make_shared<DB::SnapshotMetadata>(storage.getZXID(), 1, std::make_shared<nuraft::cluster_config>());
            DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snapshot(&storage, snapshot_meta);

            DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> manager(1, keeper_context);
            auto snp = manager.serializeSnapshotToBuffer(snapshot);
            auto file_info = manager.serializeSnapshotBufferToDisk(*snp, storage.getZXID());
            std::cout << "Snapshot serialized to path:" << fs::path(file_info->disk->getPath()) / file_info->path << std::endl;
        }
        else if (type == "keeper_log")
        {
            auto keeper_context = std::make_shared<KeeperContext>(true, std::make_shared<CoordinationSettings>());
            std::filesystem::path keeper_log_path = options["keeper-log-input-path"].as<std::string>();
            std::filesystem::path keeper_log_json_path = options["keeper-log-output-path"].as<std::string>();
            auto changelog_file_desc = DB::keeperconvertGetChangelogFileDescription(keeper_log_path);
            auto disk = std::make_shared<DiskLocal>("Keeper-log", "/");
            changelog_file_desc->disk = disk;

            auto compression_method = chooseCompressionMethod(changelog_file_desc->path, "");
            auto read_buffer_from_file = changelog_file_desc->disk->readFile(changelog_file_desc->path);
            auto read_buf = wrapReadBufferWithCompressionMethod(std::move(read_buffer_from_file), compression_method);

            WriteSettings settings;
            auto write_buffer_into_file = disk->writeFile(keeper_log_json_path.generic_string(), 10 * 1024 * 1024,  WriteMode::Rewrite, settings);

            auto & date_lut = DateLUT::instance();

            while (!read_buf->eof())
            {
                auto record = keeperconvertReadChangelogRecord(*read_buf, changelog_file_desc->path);


                auto log_entry = keeperconvertLogEntryFromRecord(record);

                if (log_entry->get_val_type() != nuraft::app_log)
                    continue;


                auto entry_buf = log_entry->get_buf_ptr();
                auto request_for_session = keeperconvertParseRequest(*entry_buf);

                auto line = fmt::format("session_id: {} time: {} time_str: {} zxid: {} request: {} \n",
                    request_for_session->session_id,
                    request_for_session->time,
                    date_lut.timeToString(request_for_session->time / 1000),
                    request_for_session->zxid,
                    request_for_session->request->toString());

                write_buffer_into_file->write(line.c_str(), line.length());

            }

            write_buffer_into_file->finalize();

        }


    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << '\n';
        return getCurrentExceptionCode();
    }

    return 0;
}
