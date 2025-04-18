#pragma once

#include <atomic>
#include <memory>
#include <map>
#include <unordered_set>

#include "storage/test_file.h"
#include "buffer/buffer_manager.h"

namespace buzzdb {
class BufferManager;
class LogManager {
public:
    enum class LogRecordType {
        INVALID_RECORD_TYPE,
        ABORT_RECORD,
        COMMIT_RECORD,
        UPDATE_RECORD,
        BEGIN_RECORD,
        CHECKPOINT_RECORD,
        VERSION_CREATE_RECORD,
        VERSION_DELETE_RECORD
    };

    LogManager(File* log_file);
    ~LogManager();

    void log_abort(uint64_t txn_id, BufferManager& buffer_manager);
    void log_commit(uint64_t txn_id, uint64_t commit_timestamp = 0);
    void log_update(uint64_t txn_id, uint64_t page_id, uint64_t length, 
                    uint64_t offset, std::byte* before_img, std::byte* after_img);
    void log_txn_begin(uint64_t txn_id, uint64_t start_timestamp = 0);
    void log_version_create(uint64_t txn_id, uint64_t page_id, uint64_t slot_id,
                            uint64_t begin_timestamp);
    void log_version_delete(uint64_t txn_id, uint64_t page_id, uint64_t slot_id,
                            uint64_t end_timestamp);
    void log_checkpoint(BufferManager &buffer_manager);
    void recovery(BufferManager &buffer_manager);
    void rollback_txn(uint64_t txn_id, BufferManager &buffer_manager);
    uint64_t get_total_log_records();
    uint64_t get_total_log_records_of_type(LogRecordType type);
    void reset(File* log_file);

private:
    struct LogRecordHeader {
        LogRecordType type;
        uint64_t txn_id;
        uint64_t prev_offset;
        uint64_t length;
        uint64_t timestamp;
    };

    File* log_file_;
    size_t current_offset_ = 0;
    std::map<uint64_t, uint64_t> txn_id_to_first_log_record;
    std::map<LogRecordType, uint64_t> log_record_type_to_count;

    void write_log_record_header(LogRecordHeader &header);
    LogRecordHeader read_log_record_header(size_t offset);
    void write_data(size_t offset, void* data, size_t length);
    void read_data(size_t offset, void* data, size_t length);
    void redo_version_create(uint64_t txn_id, uint64_t page_id, uint64_t slot_id, 
                             uint64_t begin_timestamp, BufferManager& buffer_manager);
    void redo_version_delete(uint64_t txn_id, uint64_t page_id, uint64_t slot_id,
                             uint64_t end_timestamp, BufferManager& buffer_manager);
};

}  // namespace buzzdbs
