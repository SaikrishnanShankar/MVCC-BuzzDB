#pragma once
#include "common/macros.h"
#include <vector>
#include <map>

namespace buzzdb {

class LogManager {
public:
    enum class LogType {
        UPDATE,
        NEW_VERSION,
        TXN_BEGIN,
        TXN_COMMIT,
        TXN_ABORT,
        CHECKPOINT
    };
    
    void log_update(uint64_t txn_id, uint64_t page_id, uint64_t slot_id,
                   const char* old_data, const char* new_data, uint32_t size);
    
    void log_new_version(uint64_t txn_id, uint64_t page_id, uint64_t slot_id,
                        uint64_t version_id);
    
    void recovery(BufferManager& buffer, VersionStore& versions);
    
private:
    struct LogRecord {
        LogType type;
        uint64_t txn_id;
        uint64_t timestamp;
        std::vector<char> data;
    };
    
    std::vector<LogRecord> log_;
    std::map<uint64_t, uint64_t> txn_start_pos_;
};

} // namespace buzzdb