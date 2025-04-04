#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <vector>

#include "log/log_manager.h"
#include "buffer/buffer_manager.h"
#include "common/macros.h"

namespace buzzdb {

class Transaction {
public:
    Transaction();
    Transaction(uint64_t txn_id, bool started);
    
    uint64_t txn_id_;
    uint64_t timestamp_;
    bool started_;
    bool read_only_;
    std::vector<uint64_t> modified_pages_;
    std::set<uint64_t> read_set_;
};

class TransactionManager {
public:
    TransactionManager(LogManager &log_manager, BufferManager &buffer_manager);
    ~TransactionManager();
    
    void reset(LogManager &log_manager);
    uint64_t start_txn(bool read_only = false);
    void commit_txn(uint64_t txn_id);
    void abort_txn(uint64_t txn_id);
    void add_modified_page(uint64_t txn_id, uint64_t page_id);
    void add_to_read_set(uint64_t txn_id, uint64_t page_id);
    
    LogManager &log_manager_;
    BufferManager &buffer_manager_;
    std::atomic<uint64_t> transaction_counter_;
    std::atomic<uint64_t> timestamp_counter_; 
    
private:
    std::map<uint64_t, Transaction> transaction_table_;
    std::mutex transaction_table_mutex_;
};

} // namespace buzzdb