#pragma once

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "transaction/transaction_manager.h"
#include "buffer/buffer_manager.h"
#include "log/log_manager.h"

namespace buzzdb {

class LogManager;
class BufferManager;

enum class TransactionStatus {
    INVALID,
    RUNNING,
    COMMITTED,
    ABORTED
};

class Transaction {
public:
    Transaction();
    Transaction(uint64_t txn_id, bool started);
    Transaction(uint64_t txn_id, bool started, uint64_t start_timestamp);

    uint64_t txn_id_;
    bool started_;
    uint64_t start_timestamp_;
    uint64_t commit_timestamp_;
    TransactionStatus status_;
    
    std::vector<uint64_t> modified_pages_;
    std::unordered_set<uint64_t> read_set_;
    std::unordered_set<uint64_t> write_set_;
};


class TransactionManager {
public:
    TransactionManager(LogManager &log_manager, BufferManager &buffer_manager);

    ~TransactionManager();

    void reset(LogManager &log_manager);
    uint64_t start_txn();
    void commit_txn(uint64_t txn_id);
    void abort_txn(uint64_t txn_id);
    void add_modified_page(uint64_t txn_id, uint64_t page_id);
    bool is_visible(uint64_t txn_id, uint64_t tuple_begin_ts, uint64_t tuple_end_ts);
    TransactionStatus get_txn_status(uint64_t txn_id);
    uint64_t get_txn_start_timestamp(uint64_t txn_id);
    bool is_active(uint64_t txn_id);
    void garbage_collect();
    bool check_conflict(uint64_t txn_id, uint64_t tuple_id);
    void track_read(uint64_t txn_id, uint64_t tuple_id);
    void track_write(uint64_t txn_id, uint64_t tuple_id);

private:
    LogManager &log_manager_;
    BufferManager &buffer_manager_;
    uint64_t transaction_counter_;
    uint64_t timestamp_counter_;
    std::unordered_map<uint64_t, Transaction> transaction_table_;
    std::unordered_set<uint64_t> active_txns_;
};

}  // namespace buzzdb