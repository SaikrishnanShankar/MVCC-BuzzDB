#include "transaction/transaction_manager.h"

#include <iostream>
#include <algorithm>

#include "common/macros.h"

namespace buzzdb {

Transaction::Transaction() : txn_id_(INVALID_TXN_ID), started_(false), 
                            start_timestamp_(0), commit_timestamp_(0), 
                            status_(TransactionStatus::INVALID) {}

Transaction::Transaction(uint64_t txn_id, bool started) 
    : txn_id_(txn_id), started_(started), 
      start_timestamp_(0), commit_timestamp_(0), 
      status_(TransactionStatus::RUNNING) {}

Transaction::Transaction(uint64_t txn_id, bool started, uint64_t start_timestamp) 
    : txn_id_(txn_id), started_(started), 
      start_timestamp_(start_timestamp), commit_timestamp_(0), 
      status_(TransactionStatus::RUNNING) {}

TransactionManager::TransactionManager(LogManager &log_manager, BufferManager &buffer_manager)
    : log_manager_(log_manager), buffer_manager_(buffer_manager), 
      transaction_counter_(0), timestamp_counter_(0) {}

TransactionManager::~TransactionManager() {}

void TransactionManager::reset(LogManager &log_manager) {
    log_manager_ = log_manager;
    buffer_manager_.discard_all_pages();
    transaction_counter_ = 0;
    timestamp_counter_ = 0; 
    transaction_table_.clear();
    active_txns_.clear();
}

uint64_t TransactionManager::start_txn() {
    uint64_t txn_id = ++transaction_counter_;
    uint64_t start_timestamp = ++timestamp_counter_;
    Transaction txn(txn_id, true, start_timestamp);
    txn.status_ = TransactionStatus::RUNNING;
    transaction_table_[txn_id] = txn;
    active_txns_.insert(txn_id);
    log_manager_.log_txn_begin(txn_id, start_timestamp);
    return txn_id;
}

void TransactionManager::commit_txn(uint64_t txn_id) {
    if (transaction_table_.count(txn_id) == 0) {
        std::cout << "Txn does not exist \n";
        exit(-1);
    }
    auto &txn = transaction_table_[txn_id];
    if (txn.started_) {
        txn.commit_timestamp_ = ++timestamp_counter_;
        txn.status_ = TransactionStatus::COMMITTED;
        buffer_manager_.flush_pages(txn_id);
        
        buffer_manager_.transaction_complete(txn_id);
        log_manager_.log_commit(txn_id, txn.commit_timestamp_);

        txn.started_ = false;
        
        active_txns_.erase(txn_id);
    }
}

void TransactionManager::abort_txn(uint64_t txn_id) {
    if (transaction_table_.count(txn_id) == 0) {
        std::cout << "Txn does not exist \n";
        exit(-1);
    }

    auto &txn = transaction_table_[txn_id];

    if (txn.started_) {
        txn.status_ = TransactionStatus::ABORTED;
            buffer_manager_.discard_pages(txn_id);
        
        buffer_manager_.transaction_complete(txn_id);
        log_manager_.log_abort(txn_id, buffer_manager_);
        txn.started_ = false;
        active_txns_.erase(txn_id);
    }
}

void TransactionManager::add_modified_page(uint64_t txn_id, uint64_t page_id) {
    auto &txn = transaction_table_[txn_id];
    txn.modified_pages_.push_back(page_id);
}

bool TransactionManager::is_visible(uint64_t txn_id, uint64_t tuple_begin_ts, uint64_t tuple_end_ts) {
    if (transaction_table_.count(txn_id) == 0) {
        return false;
    }

    const auto &txn = transaction_table_[txn_id];
    bool created_before_txn = tuple_begin_ts <= txn.start_timestamp_;
    bool deleted_after_txn_or_still_active = (tuple_end_ts == 0) || (tuple_end_ts > txn.start_timestamp_);
    
    return created_before_txn && deleted_after_txn_or_still_active;
}

TransactionStatus TransactionManager::get_txn_status(uint64_t txn_id) {
    if (transaction_table_.count(txn_id) == 0) {
        return TransactionStatus::INVALID;
    }
    return transaction_table_[txn_id].status_;
}

uint64_t TransactionManager::get_txn_start_timestamp(uint64_t txn_id) {
    if (transaction_table_.count(txn_id) == 0) {
        return 0;
    }
    return transaction_table_[txn_id].start_timestamp_;
}

bool TransactionManager::is_active(uint64_t txn_id) {
    return active_txns_.find(txn_id) != active_txns_.end();
}

void TransactionManager::garbage_collect() {
    uint64_t oldest_active_timestamp = UINT64_MAX;
    
    for (auto txn_id : active_txns_) {
        if (transaction_table_.count(txn_id) > 0) {
            uint64_t start_ts = transaction_table_[txn_id].start_timestamp_;
            oldest_active_timestamp = std::min(oldest_active_timestamp, start_ts);
        }
    }
}

bool TransactionManager::check_conflict(uint64_t txn_id, uint64_t tuple_id) {
    for (auto active_txn_id : active_txns_) {
        if (active_txn_id == txn_id) continue;
        
        const auto& txn = transaction_table_[active_txn_id];
        if (txn.write_set_.find(tuple_id) != txn.write_set_.end()) {
            return true;
        }
    }
    return false;
}

void TransactionManager::track_read(uint64_t txn_id, uint64_t tuple_id) {
    if (transaction_table_.count(txn_id) > 0) {
        transaction_table_[txn_id].read_set_.insert(tuple_id);
    }
}

void TransactionManager::track_write(uint64_t txn_id, uint64_t tuple_id) {
    if (transaction_table_.count(txn_id) > 0) {
        transaction_table_[txn_id].write_set_.insert(tuple_id);
    }
}

}  // namespace buzzdb