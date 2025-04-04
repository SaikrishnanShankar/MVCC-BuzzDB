#include "./../transaction/transaction_manager.h"
#include "./../mvto/mvto.h"

#include <iostream>

namespace buzzdb {

Transaction::Transaction() : 
    txn_id_(INVALID_TXN_ID), 
    timestamp_(INVALID_TXN_ID),
    started_(false),
    read_only_(false) {}

Transaction::Transaction(uint64_t txn_id, bool started) : 
    txn_id_(txn_id),
    timestamp_(INVALID_TXN_ID),
    started_(started),
    read_only_(false) {}

TransactionManager::TransactionManager(LogManager &log_manager, BufferManager &buffer_manager)
    : log_manager_(log_manager), 
      buffer_manager_(buffer_manager), 
      transaction_counter_(0),
      timestamp_counter_(1) {}

TransactionManager::~TransactionManager() {}

void TransactionManager::reset(LogManager &log_manager) {
    log_manager_ = log_manager;
    buffer_manager_.discard_all_pages();
    transaction_counter_ = 0;
    timestamp_counter_ = 1;
    transaction_table_.clear();
}

uint64_t TransactionManager::start_txn(bool read_only) {
    uint64_t txn_id = ++transaction_counter_;

    Transaction txn(txn_id, true);
    txn.timestamp_ = MVTOProtocol::assign_timestamp(*this);
    txn.read_only_ = read_only;
    
    {
        std::lock_guard<std::mutex> lock(transaction_table_mutex_);
        transaction_table_[txn_id] = txn;
    }

    log_manager_.log_txn_begin(txn_id);
    return txn_id;
}

void TransactionManager::commit_txn(uint64_t txn_id) {
    Transaction txn;
    {
        std::lock_guard<std::mutex> lock(transaction_table_mutex_);
        auto it = transaction_table_.find(txn_id);
        if (it == transaction_table_.end()) {
            std::cout << "Txn does not exist \n";
            exit(-1);
        }
        txn = it->second;
    }

    if (txn.started_) {
        buffer_manager_.flush_pages(txn_id);
        buffer_manager_.transaction_complete(txn_id);
        log_manager_.log_commit(txn_id);
        
        {
            std::lock_guard<std::mutex> lock(transaction_table_mutex_);
            transaction_table_[txn_id].started_ = false;
        }
    }
}

void TransactionManager::abort_txn(uint64_t txn_id) {
    Transaction txn;
    {
        std::lock_guard<std::mutex> lock(transaction_table_mutex_);
        auto it = transaction_table_.find(txn_id);
        if (it == transaction_table_.end()) {
            std::cout << "Txn does not exist \n";
            exit(-1);
        }
        txn = it->second;
    }

    if (txn.started_) {
        buffer_manager_.discard_pages(txn_id);
        buffer_manager_.transaction_complete(txn_id);
        log_manager_.log_abort(txn_id, buffer_manager_);
        
        {
            std::lock_guard<std::mutex> lock(transaction_table_mutex_);
            transaction_table_[txn_id].started_ = false;
        }
    }
}

void TransactionManager::add_modified_page(uint64_t txn_id, uint64_t page_id) {
    std::lock_guard<std::mutex> lock(transaction_table_mutex_);
    auto it = transaction_table_.find(txn_id);
    if (it != transaction_table_.end()) {
        it->second.modified_pages_.push_back(page_id);
    }
}

void TransactionManager::add_to_read_set(uint64_t txn_id, uint64_t page_id) {
    std::lock_guard<std::mutex> lock(transaction_table_mutex_);
    auto it = transaction_table_.find(txn_id);
    if (it != transaction_table_.end()) {
        it->second.read_set_.insert(page_id);
    }
}
} // namespace buzzdb