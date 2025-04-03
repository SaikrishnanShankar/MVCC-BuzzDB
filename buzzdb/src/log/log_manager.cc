#include "log/log_manager.h"
#include "buffer/buffer_manager.h"
#include "transaction/transaction_manager.h"
#include <cstring>
#include <iostream>

namespace buzzdb {

LogManager::LogManager(File* log_file) : log_file_(log_file), current_offset_(0) {
    // Initialize log record counters
    for (int i = 0; i <= static_cast<int>(LogRecordType::CHECKPOINT_RECORD); i++) {
        log_record_type_to_count[static_cast<LogRecordType>(i)] = 0;
    }
}

LogManager::~LogManager() {
    // Ensure all log records are flushed
    log_file_->flush();
}

void LogManager::reset(File* log_file) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    log_file_ = log_file;
    current_offset_ = 0;
    txn_id_to_first_log_record.clear();
    log_record_type_to_count.clear();
}

uint64_t LogManager::get_total_log_records() {
    std::lock_guard<std::mutex> lock(log_mutex_);
    uint64_t total = 0;
    for (const auto& [type, count] : log_record_type_to_count) {
        total += count;
    }
    return total;
}

uint64_t LogManager::get_total_log_records_of_type(LogRecordType type) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    return log_record_type_to_count[type];
}

void LogManager::log_txn_begin(uint64_t txn_id) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    // Log record structure:
    // [Type:1][TxnID:8][Timestamp:8]
    struct BeginRecord {
        LogRecordType type;
        uint64_t txn_id;
        uint64_t timestamp;
    };
    
    BeginRecord record;
    record.type = LogRecordType::BEGIN_RECORD;
    record.txn_id = txn_id;
    record.timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    
    // Store position of first log record for this transaction
    txn_id_to_first_log_record[txn_id] = current_offset_;
    
    // Write to log file
    log_file_->write_block(reinterpret_cast<const char*>(&record), 
                         current_offset_, sizeof(record));
    current_offset_ += sizeof(record);
    
    // Update statistics
    log_record_type_to_count[LogRecordType::BEGIN_RECORD]++;
}

void LogManager::log_commit(uint64_t txn_id) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    // Log record structure:
    // [Type:1][TxnID:8][Timestamp:8]
    struct CommitRecord {
        LogRecordType type;
        uint64_t txn_id;
        uint64_t timestamp;
    };
    
    CommitRecord record;
    record.type = LogRecordType::COMMIT_RECORD;
    record.txn_id = txn_id;
    record.timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    
    // Write to log file
    log_file_->write_block(reinterpret_cast<const char*>(&record),
                         current_offset_, sizeof(record));
    current_offset_ += sizeof(record);
    
    // Update statistics and clean up
    log_record_type_to_count[LogRecordType::COMMIT_RECORD]++;
    txn_id_to_first_log_record.erase(txn_id);
}

void LogManager::log_abort(uint64_t txn_id, BufferManager& buffer_manager) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    // First rollback the transaction
    rollback_txn(txn_id, buffer_manager);
    
    // Log record structure:
    // [Type:1][TxnID:8][Timestamp:8]
    struct AbortRecord {
        LogRecordType type;
        uint64_t txn_id;
        uint64_t timestamp;
    };
    
    AbortRecord record;
    record.type = LogRecordType::ABORT_RECORD;
    record.txn_id = txn_id;
    record.timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    
    // Write to log file
    log_file_->write_block(reinterpret_cast<const char*>(&record),
                         current_offset_, sizeof(record));
    current_offset_ += sizeof(record);
    
    // Update statistics and clean up
    log_record_type_to_count[LogRecordType::ABORT_RECORD]++;
    txn_id_to_first_log_record.erase(txn_id);
}

void LogManager::log_update(uint64_t txn_id, uint64_t page_id, uint64_t length,
                          uint64_t offset, std::byte* before_img, std::byte* after_img) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    // Log record structure:
    // [Type:1][TxnID:8][PageID:8][Offset:8][Length:8][BeforeImage][AfterImage]
    struct UpdateHeader {
        LogRecordType type;
        uint64_t txn_id;
        uint64_t page_id;
        uint64_t offset;
        uint64_t length;
    };
    
    UpdateHeader header;
    header.type = LogRecordType::UPDATE_RECORD;
    header.txn_id = txn_id;
    header.page_id = page_id;
    header.offset = offset;
    header.length = length;
    
    // Write header
    log_file_->write_block(reinterpret_cast<const char*>(&header),
                         current_offset_, sizeof(header));
    current_offset_ += sizeof(header);
    
    // Write before image
    log_file_->write_block(reinterpret_cast<const char*>(before_img),
                         current_offset_, length);
    current_offset_ += length;
    
    // Write after image
    log_file_->write_block(reinterpret_cast<const char*>(after_img),
                         current_offset_, length);
    current_offset_ += length;
    
    log_record_type_to_count[LogRecordType::UPDATE_RECORD]++;
}

void LogManager::log_checkpoint(BufferManager& buffer_manager) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    // Flush all dirty pages first
    buffer_manager.flush_all_pages();
    
    // Log record structure:
    // [Type:1][Timestamp:8][ActiveTxnsCount:8][ActiveTxnIDs...]
    struct CheckpointHeader {
        LogRecordType type;
        uint64_t timestamp;
        uint64_t active_txns_count;
    };
    
    // Get current active transactions
    std::vector<uint64_t> active_txns;
    for (const auto& [txn_id, _] : txn_id_to_first_log_record) {
        active_txns.push_back(txn_id);
    }
    
    CheckpointHeader header;
    header.type = LogRecordType::CHECKPOINT_RECORD;
    header.timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    header.active_txns_count = active_txns.size();
    
    // Write header
    log_file_->write_block(reinterpret_cast<const char*>(&header),
                         current_offset_, sizeof(header));
    current_offset_ += sizeof(header);
    
    // Write active transaction IDs
    if (!active_txns.empty()) {
        log_file_->write_block(reinterpret_cast<const char*>(active_txns.data()),
                             current_offset_, active_txns.size() * sizeof(uint64_t));
        current_offset_ += active_txns.size() * sizeof(uint64_t);
    }
    
    log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD]++;
}

void LogManager::recovery(BufferManager& buffer_manager) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    // Phase 1: Analysis - find active transactions and last checkpoint
    uint64_t checkpoint_offset = find_last_checkpoint();
    std::set<uint64_t> active_txns;
    std::set<uint64_t> committed_txns;
    
    // Read from checkpoint forward to build transaction states
    parse_log_records(checkpoint_offset, active_txns, committed_txns);
    
    // Phase 2: Redo - replay all operations from checkpoint
    redo_phase(buffer_manager, checkpoint_offset);
    
    // Phase 3: Undo - rollback uncommitted transactions
    undo_phase(buffer_manager, active_txns);
}

void LogManager::rollback_txn(uint64_t txn_id, BufferManager& buffer_manager) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    auto it = txn_id_to_first_log_record.find(txn_id);
    if (it == txn_id_to_first_log_record.end()) {
        return; // No records for this transaction
    }
    
    uint64_t offset = it->second;
    while (true) {
        // Read log record header
        LogRecordType type;
        uint64_t record_txn_id;
        uint64_t record_length;
        
        if (!read_log_header(offset, type, record_txn_id, record_length)) {
            break;
        }
        
        // Only process records for this transaction
        if (record_txn_id != txn_id) {
            offset += record_length;
            continue;
        }
        
        // Handle different record types
        switch (type) {
            case LogRecordType::UPDATE_RECORD: {
                uint64_t page_id, record_offset, length;
                std::vector<std::byte> before_image;
                
                read_update_record(offset, page_id, record_offset, length, before_image);
                
                // Restore before image
                BufferFrame& frame = buffer_manager.fix_page(txn_id, page_id, true);
                std::memcpy(frame.get_data() + record_offset, before_image.data(), length);
                buffer_manager.unfix_page(txn_id, frame, true);
                break;
            }
            default:
                break;
        }
        
        offset += record_length;
        
        // Stop if we've reached the end of the log or a commit/abort record
        if (type == LogRecordType::COMMIT_RECORD || 
            type == LogRecordType::ABORT_RECORD) {
            break;
        }
    }
}

// Private helper methods

uint64_t LogManager::find_last_checkpoint() {
    uint64_t offset = 0;
    uint64_t last_checkpoint = 0;
    
    while (offset < current_offset_) {
        LogRecordType type;
        uint64_t txn_id, length;
        
        if (!read_log_header(offset, type, txn_id, length)) {
            break;
        }
        
        if (type == LogRecordType::CHECKPOINT_RECORD) {
            last_checkpoint = offset;
        }
        
        offset += length;
    }
    
    return last_checkpoint;
}

void LogManager::parse_log_records(uint64_t start_offset, 
                                 std::set<uint64_t>& active_txns,
                                 std::set<uint64_t>& committed_txns) {
    uint64_t offset = start_offset;
    
    while (offset < current_offset_) {
        LogRecordType type;
        uint64_t txn_id, length;
        
        if (!read_log_header(offset, type, txn_id, length)) {
            break;
        }
        
        switch (type) {
            case LogRecordType::BEGIN_RECORD:
                active_txns.insert(txn_id);
                break;
            case LogRecordType::COMMIT_RECORD:
                committed_txns.insert(txn_id);
                active_txns.erase(txn_id);
                break;
            case LogRecordType::ABORT_RECORD:
                active_txns.erase(txn_id);
                break;
            case LogRecordType::CHECKPOINT_RECORD: {
                // For checkpoints, we need to read the active transactions list
                uint64_t timestamp;
                uint64_t active_count;
                log_file_->read_block(offset + 1, sizeof(uint64_t), 
                                    reinterpret_cast<char*>(&timestamp));
                log_file_->read_block(offset + 1 + sizeof(uint64_t), sizeof(uint64_t),
                                    reinterpret_cast<char*>(&active_count));
                
                std::vector<uint64_t> checkpoint_active(active_count);
                log_file_->read_block(offset + 1 + 2*sizeof(uint64_t), 
                                    active_count * sizeof(uint64_t),
                                    reinterpret_cast<char*>(checkpoint_active.data()));
                
                active_txns.clear();
                active_txns.insert(checkpoint_active.begin(), checkpoint_active.end());
                
                // Remove any that were committed after checkpoint
                for (auto it = active_txns.begin(); it != active_txns.end(); ) {
                    if (committed_txns.count(*it)) {
                        it = active_txns.erase(it);
                    } else {
                        ++it;
                    }
                }
                break;
            }
            default:
                break;
        }
        
        offset += length;
    }
}

void LogManager::redo_phase(BufferManager& buffer_manager, uint64_t start_offset) {
    uint64_t offset = start_offset;
    
    while (offset < current_offset_) {
        LogRecordType type;
        uint64_t txn_id, length;
        
        if (!read_log_header(offset, type, txn_id, length)) {
            break;
        }
        
        if (type == LogRecordType::UPDATE_RECORD) {
            uint64_t page_id, record_offset, data_length;
            std::vector<std::byte> after_image;
            
            read_update_record(offset, page_id, record_offset, data_length, after_image);
            
            // Apply the after image
            BufferFrame& frame = buffer_manager.fix_page(SYSTEM_TXN_ID, page_id, true);
            std::memcpy(frame.get_data() + record_offset, after_image.data(), data_length);
            buffer_manager.unfix_page(SYSTEM_TXN_ID, frame, true);
        }
        
        offset += length;
    }
}

void LogManager::undo_phase(BufferManager& buffer_manager, 
                          const std::set<uint64_t>& active_txns) {
    for (uint64_t txn_id : active_txns) {
        rollback_txn(txn_id, buffer_manager);
    }
}

bool LogManager::read_log_header(uint64_t offset, LogRecordType& type,
                               uint64_t& txn_id, uint64_t& length) {
    if (offset >= current_offset_) return false;
    
    // Read type (1 byte) and txn_id (8 bytes)
    log_file_->read_block(offset, sizeof(LogRecordType), 
                        reinterpret_cast<char*>(&type));
    log_file_->read_block(offset + sizeof(LogRecordType), sizeof(uint64_t),
                        reinterpret_cast<char*>(&txn_id));
    
    // Determine record length based on type
    switch (type) {
        case LogRecordType::BEGIN_RECORD:
        case LogRecordType::COMMIT_RECORD:
        case LogRecordType::ABORT_RECORD:
            length = sizeof(LogRecordType) + 2*sizeof(uint64_t); // type + txn_id + timestamp
            break;
        case LogRecordType::UPDATE_RECORD: {
            uint64_t data_length;
            log_file_->read_block(offset + sizeof(LogRecordType) + 3*sizeof(uint64_t),
                                sizeof(uint64_t),
                                reinterpret_cast<char*>(&data_length));
            length = sizeof(LogRecordType) + 4*sizeof(uint64_t) + 2*data_length;
            break;
        }
        case LogRecordType::CHECKPOINT_RECORD: {
            uint64_t active_count;
            log_file_->read_block(offset + sizeof(LogRecordType) + sizeof(uint64_t),
                                sizeof(uint64_t),
                                reinterpret_cast<char*>(&active_count));
            length = sizeof(LogRecordType) + 2*sizeof(uint64_t) + active_count*sizeof(uint64_t);
            break;
        }
        default:
            return false;
    }
    
    return true;
}

void LogManager::read_update_record(uint64_t offset, uint64_t& page_id,
                                  uint64_t& record_offset, uint64_t& length,
                                  std::vector<std::byte>& before_image) {
    // Skip type (1 byte) and txn_id (8 bytes)
    uint64_t base = offset + sizeof(LogRecordType) + sizeof(uint64_t);
    
    log_file_->read_block(base, sizeof(uint64_t), reinterpret_cast<char*>(&page_id));
    log_file_->read_block(base + sizeof(uint64_t), sizeof(uint64_t),
                        reinterpret_cast<char*>(&record_offset));
    log_file_->read_block(base + 2*sizeof(uint64_t), sizeof(uint64_t),
                        reinterpret_cast<char*>(&length));
    
    // Read before image (after header)
    before_image.resize(length);
    log_file_->read_block(base + 3*sizeof(uint64_t), length,
                        reinterpret_cast<char*>(before_image.data()));
}

}  // namespace buzzdb