#include <cassert>
#include <iostream>
#include <string.h>
#include <cstddef>
#include <set>
#include <unordered_map>
#include "log/log_manager.h"
#include "common/macros.h"
#include "storage/test_file.h"
#include "transaction/transaction_manager.h"

namespace buzzdb {

LogManager::LogManager(File* log_file) {
    log_file_ = log_file;
    for (int i = 0; i <= static_cast<int>(LogRecordType::CHECKPOINT_RECORD); i++) {
        log_record_type_to_count[static_cast<LogRecordType>(i)] = 0;
    }
    log_record_type_to_count[LogRecordType::VERSION_CREATE_RECORD] = 0;
    log_record_type_to_count[LogRecordType::VERSION_DELETE_RECORD] = 0;
}

LogManager::~LogManager() {
}

void LogManager::reset(File* log_file) {
    log_file_ = log_file;
    current_offset_ = 0;
    txn_id_to_first_log_record.clear();
    log_record_type_to_count.clear();
}

uint64_t LogManager::get_total_log_records() {
    uint64_t total = 0;
    for (const auto& pair : log_record_type_to_count) {
        total += pair.second;
    }
    return total;
}

uint64_t LogManager::get_total_log_records_of_type(LogRecordType type) {
    return log_record_type_to_count[type];
}

void LogManager::write_log_record_header(LogRecordHeader &header) {
    log_file_->write_block(reinterpret_cast<char*>(&header.type), current_offset_, sizeof(LogRecordType));
    current_offset_ += sizeof(LogRecordType);
    log_file_->write_block(reinterpret_cast<char*>(&header.txn_id), current_offset_, sizeof(uint64_t));
    current_offset_ += sizeof(uint64_t);
    log_file_->write_block(reinterpret_cast<char*>(&header.prev_offset), current_offset_, sizeof(uint64_t));
    current_offset_ += sizeof(uint64_t);
    log_file_->write_block(reinterpret_cast<char*>(&header.length), current_offset_, sizeof(uint64_t));
    current_offset_ += sizeof(uint64_t);
    log_file_->write_block(reinterpret_cast<char*>(&header.timestamp), current_offset_, sizeof(uint64_t));
    current_offset_ += sizeof(uint64_t);
}

LogRecordHeader LogManager::read_log_record_header(size_t offset) {
    LogRecordHeader header;
    log_file_->read_block(offset, sizeof(LogRecordType), reinterpret_cast<char*>(&header.type));
    offset += sizeof(LogRecordType);
    log_file_->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&header.txn_id));
    offset += sizeof(uint64_t);
    log_file_->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&header.prev_offset));
    offset += sizeof(uint64_t);
    log_file_->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&header.length));
    offset += sizeof(uint64_t);
    log_file_->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&header.timestamp));
    return header;
}

void LogManager::write_data(size_t offset, void* data, size_t length) {
    log_file_->write_block(reinterpret_cast<char*>(data), offset, length);
    current_offset_ += length;
}

void LogManager::read_data(size_t offset, void* data, size_t length) {
    log_file_->read_block(offset, length, reinterpret_cast<char*>(data));
}

void LogManager::log_txn_begin(uint64_t txn_id, uint64_t start_timestamp) {
    LogRecordHeader header;
    header.type = LogRecordType::BEGIN_RECORD;
    header.txn_id = txn_id;
    header.prev_offset = current_offset_;
    header.length = 0;
    header.timestamp = start_timestamp;
    write_log_record_header(header);
    txn_id_to_first_log_record[txn_id] = current_offset_;
    log_record_type_to_count[LogRecordType::BEGIN_RECORD]++;
}

void LogManager::log_commit(uint64_t txn_id, uint64_t commit_timestamp) {
    LogRecordHeader header;
    header.type = LogRecordType::COMMIT_RECORD;
    header.txn_id = txn_id;
    header.prev_offset = current_offset_;
    header.length = 0;
    header.timestamp = commit_timestamp;
    write_log_record_header(header);
    log_record_type_to_count[LogRecordType::COMMIT_RECORD]++;
}

void LogManager::log_abort(uint64_t txn_id, BufferManager& buffer_manager) {
    LogRecordHeader header;
    header.type = LogRecordType::ABORT_RECORD;
    header.txn_id = txn_id;
    header.prev_offset = current_offset_;
    header.length = 0;
    header.timestamp = 0;
    write_log_record_header(header);
    log_record_type_to_count[LogRecordType::ABORT_RECORD]++;
    rollback_txn(txn_id, buffer_manager);
}

void LogManager::log_update(uint64_t txn_id, uint64_t page_id, uint64_t length, uint64_t offset, std::byte* before_img, std::byte* after_img) {
    LogRecordHeader header;
    header.type = LogRecordType::UPDATE_RECORD;
    header.txn_id = txn_id;
    header.prev_offset = current_offset_;
    header.length = sizeof(uint64_t) * 3 + length * 2;
    header.timestamp = 0;
    write_log_record_header(header);
    write_data(current_offset_, &page_id, sizeof(uint64_t));
    write_data(current_offset_, &offset, sizeof(uint64_t));
    write_data(current_offset_, &length, sizeof(uint64_t));
    write_data(current_offset_, before_img, length);
    write_data(current_offset_, after_img, length);
    log_record_type_to_count[LogRecordType::UPDATE_RECORD]++;
}

void LogManager::log_version_create(uint64_t txn_id, uint64_t page_id, uint64_t slot_id, uint64_t begin_timestamp) {
    LogRecordHeader header;
    header.type = LogRecordType::VERSION_CREATE_RECORD;
    header.txn_id = txn_id;
    header.prev_offset = current_offset_;
    header.length = sizeof(uint64_t) * 3;
    header.timestamp = begin_timestamp;
    write_log_record_header(header);
    write_data(current_offset_, &page_id, sizeof(uint64_t));
    write_data(current_offset_, &slot_id, sizeof(uint64_t));
    write_data(current_offset_, &begin_timestamp, sizeof(uint64_t));
    log_record_type_to_count[LogRecordType::VERSION_CREATE_RECORD]++;
}

void LogManager::log_version_delete(uint64_t txn_id, uint64_t page_id, uint64_t slot_id, uint64_t end_timestamp) {
    LogRecordHeader header;
    header.type = LogRecordType::VERSION_DELETE_RECORD;
    header.txn_id = txn_id;
    header.prev_offset = current_offset_;
    header.length = sizeof(uint64_t) * 3;
    header.timestamp = end_timestamp;
    write_log_record_header(header);
    write_data(current_offset_, &page_id, sizeof(uint64_t));
    write_data(current_offset_, &slot_id, sizeof(uint64_t));
    write_data(current_offset_, &end_timestamp, sizeof(uint64_t));
    log_record_type_to_count[LogRecordType::VERSION_DELETE_RECORD]++;
}

void LogManager::log_checkpoint(BufferManager &buffer_manager) {
    LogRecordHeader header;
    header.type = LogRecordType::CHECKPOINT_RECORD;
    header.txn_id = 0;
    header.prev_offset = current_offset_;
    header.length = 0;
    header.timestamp = 0;
    write_log_record_header(header);
    buffer_manager.flush_all_pages();
    log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD]++;
}

void LogManager::recovery(BufferManager &buffer_manager) {
    std::unordered_set<uint64_t> committed_txns;
    std::unordered_set<uint64_t> active_txns;
    std::unordered_map<uint64_t, size_t> txn_last_record;
    size_t offset = 0;
    while (offset < current_offset_) {
        LogRecordHeader header = read_log_record_header(offset);
        offset += sizeof(LogRecordHeader);
        switch (header.type) {
            case LogRecordType::BEGIN_RECORD:
                active_txns.insert(header.txn_id);
                txn_last_record[header.txn_id] = offset - sizeof(LogRecordHeader);
                break;
            case LogRecordType::COMMIT_RECORD:
                committed_txns.insert(header.txn_id);
                active_txns.erase(header.txn_id);
                break;
            case LogRecordType::ABORT_RECORD:
                active_txns.erase(header.txn_id);
                break;
            default:
                txn_last_record[header.txn_id] = offset - sizeof(LogRecordHeader);
                break;
        }
        offset += header.length;
    }

    offset = 0;
    while (offset < current_offset_) {
        LogRecordHeader header = read_log_record_header(offset);
        offset += sizeof(LogRecordHeader);
        switch (header.type) {
            case LogRecordType::UPDATE_RECORD: {
                uint64_t page_id, data_offset, length;
                log_file_->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&page_id));
                offset += sizeof(uint64_t);
                log_file_->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&data_offset));
                offset += sizeof(uint64_t);
                log_file_->read_block(offset, sizeof(uint64_t), reinterpret_cast<char*>(&length));
                offset += sizeof(uint64_t);
                std::byte* after_img = new std::byte[length];
                log_file_->read_block(offset, length, reinterpret_cast<char*>(after_img));
                offset += length;
                offset += length;
                BufferFrame& frame = buffer_manager.fix_page(INVALID_TXN_ID, page_id, true);
                memcpy(&frame.get_data()[data_offset], after_img, length);
                frame.mark_dirty();
                buffer_manager.unfix_page(INVALID_TXN_ID, frame, true);
                delete[] after_img;
                break;
            }
            default:
                offset += header.length;
                break;
        }
    }

    for (auto txn_id : active_txns) {
        rollback_txn(txn_id, buffer_manager);
    }
}

} //namespace buzzdb
