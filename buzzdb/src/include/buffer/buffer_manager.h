#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include "transaction/transaction_manager.h"

namespace buzzdb {
class TransactionManager;

// const uint64_t INVALID_PAGE_ID = ~0;
// const uint64_t INVALID_FRAME_ID = ~0;
// const uint64_t INVALID_TXN_ID = ~0;

class BufferFrame {
public:
    BufferFrame();
    BufferFrame(const BufferFrame& other);
    BufferFrame& operator=(BufferFrame other);

    char* get_data();
    void mark_dirty() { dirty = true; }

    uint64_t page_id;
    uint64_t frame_id;
    bool dirty;
    bool exclusive;
    std::vector<char> data;
    uint64_t creator_txn_id;
    uint64_t version_timestamp;
};

class BufferManager {
public:
    BufferManager(size_t page_size, size_t page_count, TransactionManager& transaction_manager);
    ~BufferManager();

    BufferFrame& fix_page(uint64_t txn_id, uint64_t page_id, bool exclusive);
    void unfix_page(uint64_t txn_id, BufferFrame& page, bool is_dirty);
    
    void flush_all_pages();
    void discard_all_pages();
    
    void discard_pages(uint64_t txn_id);
    void flush_pages(uint64_t txn_id);
    void transaction_complete(uint64_t txn_id);
    void transaction_abort(uint64_t txn_id);
    
    size_t get_page_size() const { return page_size_; }

    static uint16_t get_segment_id(uint64_t page_id) { return page_id >> 48; }
    static uint64_t get_segment_page_id(uint64_t page_id) { return page_id & ((1ull << 48) - 1); }
    static uint64_t get_overall_page_id(uint16_t segment_id, uint64_t segment_page_id) {
        return (static_cast<uint64_t>(segment_id) << 48) | segment_page_id;
    }

private:
    uint64_t get_free_frame();
    void read_frame(uint64_t frame_id);
    void write_frame(uint64_t frame_id);

    size_t page_size_;
    size_t capacity_;
    std::vector<std::unique_ptr<BufferFrame>> pool_;
    std::mutex file_use_mutex_;
    std::unordered_map<uint64_t, std::vector<uint64_t>> page_versions_;
    TransactionManager& transaction_manager_;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t>> txn_page_versions_;
};

}  // namespace buzzdb