#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <iostream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <set>

#include "common/macros.h"
#include "storage/slotted_page.h"
#include "mvto.h"

namespace buzzdb {

class BufferFrame {
private:
    friend class BufferManager;

    uint64_t page_id;
    uint64_t frame_id;
    std::vector<char> data;

    bool dirty;
    bool exclusive;
    std::thread::id exclusive_thread_id;

    std::vector<TID> version_chain_;
    mutable std::mutex version_chain_mutex_;

public:
    /// Returns a pointer to this page's data.
    char *get_data();

    /// Get visible version for MVTO protocol
    TID get_visible_version(uint64_t txn_timestamp);

    /// Add a new version to the version chain
    void add_version(TID new_version);

    BufferFrame();

    BufferFrame(const BufferFrame &other);

    BufferFrame &operator=(BufferFrame other);

    void mark_dirty() { dirty = true; }
};

class buffer_full_error : public std::exception {
public:
    const char *what() const noexcept override { return "buffer is full"; }
};

class transaction_abort_error : public std::exception {
public:
    const char *what() const noexcept override { return "transaction aborted"; }
};

class BufferManager {
public:
    /// Constructor.
    /// @param[in] page_size  Size in bytes that all pages will have.
    /// @param[in] page_count Maximum number of pages that should reside in
    ///                       memory at the same time.
    BufferManager(size_t page_size, size_t page_count);

    /// Destructor. Writes all dirty pages to disk.
    ~BufferManager();

    BufferFrame &fix_page(uint64_t txn_id, uint64_t page_id, bool exclusive);

    void unfix_page(uint64_t txn_id, BufferFrame &page, bool is_dirty);

    /// Returns the segment id for a given page id which is contained in the 16
    /// most significant bits of the page id.
    static constexpr uint16_t get_segment_id(uint64_t page_id) {
        return page_id >> 48;
    }

    /// Returns the page id within its segment for a given page id. This
    /// corresponds to the 48 least significant bits of the page id.
    static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
        return page_id & ((1ull << 48) - 1);
    }

    /// Returns the overall page id associated with a segment id and
    /// a given segment page id.
    static uint64_t get_overall_page_id(uint16_t segment_id, uint64_t segment_page_id) {
        return (static_cast<uint64_t>(segment_id) << 48) | segment_page_id;
    }

    /// Print page id
    static std::string print_page_id(uint64_t page_id) {
        if (page_id == INVALID_NODE_ID) {
            return "INVALID";
        } else {
            auto segment_id = BufferManager::get_segment_id(page_id);
            auto segment_page_id = BufferManager::get_segment_page_id(page_id);
            return "( " + std::to_string(segment_id) + " " +
                   std::to_string(segment_page_id) + " )";
        }
    }

    size_t get_page_size() { return page_size_; }

    void flush_all_pages();
    void flush_page(uint64_t page_id);
    void discard_page(uint64_t page_id);
    void discard_all_pages();
    void flush_pages(uint64_t txn_id);
    void discard_pages(uint64_t txn_id);
    void transaction_complete(uint64_t txn_id);
    void transaction_abort(uint64_t txn_id);

private:
    uint64_t capacity_;
    size_t page_size_;
    std::vector<std::unique_ptr<BufferFrame>> pool_;
    mutable std::mutex file_use_mutex_;

    void read_frame(uint64_t frame_id);
    void write_frame(uint64_t frame_id);
};

}  // namespace buzzdb