#include "buffer_manager.h"
#include "common/macros.h"
#include "storage/file.h"
#include <chrono>
#include <ctime>

uint64_t wake_timeout_ = 100;
uint64_t timeout_ = 2;

namespace buzzdb {

char* BufferFrame::get_data() { return data.data(); }

TID BufferFrame::get_visible_version(uint64_t txn_timestamp) {
    std::lock_guard<std::mutex> lock(version_chain_mutex_);
    for (const auto& version : version_chain_) {
        // timestamp <= txn_timestamp  => it should be visible
        if (version.timestamp <= txn_timestamp) {
            return version;
        }
    }
    return TID(INVALID_PAGE_ID, 0);
}

void BufferFrame::add_version(TID new_version) {
    std::lock_guard<std::mutex> lock(version_chain_mutex_);
    version_chain_.insert(version_chain_.begin(), new_version);
}

BufferFrame::BufferFrame()
    : page_id(INVALID_PAGE_ID),
      frame_id(INVALID_FRAME_ID),
      dirty(false),
      exclusive(false) {}

BufferFrame::BufferFrame(const BufferFrame& other)
    : page_id(other.page_id),
      frame_id(other.frame_id),
      data(other.data),
      dirty(other.dirty),
      exclusive(other.exclusive),
      version_chain_(other.version_chain_) {}

BufferFrame& BufferFrame::operator=(BufferFrame other) {
    std::swap(this->page_id, other.page_id);
    std::swap(this->frame_id, other.frame_id);
    std::swap(this->data, other.data);
    std::swap(this->dirty, other.dirty);
    std::swap(this->exclusive, other.exclusive);
    std::swap(this->version_chain_, other.version_chain_);
    return *this;
}

BufferManager::BufferManager(size_t page_size, size_t page_count) {
    capacity_ = page_count;
    page_size_ = page_size;
    pool_.resize(capacity_);
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        pool_[frame_id].reset(new BufferFrame());
        pool_[frame_id]->data.resize(page_size_);
        pool_[frame_id]->frame_id = frame_id;
    }
}

BufferManager::~BufferManager() {
    flush_all_pages();
}

BufferFrame& BufferManager::fix_page(uint64_t txn_id, uint64_t page_id, bool exclusive) {
    for (auto& frame : pool_) {
        if (frame->page_id == page_id) {
            if (exclusive && !frame->exclusive) {
                frame->exclusive = true;
                frame->exclusive_thread_id = std::this_thread::get_id();
            }
            return *frame;
        }
    }
    for (auto& frame : pool_) {
        if (frame->page_id == INVALID_PAGE_ID) {
            frame->page_id = page_id;
            frame->dirty = false;
            frame->exclusive = exclusive;
            if (exclusive) {
                frame->exclusive_thread_id = std::this_thread::get_id();
            }
            read_frame(frame->frame_id);
            return *frame;
        }
    }
    throw buffer_full_error();
}

void BufferManager::unfix_page(uint64_t txn_id, BufferFrame& page, bool is_dirty) {
    if (is_dirty) {
        page.dirty = true;
    }
    if (page.exclusive) {
        page.exclusive = false;
    }
}

void BufferManager::flush_all_pages() {
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        if (pool_[frame_id]->dirty) {
            write_frame(frame_id);
        }
    }
}

void BufferManager::flush_page(uint64_t page_id) {
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        if (pool_[frame_id]->page_id == page_id && pool_[frame_id]->dirty) {
            write_frame(frame_id);
            return;
        }
    }
}

void BufferManager::discard_page(uint64_t page_id) {
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        if (pool_[frame_id]->page_id == page_id) {
            pool_[frame_id].reset(new BufferFrame());
            pool_[frame_id]->data.resize(page_size_);
            return;
        }
    }
}

void BufferManager::discard_all_pages() {
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        pool_[frame_id].reset(new BufferFrame());
        pool_[frame_id]->page_id = INVALID_PAGE_ID;
        pool_[frame_id]->dirty = false;
        pool_[frame_id]->data.resize(page_size_);
    }
}

void BufferManager::flush_pages(uint64_t txn_id) {
    flush_all_pages();
}

void BufferManager::discard_pages(uint64_t txn_id) {
    discard_all_pages();
}

void BufferManager::transaction_complete(uint64_t txn_id) {
    // We haven't implemented because version based recovery hasn't been implemented yet
}

void BufferManager::transaction_abort(uint64_t txn_id) {
    discard_pages(txn_id);
}

void BufferManager::read_frame(uint64_t frame_id) {
    std::lock_guard<std::mutex> file_guard(file_use_mutex_);

    auto segment_id = get_segment_id(pool_[frame_id]->page_id);
    auto file_handle = File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
    size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
    file_handle->read_block(start, page_size_, pool_[frame_id]->data.data());
}

void BufferManager::write_frame(uint64_t frame_id) {
    std::lock_guard<std::mutex> file_guard(file_use_mutex_);

    auto segment_id = get_segment_id(pool_[frame_id]->page_id);
    auto file_handle = File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
    size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
    file_handle->write_block(pool_[frame_id]->data.data(), start, page_size_);
    pool_[frame_id]->dirty = false;
}
}  // namespace buzzdb