#include <cassert>
#include <iostream>
#include <string>
#include <algorithm>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/file.h"
#include <chrono>
#include <ctime> 

uint64_t wake_timeout_ = 100;
uint64_t timeout_ = 2;

namespace buzzdb {

char* BufferFrame::get_data() { return data.data(); }

BufferFrame::BufferFrame()
    : page_id(INVALID_PAGE_ID),
      frame_id(INVALID_FRAME_ID),
      dirty(false),
      exclusive(false),
      creator_txn_id(INVALID_TXN_ID),
      version_timestamp(0) {}

BufferFrame::BufferFrame(const BufferFrame& other)
    : page_id(other.page_id),
      frame_id(other.frame_id),
      dirty(other.dirty),
      exclusive(other.exclusive),
      data(other.data),
      creator_txn_id(other.creator_txn_id),
      version_timestamp(other.version_timestamp) {}

BufferFrame& BufferFrame::operator=(BufferFrame other) {
  std::swap(this->page_id, other.page_id);
  std::swap(this->frame_id, other.frame_id);
  std::swap(this->data, other.data);
  std::swap(this->dirty, other.dirty);
  std::swap(this->exclusive, other.exclusive);
  std::swap(this->creator_txn_id, other.creator_txn_id);
  std::swap(this->version_timestamp, other.version_timestamp);
  return *this;
}

BufferManager::BufferManager(size_t page_size, size_t page_count, TransactionManager& transaction_manager) 
    :transaction_manager_(transaction_manager){
  capacity_ = page_count;
  page_size_ = page_size;

  pool_.resize(capacity_);
  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    pool_[frame_id].reset(new BufferFrame());
    pool_[frame_id]->data.resize(page_size_);
    pool_[frame_id]->frame_id = frame_id;
  }
  
  page_versions_.clear();
  txn_page_versions_.clear();
}

BufferManager::~BufferManager() {
}

BufferFrame& BufferManager::fix_page(uint64_t txn_id, uint64_t page_id, bool exclusive) {

    auto txn_it = txn_page_versions_.find(txn_id);
    if (txn_it != txn_page_versions_.end()) {
        auto page_it = txn_it->second.find(page_id);
        if (page_it != txn_it->second.end()) {
            uint64_t frame_id = page_it->second;
            return *pool_[frame_id];
        }
    }
    
    auto versions_it = page_versions_.find(page_id);
    
    if (versions_it != page_versions_.end() && !versions_it->second.empty()) {
        
        uint64_t txn_timestamp = transaction_manager_.get_txn_start_timestamp(txn_id);
        uint64_t frame_id = INVALID_FRAME_ID;
        
        for (auto& version_frame_id : versions_it->second) {
            if (pool_[version_frame_id]->version_timestamp <= txn_timestamp) {
                if (frame_id == INVALID_FRAME_ID || 
                    pool_[version_frame_id]->version_timestamp > pool_[frame_id]->version_timestamp) {
                    frame_id = version_frame_id;
                }
            }
        }
        
        // if (frame_id == INVALID_FRAME_ID) {
        //     // Handle case where no version is visible
        // }
        
        if (exclusive) {

            uint64_t new_frame_id = get_free_frame();
            
            std::copy(pool_[frame_id]->data.begin(), pool_[frame_id]->data.end(), 
                     pool_[new_frame_id]->data.begin());
            
            pool_[new_frame_id]->page_id = page_id;
            pool_[new_frame_id]->dirty = false;
            pool_[new_frame_id]->exclusive = true;
            pool_[new_frame_id]->creator_txn_id = txn_id;
            
            if (txn_page_versions_.find(txn_id) == txn_page_versions_.end()) {
                txn_page_versions_[txn_id] = std::unordered_map<uint64_t, uint64_t>();
            }
            txn_page_versions_[txn_id][page_id] = new_frame_id;
            
            return *pool_[new_frame_id];
        }
        
        return *pool_[frame_id];
    }
    
    uint64_t frame_id = get_free_frame();

    pool_[frame_id]->page_id = page_id;
    pool_[frame_id]->dirty = false;
    pool_[frame_id]->exclusive = exclusive;
    pool_[frame_id]->creator_txn_id = exclusive ? txn_id : INVALID_TXN_ID;
    
 
    if (page_versions_.find(page_id) == page_versions_.end()) {
        page_versions_[page_id] = std::vector<uint64_t>();
    }
    page_versions_[page_id].push_back(frame_id);
    
    if (exclusive) {
        if (txn_page_versions_.find(txn_id) == txn_page_versions_.end()) {
            txn_page_versions_[txn_id] = std::unordered_map<uint64_t, uint64_t>();
        }
        txn_page_versions_[txn_id][page_id] = frame_id;
    }
    
    return *pool_[frame_id];
}

uint64_t BufferManager::get_free_frame() {

    for (size_t i = 0; i < capacity_; i++) {
        if (pool_[i]->page_id == INVALID_PAGE_ID) {
            return i;
        }
    }

    return 0;
}

// MVCC modification  to handle versioning
void BufferManager::unfix_page(uint64_t txn_id, BufferFrame& page, bool is_dirty) {
    (void)txn_id;
    if (is_dirty) {
        page.dirty = true;

    }

}

void BufferManager::flush_all_pages(){
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        if (pool_[frame_id]->dirty == true && pool_[frame_id]->page_id != INVALID_PAGE_ID) {
            write_frame(frame_id);
            pool_[frame_id]->dirty = false;
        }
    }
}

void BufferManager::discard_all_pages(){
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
        pool_[frame_id].reset(new BufferFrame());
        pool_[frame_id]->page_id = INVALID_PAGE_ID;
        pool_[frame_id]->dirty = false;
        pool_[frame_id]->data.resize(page_size_);
        pool_[frame_id]->frame_id = frame_id;
    }
    

    page_versions_.clear();
    txn_page_versions_.clear();
}

void BufferManager::discard_pages(uint64_t txn_id){
    auto txn_it = txn_page_versions_.find(txn_id);
    if (txn_it != txn_page_versions_.end()) {
        for (auto& page_entry : txn_it->second) {
            uint64_t page_id = page_entry.first;
            uint64_t frame_id = page_entry.second;
            
            pool_[frame_id]->page_id = INVALID_PAGE_ID;
            pool_[frame_id]->dirty = false;
            pool_[frame_id]->creator_txn_id = INVALID_TXN_ID;
            
            auto versions_it = page_versions_.find(page_id);
            if (versions_it != page_versions_.end()) {
                auto& versions = versions_it->second;
                versions.erase(std::remove_if(versions.begin(), versions.end(), [frame_id](uint64_t id) { return id == frame_id; }), versions.end());
            }
        }
        
        txn_page_versions_.erase(txn_id);
    }
}

void BufferManager::flush_pages(uint64_t txn_id){
    auto txn_it = txn_page_versions_.find(txn_id);
    if (txn_it != txn_page_versions_.end()) {
        for (auto& page_entry : txn_it->second) {
            uint64_t frame_id = page_entry.second;
            
            if (pool_[frame_id]->dirty) {
                write_frame(frame_id);
                pool_[frame_id]->dirty = false;
            }
        }
    }
}


void BufferManager::transaction_complete(uint64_t txn_id){

    txn_page_versions_.erase(txn_id);
}


void BufferManager::transaction_abort(uint64_t txn_id){
    discard_pages(txn_id);
}


void BufferManager::read_frame(uint64_t frame_id) {
  std::lock_guard<std::mutex> file_guard(file_use_mutex_);

  auto segment_id = get_segment_id(pool_[frame_id]->page_id);
  auto file_handle =
      File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
  size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
  file_handle->read_block(start, page_size_, pool_[frame_id]->data.data());
}

void BufferManager::write_frame(uint64_t frame_id) {
  std::lock_guard<std::mutex> file_guard(file_use_mutex_);

  auto segment_id = get_segment_id(pool_[frame_id]->page_id);
  auto file_handle =
      File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
  size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
  file_handle->write_block(pool_[frame_id]->data.data(), start, page_size_);
}


}  // namespace buzzdb