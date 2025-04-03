#include "storage/slotted_page.h"
#include "common/macros.h"
#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace buzzdb {

SlottedPage::SlottedPage(char* buffer, uint32_t page_size) 
    : header_(reinterpret_cast<Header*>(buffer)),
      slots_(reinterpret_cast<Slot*>(buffer + sizeof(Header))),
      data_area_(buffer + sizeof(Header) + (page_size / sizeof(Slot))),
      page_size_(page_size) {
    
    // Initialize header if new page
    if (header_->page_id == INVALID_PAGE_ID) {
        header_->page_id = INVALID_PAGE_ID;
        header_->last_mod_txn = INVALID_TXN_ID;
        header_->free_space = page_size - sizeof(Header);
        header_->version_count = 0;
    }
}

uint64_t SlottedPage::read_version(uint64_t txn_id, uint64_t slot_id, 
                                  char* buffer, uint32_t size) const {
    std::lock_guard<std::mutex> lock(page_mutex_);
    
    const VersionMetadata* version = get_visible_version(slot_id, txn_id);
    if (!version) {
        throw std::runtime_error("No visible version found");
    }

    if (size < version->data_length) {
        throw std::runtime_error("Buffer too small for version data");
    }

    const char* version_data = get_version_location(version->prev_version_offset);
    std::memcpy(buffer, version_data, version->data_length);
    return version->data_length;
}

uint64_t SlottedPage::create_version(uint64_t txn_id, uint64_t slot_id,
                                    const char* data, uint32_t size) {
    std::lock_guard<std::mutex> lock(page_mutex_);

    // Check for write-write conflicts
    if (header_->last_mod_txn > txn_id) {
        throw std::runtime_error("Write-write conflict detected");
    }

    // Allocate space for new version
    uint64_t new_offset = allocate_version_space(size + sizeof(VersionMetadata));
    if (new_offset == INVALID_PAGE_ID) {
        throw std::runtime_error("Not enough space for new version");
    }

    // Create new version metadata
    VersionMetadata new_ver;
    new_ver.create_ts = txn_id;
    new_ver.delete_ts = INVALID_TXN_ID;
    new_ver.data_length = size;
    
    // Link to previous version
    new_ver.prev_version_offset = slots_[slot_id].current_version;
    if (slots_[slot_id].oldest_version == VERSION_CHAIN_END) {
        slots_[slot_id].oldest_version = new_offset;
    }

    // Write version data
    char* version_location = get_version_location(new_offset);
    std::memcpy(version_location, &new_ver, sizeof(VersionMetadata));
    std::memcpy(version_location + sizeof(VersionMetadata), data, size);

    // Update slot pointer
    slots_[slot_id].current_version = new_offset;
    header_->last_mod_txn = txn_id;
    header_->version_count++;

    return new_offset;
}

const VersionMetadata* SlottedPage::get_visible_version(uint64_t slot_id, 
                                                      uint64_t txn_id) const {
    uint64_t current = slots_[slot_id].current_version;
    
    while (current != VERSION_CHAIN_END) {
        const VersionMetadata* ver = reinterpret_cast<const VersionMetadata*>(
            get_version_location(current));
        
        // Visibility check
        if (ver->create_ts <= txn_id && 
            (ver->delete_ts == INVALID_TXN_ID || ver->delete_ts > txn_id)) {
            return ver;
        }
        current = ver->prev_version_offset;
    }
    return nullptr;
}

void SlottedPage::collect_garbage(uint64_t oldest_active_ts) {
    std::lock_guard<std::mutex> lock(page_mutex_);
    
    for (uint32_t i = 0; i < (page_size_ / sizeof(Slot)); i++) {
        if (slots_[i].current_version == VERSION_CHAIN_END) continue;
        
        uint64_t* prev_ptr = &slots_[i].current_version;
        uint64_t current = *prev_ptr;
        
        while (current != VERSION_CHAIN_END) {
            VersionMetadata* ver = reinterpret_cast<VersionMetadata*>(
                get_version_location(current));
            
            if (ver->delete_ts != INVALID_TXN_ID && 
                ver->delete_ts < oldest_active_ts) {
                // Remove from chain
                *prev_ptr = ver->prev_version_offset;
                header_->free_space += ver->data_length + sizeof(VersionMetadata);
                header_->version_count--;
            } else {
                prev_ptr = &ver->prev_version_offset;
            }
            current = ver->prev_version_offset;
        }
    }
    
    compactify();
}

void SlottedPage::compactify() {
    // Simple compaction that moves all versions to end of page
    std::vector<char> temp_buffer(page_size_);
    uint64_t new_offset = page_size_;
    
    for (uint32_t i = 0; i < (page_size_ / sizeof(Slot)); i++) {
        if (slots_[i].current_version == VERSION_CHAIN_END) continue;
        
        uint64_t current = slots_[i].current_version;
        slots_[i].current_version = VERSION_CHAIN_END;
        slots_[i].oldest_version = VERSION_CHAIN_END;
        
        while (current != VERSION_CHAIN_END) {
            VersionMetadata* ver = reinterpret_cast<VersionMetadata*>(
                get_version_location(current));
            
            // Calculate new position
            new_offset -= ver->data_length + sizeof(VersionMetadata);
            char* new_location = &temp_buffer[new_offset];
            
            // Copy version data
            std::memcpy(new_location, ver, sizeof(VersionMetadata) + ver->data_length);
            
            // Update pointers
            VersionMetadata* new_ver = reinterpret_cast<VersionMetadata*>(new_location);
            uint64_t old_prev = new_ver->prev_version_offset;
            new_ver->prev_version_offset = slots_[i].current_version;
            slots_[i].current_version = new_offset;
            
            if (slots_[i].oldest_version == VERSION_CHAIN_END) {
                slots_[i].oldest_version = new_offset;
            }
            
            current = old_prev;
        }
    }
    
    // Copy compacted data back
    std::memcpy(data_area_, &temp_buffer[new_offset], page_size_ - new_offset);
    header_->free_space = new_offset - (sizeof(Header) + (page_size_ / sizeof(Slot)));
}

char* SlottedPage::get_version_location(uint64_t offset) const {
    if (offset == VERSION_CHAIN_END) return nullptr;
    if (offset >= page_size_) {
        throw std::runtime_error("Invalid version offset");
    }
    return data_area_ + offset;
}

uint64_t SlottedPage::allocate_version_space(uint32_t size) {
    if (header_->free_space < size) {
        return INVALID_PAGE_ID;
    }
    
    uint64_t offset = page_size_ - header_->free_space;
    header_->free_space -= size;
    return offset;
}
} // namespace buzzdb