#pragma once

#include <bitset>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>
#include <mutex>

namespace buzzdb {

static constexpr uint64_t INVALID_TXN_ID = UINT64_MAX;
static constexpr uint64_t VERSION_CHAIN_END = UINT64_MAX;

struct VersionMetadata {
    uint64_t create_ts;
    uint64_t delete_ts;
    uint64_t data_length;
    uint64_t prev_version_offset;
};

struct TID {
    /// Constructor
    explicit TID(uint64_t raw_value) : value(raw_value), timestamp(0) {}
    
    /// Constructor
    TID(uint64_t page, uint16_t slot) : value((page << 16) | slot), timestamp(0) {}
    
    /// The TID value
    uint64_t value;
    
    /// Timestamp for MVTO
    uint64_t timestamp;
};

std::ostream &operator<<(std::ostream &os, TID const &t);

struct SlottedPage {
    struct Header {
        /// overall page id
        uint64_t page_id;
        /// last modifying transaction for MVTO
        uint64_t last_mod_txn;
        /// free space in page
        uint32_t free_space;
        /// number of versions in page
        uint32_t version_count;
    };

    struct Slot {
        /// current version offset
        uint64_t current_version;
        /// oldest version offset
        uint64_t oldest_version;
    };

    /// Constructor.
    SlottedPage(char* buffer, uint32_t page_size);
    
    /// Read a visible version
    uint64_t read_version(uint64_t txn_id, uint64_t slot_id, 
                         char* buffer, uint32_t size) const;
    
    /// Create a new version
    uint64_t create_version(uint64_t txn_id, uint64_t slot_id,
                           const char* data, uint32_t size);
    
    /// Get visible version for a transaction
    const VersionMetadata* get_visible_version(uint64_t slot_id, 
                                             uint64_t txn_id) const;
    
    /// Garbage collection
    void collect_garbage(uint64_t oldest_active_ts);
    
    /// Compact the page
    void compactify();
    
    /// The header
    Header* header_;
    
    /// Slot array
    Slot* slots_;
    
    /// Data area (after slots)
    char* data_area_;
    
    /// Page size
    uint32_t page_size_;
    
    /// Mutex for thread safety
    mutable std::mutex page_mutex_;
    
private:
    /// Get location of version data
    char* get_version_location(uint64_t offset) const;
    
    /// Allocate space for a new version
    uint64_t allocate_version_space(uint32_t size);
};

}  // namespace buzzdb