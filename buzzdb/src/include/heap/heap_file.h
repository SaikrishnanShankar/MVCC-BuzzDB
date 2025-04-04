#pragma once

#include <string>
#include <vector>
#include <atomic>
#include <cstddef>

#include "buffer/buffer_manager.h"
#include "log/log_manager.h"
#include "storage/slotted_page.h"
#include "common/macros.h"

namespace buzzdb {

struct HeapPage {
    struct Header {
        explicit Header(char *_buffer_frame, uint32_t page_size);

        uint64_t overall_page_id;
        uint64_t last_dirtied_timestamp;
        char *buffer_frame;
        uint16_t slot_count;
        uint16_t first_free_slot;
        uint32_t data_start;
        uint32_t free_space;
    };

    struct Slot {
        Slot() = default;
        uint64_t value;
    };

    explicit HeapPage(char *buffer_frame, uint32_t page_size);

    Header header;

    Slot getSlot(uint16_t slotId);
    TID addSlot(uint32_t size);
    void setSlot(uint16_t slotId, uint64_t value);
};

std::ostream &operator<<(std::ostream &os, HeapPage::Slot const &m);
std::ostream &operator<<(std::ostream &os, HeapPage::Header const &h);
std::ostream &operator<<(std::ostream &os, HeapPage const &p);

class HeapSegment {
public:
    HeapSegment(uint16_t segment_id, LogManager &log_manager,
               BufferManager &buffer_manager);

    TID allocate(uint32_t record_size, uint64_t txn_id = INVALID_TXN_ID);
    uint32_t read(TID tid, std::byte *record, uint32_t capacity, 
                 uint64_t txn_id = INVALID_TXN_ID) const;
    uint32_t write(TID tid, std::byte* record, uint32_t record_size, 
                  uint64_t txn_id = INVALID_TXN_ID);

    uint16_t segment_id_;
    LogManager &log_manager_;
    BufferManager &buffer_manager_;
    uint64_t page_count_;
};

std::ostream &operator<<(std::ostream &os, HeapSegment const &s);

}  // namespace buzzdb