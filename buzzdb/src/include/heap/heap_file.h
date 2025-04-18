#pragma once

#include <cstddef>
#include <iostream>

#include "buffer/buffer_manager.h"
#include "log/log_manager.h"
#include "storage/slotted_page.h"

namespace buzzdb {

class TransactionManager;

class HeapPage {
 public:
  struct alignas(8) Slot {
    uint64_t value;
  };

  struct alignas(8) Header {
    Header(char *buffer_frame, uint32_t page_size);

    char *buffer_frame;
    uint64_t last_dirtied_transaction_id;  
    uint16_t first_free_slot;
    uint32_t data_start;
    uint16_t slot_count;
    uint32_t free_space;
    uint64_t overall_page_id;
  };

  explicit HeapPage(char *buffer_frame, uint32_t page_size);

  TID addSlot(uint32_t size, uint64_t begin_timestamp);
  Slot getSlot(uint16_t slotId);
  void setSlot(uint16_t slotId, uint64_t value);

  Header header alignas(8);

  friend std::ostream &operator<<(std::ostream &os, HeapPage::Slot const &m);
  friend std::ostream &operator<<(std::ostream &os, HeapPage::Header const &h);
  friend std::ostream &operator<<(std::ostream &os, HeapPage const &p);
};

class HeapSegment {
 public:
  HeapSegment(uint16_t segment_id, LogManager &log_manager,
              BufferManager& buffer_manager);
  
  HeapSegment(uint16_t segment_id, LogManager &log_manager,
              BufferManager& buffer_manager, TransactionManager& transaction_manager);

  TID allocate(uint32_t record_size, uint64_t txn_id);
  uint32_t read(TID tid, std::byte* record, uint32_t capacity, uint64_t txn_id) const;
  uint32_t write(TID tid, std::byte* record, uint32_t record_size, uint64_t txn_id);
  
  uint32_t write_new_version(TID tid, std::byte* record, uint32_t record_size, uint64_t txn_id);
  uint32_t remove(TID tid, uint64_t txn_id);
  void garbage_collect(uint64_t oldest_active_timestamp);

  friend std::ostream &operator<<(std::ostream &os, HeapSegment const &s);

 private:
  uint16_t segment_id_;
  LogManager &log_manager_;
  BufferManager &buffer_manager_;
  TransactionManager &transaction_manager_;
  uint64_t page_count_;
};

}  // namespace buzzdb