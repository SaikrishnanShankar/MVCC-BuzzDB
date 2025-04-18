#pragma once

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <vector>

namespace buzzdb {

class TID {
 public:
  uint64_t value;
  
  TID() : value(0) {}
  explicit TID(uint64_t raw_value);
  TID(uint64_t page, uint16_t slot);

  friend std::ostream &operator<<(std::ostream &os, TID const &t); //  check this
  
};

class SlottedPage {
 public:
  struct alignas(8) Slot {
    uint64_t value;
  };
  struct alignas(8) Header {
    Header(char *buffer_frame, uint32_t page_size);

    char *buffer_frame;
    uint16_t first_free_slot;
    uint32_t data_start;
    uint16_t slot_count;
    uint32_t free_space;
    uint64_t overall_page_id;
    
    uint64_t last_modified_txn_id;
    uint64_t page_version_timestamp;
  };

  struct VersionIndex {
    uint16_t slot_id;
    uint64_t begin_ts;
    uint64_t end_ts;
  };

  std::vector<VersionIndex> version_index;
  void updateVersionIndex(uint16_t slot_id);
  uint16_t findVisibleVersion(uint64_t txn_timestamp);

  explicit SlottedPage(char *buffer_frame, uint32_t page_size);

  void compactify(uint32_t page_size);
  
  TID addSlot(uint32_t size, uint64_t txn_id = 0, uint64_t timestamp = 0);
  
  Slot getSlot(uint16_t slotId);
  void setSlot(uint16_t slotId, uint64_t value);
  
  bool markDeleted(uint16_t slotId, uint64_t txn_id, uint64_t timestamp);
  bool isVisible(uint16_t slotId, uint64_t txn_start_timestamp);
  char* getDataPtr(uint16_t slotId);
  uint32_t getDataSize(uint16_t slotId);
  bool getVersionInfo(uint16_t slotId, uint64_t &begin_ts, uint64_t &end_ts);
  void garbageCollect(uint64_t oldest_active_timestamp); 

  Header header alignas(8);

  friend std::ostream &operator<<(std::ostream &os, SlottedPage::Slot const &m);
  friend std::ostream &operator<<(std::ostream &os, SlottedPage::Header const &h);
  friend std::ostream &operator<<(std::ostream &os, SlottedPage const &p);
};


std::ostream &operator<<(std::ostream &os, TID const &t);
std::ostream &operator<<(std::ostream &os, SlottedPage::Slot const &m);
std::ostream &operator<<(std::ostream &os, SlottedPage::Header const &h);
std::ostream &operator<<(std::ostream &os, SlottedPage const &p);

}  // namespace buzzdb