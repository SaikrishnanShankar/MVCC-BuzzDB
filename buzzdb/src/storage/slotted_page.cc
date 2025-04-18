#include <algorithm>
#include <bitset>
#include <cassert>
#include <cstring>
#include <vector>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/slotted_page.h"

using SlottedPage = buzzdb::SlottedPage;
using TID = buzzdb::TID;

TID::TID(uint64_t raw_value) { value = raw_value; }

TID::TID(uint64_t page, uint16_t slot) { value = (page << 16) | slot; }


SlottedPage::Header::Header(char *_buffer_frame, uint32_t page_size) {
  buffer_frame = _buffer_frame;
  first_free_slot = 0;
  data_start = page_size;
  slot_count = 0;
  free_space = page_size - sizeof(header);
  slot_count = 0;
  overall_page_id = -1;
  last_modified_txn_id = INVALID_TXN_ID;
  page_version_timestamp = 0;
}

SlottedPage::SlottedPage(char *buffer_frame, uint32_t page_size)
    : header(buffer_frame, page_size) {}

namespace buzzdb {
  std::ostream &operator<<(std::ostream &os, TID const &t) {
    uint64_t overall_page_id = t.value >> 16;
    uint64_t page_id = BufferManager::get_segment_page_id(overall_page_id);
    uint16_t slot = t.value & ((1ull << 16) - 1);
  
    os << "TID: page_id: " << page_id << " -- ";
    os << "slot: " << slot << " \n";
    return os;
  }

  std::ostream &operator<<(std::ostream &os, SlottedPage::Slot const &m) {
    uint64_t t = m.value >> 56;
    uint64_t s = m.value << 8 >> 56;
    uint64_t length = m.value << 40 >> 40;
    uint64_t offset = m.value << 16 >> 40;

    os << "[ T: " << t << " ";
    os << "S: " << s << " ";
    os << "O: " << offset << " ";
    os << "L: " << length << " ]";
    
    os << "\n";

    return os;
  }

  std::ostream &operator<<(std::ostream &os,
                                  SlottedPage::Header const &h) {
    os << "first_free_slot  : " << h.first_free_slot << "\n";
    os << "data_start       : " << h.data_start << "\n";
    os << "free_space       : " << h.free_space << "\n";
    os << "slot_count       : " << h.slot_count << "\n";
    os << "last_modified_txn: " << h.last_modified_txn_id << "\n";
    os << "version_timestamp: " << h.page_version_timestamp << "\n";

    return os;
  }

  std::ostream &operator<<(std::ostream &os, SlottedPage const &p) {
    uint16_t segment_id = BufferManager::get_segment_id(p.header.overall_page_id);
    uint64_t page_id =
        BufferManager::get_segment_page_id(p.header.overall_page_id);

    os << "------------------------------------------------\n";
    os << "Slotted Page:: segment " << segment_id << " :: page " << page_id
      << " \n";

    os << "Header: \n";
    os << p.header;

    os << "Slot List: ";
    os << " (" << p.header.slot_count << " slots)\n";

    auto slots = reinterpret_cast<buzzdb::SlottedPage::Slot *>(
        p.header.buffer_frame + sizeof(p.header));
    for (uint16_t slot_itr = 0; slot_itr < p.header.slot_count; slot_itr++) {
      os << slot_itr << " :: " << slots[slot_itr];
    }

    os << "------------------------------------------------\n";
    return os;
  }
}

void SlottedPage::compactify(uint32_t page_size) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  std::vector<char> temp_buffer(page_size);
  uint32_t current_offset = page_size;
  for (uint16_t slot_id = 0; slot_id < header.slot_count; slot_id++) {
    Slot &slot = slots[slot_id];
    if (slot.value == 0) continue; // Skip empty slots
    
    uint64_t value = slot.value;
    uint64_t length = value << 40 >> 40;
    uint64_t old_offset = value << 16 >> 40;
    if (length == 0) continue;
    current_offset -= length;
    memcpy(&temp_buffer[current_offset], 
           header.buffer_frame + old_offset, 
           length);
    
    uint64_t t = value >> 56;
    uint64_t s = value << 8 >> 56;
    
    uint64_t new_value = 0;
    new_value += t << 56;
    new_value += s << 56 >> 8;
    uint64_t shifted_value = ((uint64_t)current_offset << 24) >> 16;  
    new_value += shifted_value;
    // new_value += (current_offset << 40 >> 16);
    new_value += (length << 40 >> 40);
    slot.value = new_value;
  }
  

  if (current_offset < page_size) {
    memcpy(header.buffer_frame + current_offset, 
           &temp_buffer[current_offset], 
           page_size - current_offset);
  }
  
  header.data_start = current_offset;
  uint32_t slot_space = header.slot_count * sizeof(Slot);
  header.free_space = header.data_start - slot_space - sizeof(header);
}

void SlottedPage::updateVersionIndex(uint16_t slot_id) {
  uint64_t begin_ts, end_ts;
  if (getVersionInfo(slot_id, begin_ts, end_ts)) {
      version_index.push_back({slot_id, begin_ts, end_ts});
  }
}

uint16_t SlottedPage::findVisibleVersion(uint64_t txn_timestamp) {
  uint16_t result = UINT16_MAX;
  uint64_t latest_ts = 0;
  
  for (const auto& idx : version_index) {
      if (idx.begin_ts <= txn_timestamp && 
          (idx.end_ts == 0 || idx.end_ts > txn_timestamp) &&
          idx.begin_ts > latest_ts) {
          result = idx.slot_id;
          latest_ts = idx.begin_ts;
      }
  }
  
  return result;
}

buzzdb::SlottedPage::Slot SlottedPage::getSlot(uint16_t slotId) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  return slots[slotId];
}

void SlottedPage::setSlot(uint16_t slotId, uint64_t value) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  slots[slotId].value = value;
}

TID SlottedPage::addSlot(uint32_t size, uint64_t txn_id, uint64_t timestamp) {
  uint32_t total_size = size + (2 * sizeof(uint64_t)); 
  uint64_t t = ~0;
  uint64_t s = 0;
  uint64_t offset = header.data_start - total_size;
  uint64_t length = total_size;
  header.data_start = offset;

  if (total_size > header.free_space) {
    std::cout << "No space in page to add slot \n";
    std::cout << *this;
    std::cout << "free space: " << header.free_space << "\n";
    std::cout << "requested size: " << total_size << "\n";
    exit(0);
  }

  uint64_t slotValue = 0;
  slotValue += t << 56;
  slotValue += s << 56 >> 8;
  slotValue += (offset << 40 >> 16);
  slotValue += (length << 40 >> 40);

  // Slot newSlot;
  // newSlot.value = slotValue;

  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));


  if (header.first_free_slot == header.slot_count) {
    slots[header.slot_count].value = slotValue;
    header.slot_count++;
  } else {
    slots[header.first_free_slot].value = slotValue;
  }

  uint32_t slot_space = header.slot_count * sizeof(Slot);
  header.free_space = header.data_start - slot_space - sizeof(header);

  TID new_tid = TID(header.overall_page_id, header.first_free_slot);

  bool found_empty_slot = false;
  for (uint16_t slot_itr = 0; slot_itr < header.slot_count; slot_itr++) {
    Slot l = slots[slot_itr];
    if (l.value == 0) {
      found_empty_slot = true;
      header.first_free_slot = slot_itr;
      break;
    }
  }

  if (!found_empty_slot) {
    header.first_free_slot = header.slot_count;
  }
  
  char* data_ptr = header.buffer_frame + offset;
  *reinterpret_cast<uint64_t*>(data_ptr) = timestamp;                    
  *reinterpret_cast<uint64_t*>(data_ptr + sizeof(uint64_t)) = 0;         
  header.last_modified_txn_id = txn_id;
  header.page_version_timestamp = timestamp;

  return new_tid;
}

bool SlottedPage::markDeleted(uint16_t slotId, uint64_t txn_id, uint64_t timestamp) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  Slot &slot = slots[slotId];
  
  if (slot.value == 0) {
    return false; 
  }
  
  uint64_t value = slot.value;
  uint64_t offset = value << 16 >> 40;
  
  char* data_ptr = header.buffer_frame + offset;
  // uint64_t *begin_ts_ptr = reinterpret_cast<uint64_t*>(data_ptr);
  uint64_t *end_ts_ptr = reinterpret_cast<uint64_t*>(data_ptr + sizeof(uint64_t));
  
  if (*end_ts_ptr != 0) {
    return false; 
  }
  
  *end_ts_ptr = timestamp;
  
  header.last_modified_txn_id = txn_id;
  header.page_version_timestamp = timestamp;
  
  return true;
}

bool SlottedPage::isVisible(uint16_t slotId, uint64_t txn_start_timestamp) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  Slot &slot = slots[slotId];
  
  if (slot.value == 0) {
    return false; 
  }
  
  uint64_t value = slot.value;
  uint64_t offset = value << 16 >> 40;
  
  char* data_ptr = header.buffer_frame + offset;
  uint64_t begin_timestamp = *reinterpret_cast<uint64_t*>(data_ptr);
  uint64_t end_timestamp = *reinterpret_cast<uint64_t*>(data_ptr + sizeof(uint64_t));
  bool created_before_txn = begin_timestamp <= txn_start_timestamp;
  bool deleted_after_txn_or_active = (end_timestamp == 0) || (end_timestamp > txn_start_timestamp);
  
  return created_before_txn && deleted_after_txn_or_active;
}

char* SlottedPage::getDataPtr(uint16_t slotId) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  Slot &slot = slots[slotId];
  
  if (slot.value == 0) {
    return nullptr; 
  }
  uint64_t value = slot.value;
  uint64_t offset = value << 16 >> 40;
  return header.buffer_frame + offset + (2 * sizeof(uint64_t));
}

uint32_t SlottedPage::getDataSize(uint16_t slotId) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  Slot &slot = slots[slotId];
  
  if (slot.value == 0) {
    return 0; // 
  }
  
  uint64_t value = slot.value;
  uint64_t length = value << 40 >> 40;
    return length - (2 * sizeof(uint64_t));
}

bool SlottedPage::getVersionInfo(uint16_t slotId, uint64_t &begin_ts, uint64_t &end_ts) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  Slot &slot = slots[slotId];
  
  if (slot.value == 0) {
    return false; 
  }
  
  uint64_t value = slot.value;
  uint64_t offset = value << 16 >> 40;
    char* data_ptr = header.buffer_frame + offset;
  begin_ts = *reinterpret_cast<uint64_t*>(data_ptr);
  end_ts = *reinterpret_cast<uint64_t*>(data_ptr + sizeof(uint64_t));
  
  return true;
}

void SlottedPage::garbageCollect(uint64_t oldest_active_timestamp) {
  auto *slots = reinterpret_cast<Slot *>(header.buffer_frame + sizeof(header));
  bool need_compaction = false;
  
  for (uint16_t slot_id = 0; slot_id < header.slot_count; slot_id++) {
    Slot &slot = slots[slot_id];
    
    if (slot.value == 0) {
      continue; 
    }
    
    uint64_t begin_ts, end_ts;
    if (getVersionInfo(slot_id, begin_ts, end_ts)) {
      if (end_ts != 0 && end_ts < oldest_active_timestamp) {
        slot.value = 0; 
        need_compaction = true;
      }
    }
  }

  if (need_compaction) {
    compactify(header.data_start + header.free_space);
  }
}