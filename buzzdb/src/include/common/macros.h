#pragma once

#include <limits>
#include <string>

namespace buzzdb {

// MVCC Constants
constexpr uint64_t INVALID_TXN_ID = std::numeric_limits<uint64_t>::max();
constexpr uint64_t MAX_TXN_ID = INVALID_TXN_ID - 1;
constexpr uint64_t INITIAL_TXN_ID = 1;
constexpr uint64_t SYSTEM_TXN_ID = 0;
constexpr uint64_t VERSION_CHAIN_END = 0;

// Page/Slot Constants
constexpr uint64_t INVALID_PAGE_ID = INVALID_TXN_ID;
constexpr uint64_t INVALID_FRAME_ID = INVALID_TXN_ID;
constexpr uint64_t INVALID_NODE_ID = INVALID_TXN_ID;
constexpr uint64_t INVALID_FIELD = INVALID_TXN_ID;

// Sizes
constexpr uint64_t REGISTER_SIZE = 16 + 1;  // + null delimiter
constexpr uint64_t VERSION_METADATA_SIZE = 24; // bytes

// File Paths
const std::string LOG_FILE_PATH = "BuzzDB.log";
const std::string VERSION_STORE_PATH = "BuzzDB.versions";

// Debugging
#define MVCC_DEBUG 0
#if MVCC_DEBUG
#define MVCC_LOG(msg) std::cerr << "[MVCC] " << msg << std::endl
#else
#define MVCC_LOG(msg)
#endif

} // namespace buzzdb