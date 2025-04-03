#include "mvto.h"
#include "transaction_manager.h"

namespace buzzdb {

bool MVTOProtocol::validate_read(Transaction& txn, uint64_t tuple_timestamp) {
    // MVTO read validation rule:
    // A transaction can read a version if the version was created by a committed
    // transaction with timestamp < txn.timestamp_
    return tuple_timestamp < txn.timestamp_;
}

bool MVTOProtocol::validate_write(Transaction& txn, uint64_t tuple_timestamp) {
    // MVTO write validation rules:
    // 1. If the tuple was last written by a txn with higher timestamp, abort
    // 2. If the tuple was read by a txn with higher timestamp, abort
    if (tuple_timestamp > txn.timestamp_) {
        return false;
    }
    
    // For now, skip checking read timestamps (will implement fully later)
    return true;
}

uint64_t MVTOProtocol::assign_timestamp(TransactionManager& txn_manager) {
    // Simple timestamp assignment - increment global counter
    return txn_manager.timestamp_counter_++;
}

} // namespace buzzdb