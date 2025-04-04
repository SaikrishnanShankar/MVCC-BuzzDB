#include "./../include/mvto/mvto.h"
#include "./../include/transaction/transaction_manager.h"

namespace buzzdb {

bool MVTOProtocol::validate_read(Transaction& txn, uint64_t tuple_timestamp) {
    return tuple_timestamp < txn.timestamp_;
}

bool MVTOProtocol::validate_write(Transaction& txn, uint64_t tuple_timestamp) {
    if (tuple_timestamp > txn.timestamp_) {
        return false;
    }
    
    // Checking if any transaction with higher timestamp has read this tuple
    for (const auto& read_txn : txn.read_set_) {
        if (read_txn > txn.timestamp_) {
            return false;
        }
    }
    
    return true;
}

uint64_t MVTOProtocol::assign_timestamp(TransactionManager& txn_manager) {
    return txn_manager.timestamp_counter_++;
}

} // namespace buzzdb