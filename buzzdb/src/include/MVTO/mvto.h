#pragma once

#include "./transaction/transaction_manager.h"
#include "./buffer/buffer_manager.h"

namespace buzzdb {

class MVTOProtocol {
public:
    /// Validate a read operation for MVTO
    /// @param txn The transaction attempting the read
    /// @param tuple_timestamp Timestamp of the tuple being read
    /// @return true if read is valid, false if should abort
    static bool validate_read(Transaction& txn, uint64_t tuple_timestamp);

    /// Validate a write operation for MVTO
    /// @param txn The transaction attempting the write
    /// @param tuple_timestamp Timestamp of the tuple being written
    /// @return true if write is valid, false if should abort
    static bool validate_write(Transaction& txn, uint64_t tuple_timestamp);

    /// Assign a timestamp to a new transaction
    /// @param txn_manager The transaction manager
    /// @return New timestamp
    static uint64_t assign_timestamp(TransactionManager& txn_manager);
};

} // namespace buzzdb