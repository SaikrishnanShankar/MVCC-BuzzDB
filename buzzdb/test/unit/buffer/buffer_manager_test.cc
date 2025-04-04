#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>
#include "buffer/buffer_manager.h"
#include "heap/heap_file.h"
#include "transaction/transaction_manager.h"
#include "log/log_manager.h"

using namespace buzzdb;

const char* LOG_FILE = buzzdb::LOG_FILE_PATH.c_str();

namespace {

/* MVCC-SPECIFIC TEST HELPERS */
struct MVCCTestHelper {
    BufferManager buffer_manager;
    File log_file;
    LogManager log_manager;
    TransactionManager txn_manager;
    HeapSegment heap_segment;

    MVCCTestHelper() : 
        buffer_manager(1024, 10),
        log_file(File::open_file(LOG_FILE, File::WRITE)),
        log_manager(&log_file),
        txn_manager(log_manager, buffer_manager),
        heap_segment(1, log_manager, buffer_manager) {}
};

TID insert_version(MVCCTestHelper& helper, uint64_t txn_id, uint64_t value) {
    auto tuple_size = sizeof(uint64_t);
    TID tid = helper.heap_segment.allocate(tuple_size, txn_id);
    
    std::vector<char> buf(tuple_size);
    memcpy(buf.data(), &value, sizeof(uint64_t));
    
    helper.heap_segment.write(tid, reinterpret_cast<std::byte*>(buf.data()), 
                           tuple_size, txn_id);
    return tid;
}

/* COMPLETED MVCC TESTS */
TEST(MVCCTest, ReadOwnWrites) {
    MVCCTestHelper helper;
    uint64_t txn_id = helper.txn_manager.start_txn();
    
    // Insert initial version
    TID tid = insert_version(helper, txn_id, 42);
    
    // Read should see uncommitted version
    std::vector<char> buf(sizeof(uint64_t));
    helper.heap_segment.read(tid, reinterpret_cast<std::byte*>(buf.data()), 
                         buf.size(), txn_id);
    
    uint64_t read_val;
    memcpy(&read_val, buf.data(), sizeof(uint64_t));
    EXPECT_EQ(42, read_val);
    
    helper.txn_manager.commit_txn(txn_id);
}

TEST(MVCCTest, ReadCommittedIsolation) {
    MVCCTestHelper helper;
    
    // Txn1 writes version
    uint64_t txn1 = helper.txn_manager.start_txn();
    TID tid = insert_version(helper, txn1, 100);
    helper.txn_manager.commit_txn(txn1);
    
    // Txn2 should see committed version
    uint64_t txn2 = helper.txn_manager.start_txn();
    std::vector<char> buf(sizeof(uint64_t));
    helper.heap_segment.read(tid, reinterpret_cast<std::byte*>(buf.data()), 
                           buf.size(), txn2);
    
    uint64_t read_val;
    memcpy(&read_val, buf.data(), sizeof(uint64_t));
    EXPECT_EQ(100, read_val);
    
    helper.txn_manager.commit_txn(txn2);
}

TEST(MVCCTest, WriteConflictDetection) {
    MVCCTestHelper helper;
    
    uint64_t txn1 = helper.txn_manager.start_txn();
    uint64_t txn2 = helper.txn_manager.start_txn();
    
    // Both txns read same tuple
    TID tid = insert_version(helper, 0, 50);
    
    std::vector<char> buf(sizeof(uint64_t));
    helper.heap_segment.read(tid, reinterpret_cast<std::byte*>(buf.data()),
                         buf.size(), txn1);
    helper.heap_segment.read(tid, reinterpret_cast<std::byte*>(buf.data()),
                         buf.size(), txn2);
    
    // Txn1 writes - should succeed
    uint64_t new_val1 = 51;
    helper.heap_segment.write(tid, reinterpret_cast<std::byte*>(&new_val1),
                            sizeof(uint64_t), txn1);
    
    // Txn2 tries to write - should fail
    uint64_t new_val2 = 52;
    EXPECT_THROW(
        helper.heap_segment.write(tid, reinterpret_cast<std::byte*>(&new_val2),
                                sizeof(uint64_t), txn2),
        transaction_abort_error);
    
    helper.txn_manager.commit_txn(txn1);
    helper.txn_manager.abort_txn(txn2);
}

/* CONCURRENCY TESTS */
TEST(ConcurrencyTest, ConcurrentReaders) {
    MVCCTestHelper helper;
    TID tid = insert_version(helper, 0, 123);
    
    auto reader = [&](uint64_t txn_id) {
        std::vector<char> buf(sizeof(uint64_t));
        helper.heap_segment.read(tid, reinterpret_cast<std::byte*>(buf.data()),
                             buf.size(), txn_id);
        uint64_t val;
        memcpy(&val, buf.data(), sizeof(uint64_t));
        EXPECT_EQ(123, val);
    };
    
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([&, i]() {
            uint64_t txn_id = helper.txn_manager.start_txn(true);
            reader(txn_id);
            helper.txn_manager.commit_txn(txn_id);
        });
    }
    
    for (auto& t : threads) t.join();
}

TEST(MVCCTest, VersionChainTraversal) {
    MVCCTestHelper helper;
    uint64_t txn1 = helper.txn_manager.start_txn();
    TID tid = insert_version(helper, txn1, 1);
    helper.txn_manager.commit_txn(txn1);
    uint64_t txn2 = helper.txn_manager.start_txn();
    helper.heap_segment.write(tid, reinterpret_cast<std::byte*>(&(uint64_t){2}),
                         sizeof(uint64_t), txn2);
    helper.txn_manager.commit_txn(txn2);   
    uint64_t txn3 = helper.txn_manager.start_txn();
    helper.heap_segment.write(tid, reinterpret_cast<std::byte*>(&(uint64_t){3}),
                         sizeof(uint64_t), txn3);
    std::vector<char> buf(sizeof(uint64_t));
    helper.heap_segment.read(tid, reinterpret_cast<std::byte*>(buf.data()),
                         buf.size(), txn2);
    
    uint64_t read_val;
    memcpy(&read_val, buf.data(), sizeof(uint64_t));
    EXPECT_EQ(2, read_val);
    
    helper.txn_manager.commit_txn(txn3);
}

TEST(MVCCTest, AbortedVersionInvisible) {
    MVCCTestHelper helper;
    
    uint64_t txn1 = helper.txn_manager.start_txn();
    TID tid = insert_version(helper, txn1, 100);
    helper.txn_manager.abort_txn(txn1);
    
    uint64_t txn2 = helper.txn_manager.start_txn();
    std::vector<char> buf(sizeof(uint64_t));
    
    EXPECT_THROW(
        helper.heap_segment.read(tid, reinterpret_cast<std::byte*>(buf.data()),
                                buf.size(), txn2),
        std::runtime_error);
    
    helper.txn_manager.commit_txn(txn2);
}

} // namespace

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}