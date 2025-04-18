#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <random>
#include <thread>
#include <vector>
#include <future>

#include "buffer/buffer_manager.h"
#include "heap/heap_file.h"
#include "transaction/transaction_manager.h"
#include "log/log_manager.h"

using buzzdb::LogManager;
using buzzdb::BufferManager;
using buzzdb::HeapSegment;
using buzzdb::TransactionManager;
using buzzdb::TID;
using buzzdb::BufferFrame;
using buzzdb::SlottedPage;
using buzzdb::File;

namespace buzzdb {
    const std::string LOG_FILE_PATH = "log_file.log";
}

const char* LOG_FILE = buzzdb::LOG_FILE_PATH.c_str();
const uint64_t WAIT_INTERVAL = 2;
const uint64_t TIMEOUT = 1;

namespace {

TID insert_row(HeapSegment& heap_segment,
        TransactionManager& transaction_manager,
        uint64_t txn_id,
        uint64_t table_id, uint64_t field) {
    (void)transaction_manager;

    auto tuple_size = sizeof(uint64_t)*2; 
    auto tid = heap_segment.allocate(tuple_size, txn_id);
    std::vector<char> buf;
    buf.resize(tuple_size);
    memcpy(buf.data() + 0, &table_id, sizeof(uint64_t));
    memcpy(buf.data() + sizeof(uint64_t), &field, sizeof(uint64_t));

    heap_segment.write(tid, reinterpret_cast<std::byte *>(buf.data()),
            tuple_size, txn_id);
    return tid;
}

void update_row(HeapSegment& heap_segment,
        TransactionManager& transaction_manager,
        uint64_t txn_id,
        TID tid, uint64_t table_id, uint64_t new_field) {
    (void)transaction_manager;

    auto tuple_size = sizeof(uint64_t)*2; 

    std::vector<char> buf;
    buf.resize(tuple_size);
    memcpy(buf.data() + 0, &table_id, sizeof(uint64_t));
    memcpy(buf.data() + sizeof(uint64_t), &new_field, sizeof(uint64_t));

    heap_segment.write(tid, reinterpret_cast<std::byte *>(buf.data()),
            tuple_size, txn_id);
}

bool read_row(HeapSegment& heap_segment,
        uint64_t txn_id,
        TID tid, uint64_t& table_id, uint64_t& field) {

    auto tuple_size = sizeof(uint64_t)*2; 
    std::vector<char> buf;
    buf.resize(tuple_size);
    
    uint32_t bytes_read = heap_segment.read(tid, reinterpret_cast<std::byte *>(buf.data()),
            tuple_size, txn_id);
    
    if (bytes_read == 0) {
        return false; 
    }
    memcpy(&table_id, buf.data() + 0, sizeof(uint64_t));
    memcpy(&field, buf.data() + sizeof(uint64_t), sizeof(uint64_t));
    
    return true;
}

bool delete_row(HeapSegment& heap_segment,
        TransactionManager& transaction_manager,
        uint64_t txn_id,
        TID tid) {
    (void)transaction_manager;

    return heap_segment.remove(tid, txn_id) > 0;
}

TEST(MVTOTest, BasicVersionVisibility) {
    auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
    LogManager log_manager(logfile.get());
    TransactionManager* txn_manager_ptr = new TransactionManager(log_manager, *(BufferManager*)nullptr);
    BufferManager* buffer_manager_ptr = new BufferManager(128, 10, *txn_manager_ptr);
    delete txn_manager_ptr;
    txn_manager_ptr = new TransactionManager(log_manager, *buffer_manager_ptr);
    BufferManager& buffer_manager = *buffer_manager_ptr;
    TransactionManager& transaction_manager = *txn_manager_ptr;
    HeapSegment heap_segment(1, log_manager, buffer_manager, transaction_manager);
    
    buffer_manager.discard_all_pages();
    uint64_t txn1 = transaction_manager.start_txn();
    uint64_t table_id = 101;
    TID tid = insert_row(heap_segment, transaction_manager, txn1, table_id, 5);
    transaction_manager.commit_txn(txn1);
    uint64_t txn2 = transaction_manager.start_txn();
    uint64_t read_table_id, read_field;
    bool success = read_row(heap_segment, txn2, tid, read_table_id, read_field);
    EXPECT_TRUE(success);
    EXPECT_EQ(read_table_id, table_id);
    EXPECT_EQ(read_field, 5);
    transaction_manager.commit_txn(txn2);
    delete buffer_manager_ptr;
    delete txn_manager_ptr;
}

TEST(MVTOTest, VersionUpdates) {
    auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
    LogManager log_manager(logfile.get());
    TransactionManager* txn_manager_ptr = new TransactionManager(log_manager, *(BufferManager*)nullptr);
    BufferManager* buffer_manager_ptr = new BufferManager(128, 10, *txn_manager_ptr);
    delete txn_manager_ptr;
    txn_manager_ptr = new TransactionManager(log_manager, *buffer_manager_ptr);
    BufferManager& buffer_manager = *buffer_manager_ptr;
    TransactionManager& transaction_manager = *txn_manager_ptr;
    HeapSegment heap_segment(2, log_manager, buffer_manager, transaction_manager);
    buffer_manager.discard_all_pages();
    uint64_t txn1 = transaction_manager.start_txn();
    uint64_t table_id = 101;
    TID tid = insert_row(heap_segment, transaction_manager, txn1, table_id, 10);
    transaction_manager.commit_txn(txn1);
    uint64_t txn2 = transaction_manager.start_txn();
    update_row(heap_segment, transaction_manager, txn2, tid, table_id, 20);
    uint64_t txn3 = transaction_manager.start_txn();
    transaction_manager.commit_txn(txn2);
    uint64_t read_table_id, read_field;
    bool success = read_row(heap_segment, txn3, tid, read_table_id, read_field);
    EXPECT_TRUE(success);
    EXPECT_EQ(read_table_id, table_id);
    EXPECT_EQ(read_field, 10);
    uint64_t txn4 = transaction_manager.start_txn();
    success = read_row(heap_segment, txn4, tid, read_table_id, read_field);
    EXPECT_TRUE(success);
    EXPECT_EQ(read_table_id, table_id);
    EXPECT_EQ(read_field, 20);
    transaction_manager.commit_txn(txn3);
    transaction_manager.commit_txn(txn4);
    delete buffer_manager_ptr;
    delete txn_manager_ptr;
}

TEST(MVTOTest, TransactionAbort) {
    auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
    LogManager log_manager(logfile.get());
    TransactionManager* txn_manager_ptr = new TransactionManager(log_manager, *(BufferManager*)nullptr);
    BufferManager* buffer_manager_ptr = new BufferManager(128, 10, *txn_manager_ptr);
    delete txn_manager_ptr;
    txn_manager_ptr = new TransactionManager(log_manager, *buffer_manager_ptr);
    BufferManager& buffer_manager = *buffer_manager_ptr;
    TransactionManager& transaction_manager = *txn_manager_ptr;
    HeapSegment heap_segment(3, log_manager, buffer_manager, transaction_manager);
    buffer_manager.discard_all_pages();
    uint64_t txn1 = transaction_manager.start_txn();
    uint64_t table_id = 101;
    TID tid = insert_row(heap_segment, transaction_manager, txn1, table_id, 30);
    transaction_manager.commit_txn(txn1);
    uint64_t txn2 = transaction_manager.start_txn();
    update_row(heap_segment, transaction_manager, txn2, tid, table_id, 40);
    transaction_manager.abort_txn(txn2);
    uint64_t txn3 = transaction_manager.start_txn();
    uint64_t read_table_id, read_field;
    bool success = read_row(heap_segment, txn3, tid, read_table_id, read_field);
    EXPECT_TRUE(success);
    EXPECT_EQ(read_table_id, table_id);
    EXPECT_EQ(read_field, 30);
    transaction_manager.commit_txn(txn3);
    delete buffer_manager_ptr;
    delete txn_manager_ptr;
}

TEST(MVTOTest, ConcurrentTransactions) {
    auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
    LogManager log_manager(logfile.get());
    TransactionManager* txn_manager_ptr = new TransactionManager(log_manager, *(BufferManager*)nullptr);
    BufferManager* buffer_manager_ptr = new BufferManager(128, 10, *txn_manager_ptr);
    delete txn_manager_ptr;
    txn_manager_ptr = new TransactionManager(log_manager, *buffer_manager_ptr);
    BufferManager& buffer_manager = *buffer_manager_ptr;
    TransactionManager& transaction_manager = *txn_manager_ptr;
    HeapSegment heap_segment(4, log_manager, buffer_manager, transaction_manager);
    buffer_manager.discard_all_pages();
    uint64_t txn1 = transaction_manager.start_txn();
    uint64_t table_id = 101;
    TID tid1 = insert_row(heap_segment, transaction_manager, txn1, table_id, 50);
    TID tid2 = insert_row(heap_segment, transaction_manager, txn1, table_id, 60);
    uint64_t txn2 = transaction_manager.start_txn();
    uint64_t read_table_id, read_field;
    bool success1 = read_row(heap_segment, txn2, tid1, read_table_id, read_field);
    bool success2 = read_row(heap_segment, txn2, tid2, read_table_id, read_field);
    EXPECT_FALSE(success1);
    EXPECT_FALSE(success2);
    transaction_manager.commit_txn(txn1);
    uint64_t txn3 = transaction_manager.start_txn();
    success1 = read_row(heap_segment, txn3, tid1, read_table_id, read_field);
    EXPECT_TRUE(success1);
    EXPECT_EQ(read_field, 50);
    success2 = read_row(heap_segment, txn3, tid2, read_table_id, read_field);
    EXPECT_TRUE(success2);
    EXPECT_EQ(read_field, 60);
    success1 = read_row(heap_segment, txn2, tid1, read_table_id, read_field);
    EXPECT_FALSE(success1);
    transaction_manager.commit_txn(txn2);
    transaction_manager.commit_txn(txn3);
    delete buffer_manager_ptr;
    delete txn_manager_ptr;
}

TEST(MVTOTest, DeleteFunctionality) {
    auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
    LogManager log_manager(logfile.get());
    TransactionManager* txn_manager_ptr = new TransactionManager(log_manager, *(BufferManager*)nullptr);
    BufferManager* buffer_manager_ptr = new BufferManager(128, 10, *txn_manager_ptr);
    delete txn_manager_ptr;
    txn_manager_ptr = new TransactionManager(log_manager, *buffer_manager_ptr);
    BufferManager& buffer_manager = *buffer_manager_ptr;
    TransactionManager& transaction_manager = *txn_manager_ptr;
    HeapSegment heap_segment(5, log_manager, buffer_manager, transaction_manager);
    buffer_manager.discard_all_pages();
    uint64_t txn1 = transaction_manager.start_txn();
    uint64_t table_id = 101;
    TID tid = insert_row(heap_segment, transaction_manager, txn1, table_id, 70);
    transaction_manager.commit_txn(txn1);
    uint64_t txn2 = transaction_manager.start_txn();
    bool delete_success = delete_row(heap_segment, transaction_manager, txn2, tid);
    EXPECT_TRUE(delete_success);
    uint64_t txn3 = transaction_manager.start_txn();
    transaction_manager.commit_txn(txn2);
    uint64_t read_table_id, read_field;
    bool success = read_row(heap_segment, txn3, tid, read_table_id, read_field);
    EXPECT_TRUE(success);
    EXPECT_EQ(read_table_id, table_id);
    EXPECT_EQ(read_field, 70);
    uint64_t txn4 = transaction_manager.start_txn();
    success = read_row(heap_segment, txn4, tid, read_table_id, read_field);
    EXPECT_FALSE(success);
    transaction_manager.commit_txn(txn3);
    transaction_manager.commit_txn(txn4);
    delete buffer_manager_ptr;
    delete txn_manager_ptr;
}

TEST(MVTOTest, GarbageCollection) {
    auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
    LogManager log_manager(logfile.get());
    TransactionManager* txn_manager_ptr = new TransactionManager(log_manager, *(BufferManager*)nullptr);
    BufferManager* buffer_manager_ptr = new BufferManager(128, 10, *txn_manager_ptr);
    delete txn_manager_ptr;
    txn_manager_ptr = new TransactionManager(log_manager, *buffer_manager_ptr);
    BufferManager& buffer_manager = *buffer_manager_ptr;
    TransactionManager& transaction_manager = *txn_manager_ptr;
    HeapSegment heap_segment(6, log_manager, buffer_manager, transaction_manager);
    buffer_manager.discard_all_pages();
    uint64_t table_id = 101;
    uint64_t txn1 = transaction_manager.start_txn();
    TID tid = insert_row(heap_segment, transaction_manager, txn1, table_id, 100);
    transaction_manager.commit_txn(txn1);
    uint64_t txn2 = transaction_manager.start_txn();
    update_row(heap_segment, transaction_manager, txn2, tid, table_id, 200);
    transaction_manager.commit_txn(txn2);
    uint64_t txn3 = transaction_manager.start_txn();
    update_row(heap_segment, transaction_manager, txn3, tid, table_id, 300);
    transaction_manager.commit_txn(txn3);
    transaction_manager.garbage_collect();
    uint64_t txn4 = transaction_manager.start_txn();
    uint64_t read_table_id, read_field;
    bool success = read_row(heap_segment, txn4, tid, read_table_id, read_field);
    EXPECT_TRUE(success);
    EXPECT_EQ(read_field, 300);
    transaction_manager.commit_txn(txn4);
    heap_segment.garbage_collect(transaction_manager.get_txn_start_timestamp(txn4));
    uint64_t txn5 = transaction_manager.start_txn();
    success = read_row(heap_segment, txn5, tid, read_table_id, read_field);
    EXPECT_TRUE(success);
    EXPECT_EQ(read_field, 300);
    transaction_manager.commit_txn(txn5);
    delete buffer_manager_ptr;
    delete txn_manager_ptr;
}

}  

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
