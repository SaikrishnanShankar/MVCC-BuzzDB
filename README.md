# BuzzDB MVCC Implementation

![Database Logo](https://img.icons8.com/color/96/000000/database.png)

**Team Members**: Aruneswari Sankar, Salkrishnan Sankar  

## 📌 Project Overview

We've implemented **Multi-Version Concurrency Control (MVCC)** in BuzzDB

## 🛠 Technical Implementation

### Core Components

1. **Versioned Storage Engine** (`buffer_manager.cc/h`)
   - Each tuple now has a chain of versions
   - New versions are appended instead of in-place updates
   - Version headers store creation timestamps

2. **Transaction Manager** (`transaction_manager.cc/h`)
   - Assigns unique timestamps to transactions
   - Tracks read/write sets for validation
   - Handles commit/abort operations

3. **MVTO Protocol** (`mvto.cc/h`)
   - Uses timestamp ordering for conflicts
   - Read validation: `txn_timestamp > version_timestamp`
   - Write validation: No overlapping reads from newer transactions


###How to Test

# Clone repository

# Build with CMake
mkdir build && cd build
cmake .. && make

# Run tests
./test/buzzdb_tests

# Run benchmarks
./benchmark/mvcc_benchmark
