# LSM-Tree Key-Value Store

A high-performance Key-Value store implementation using the Log-Structured Merge-Tree (LSM-Tree) architecture.

## Requirements

This project requires a modern C++ compiler that supports **C++20** (for Concepts and `std::span`):
- **GCC**: Version 10 or later
- **Clang**: Version 10 or later
- **MSVC**: Visual Studio 2019 version 16.11 or later

## How to Build and Run

### Manual Compilation
To compile the test suite manually with GCC:
```bash
g++ -std=c++20 -I./src test/test_kv_store.cpp -o kv_test.exe
./kv_test.exe
```

### Using CMake
If you have CMake installed:
```bash
mkdir build
cd build
cmake ..
cmake --build .
ctest
```

## Project Features
- **MemTable**: In-memory storage using `std::pmr::monotonic_buffer_resource` for low-latency allocation.
- **WAL**: Write-Ahead Logging for durability and crash recovery.
- **SSTable**: Immutable on-disk sorted tables with sparse indexing for efficient lookups.
- **Compactor**: Merges multiple SSTables into a single file to optimize space and performance.
- **C++20 Concepts**: Ensures type safety for key-value serialization.
