#include "WAL.hpp"
#include "MemTable.hpp"
#include "SSTable.hpp"
#include "KVStore.hpp"
#include "Compactor.hpp"
#include <iostream>
#include <cassert>
#include <filesystem>
#include <thread>
#include <vector>

void test_wal() {
    std::filesystem::path test_dir = "./test_db_wal";
    std::filesystem::remove_all(test_dir);
    
    {
        lsm::WAL wal(test_dir);
        wal.Append(std::string("key1"), std::string("value1"));
        wal.Append(std::string("key2"), std::string("value2"));
    }

    {
        lsm::WAL wal(test_dir);
        auto recovered = wal.Recover();
        assert(recovered.size() == 2);
        assert(recovered[0].first == "key1");
        assert(recovered[1].second == "value2");
        wal.Clear();
    }
    
    std::filesystem::remove_all(test_dir);
    std::cout << "WAL tests passed.\n";
}

void test_memtable() {
    lsm::MemTable mt;
    mt.Put("k1", "v1");
    mt.Put("k2", "v2");
    assert(mt.Get("k1").value() == "v1");
    assert(mt.Get("k2").value() == "v2");
    mt.Clear();
    assert(!mt.Get("k1").has_value());
    std::cout << "MemTable tests passed.\n";
}

void test_sstable() {
    std::filesystem::path test_file = "./test_sstable_v2.sst";
    std::filesystem::remove(test_file);
    
    {
        lsm::MemTable mt;
        mt.Put("a", "apple");
        mt.Put("b", "banana");
        lsm::SSTableWriter::Flush(mt, test_file);
    }
    
    {
        lsm::SSTableReader reader(test_file);
        assert(reader.Get("a").value() == "apple");
        assert(reader.Get("b").value() == "banana");
        assert(!reader.Get("c").has_value());
    }
    
    std::filesystem::remove(test_file);
    std::cout << "SSTable (v2 with Bloom) tests passed.\n";
}

void test_kvstore_basic() {
    std::filesystem::path test_dir = "./test_kvstore_basic";
    std::filesystem::remove_all(test_dir);
    
    {
        lsm::KVStore store(test_dir, 100); 
        store.Put("key1", "val1");
        store.Del("key1");
        assert(!store.Get("key1").has_value());
        
        store.Put("key2", "val2");
        assert(store.Get("key2").value() == "val2");
    }
    
    std::filesystem::remove_all(test_dir);
    std::cout << "KVStore Basic (with Del) tests passed.\n";
}

void test_concurrency() {
    std::filesystem::path test_dir = "./test_kv_concurrency";
    std::filesystem::remove_all(test_dir);
    
    lsm::KVStore store(test_dir, 1024 * 1024);
    const int num_threads = 8;
    const int ops_per_thread = 1000;
    
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&store, i, ops_per_thread]() {
            for (int j = 0; j < ops_per_thread; ++j) {
                std::string key = "key_" + std::to_string(i) + "_" + std::to_string(j);
                std::string val = "val_" + std::to_string(j);
                store.Put(key, val);
                auto res = store.Get(key);
                assert(res.has_value() && res.value() == val);
            }
        });
    }
    
    for (auto& t : threads) t.join();
    
    std::filesystem::remove_all(test_dir);
    std::cout << "Concurrency tests passed.\n";
}

void test_compactor() {
    std::filesystem::path test_dir = "./test_compactor_v2";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    
    auto sst1 = test_dir / "1.sst";
    auto sst2 = test_dir / "2.sst";
    
    { lsm::MemTable mt; mt.Put("key1", "old"); mt.Put("key2", "keep"); lsm::SSTableWriter::Flush(mt, sst1); }
    { lsm::MemTable mt; mt.Put("key1", "new"); mt.Del("key2"); lsm::SSTableWriter::Flush(mt, sst2); }
    
    auto compacted = test_dir / "compact.sst";
    lsm::Compactor::Compact({sst1, sst2}, compacted);
    
    lsm::SSTableReader reader(compacted);
    assert(reader.Get("key1").value() == "new");
    assert(!reader.Get("key2").has_value());
    
    std::filesystem::remove_all(test_dir);
    std::cout << "Compactor (v2 with Tombstones) tests passed.\n";
}

int main() {
    std::cout << "Starting Comprehensive LSM-Tree Tests...\n";
    test_wal();
    test_memtable();
    test_sstable();
    test_kvstore_basic();
    test_compactor();
    test_concurrency();
    std::cout << "All tests passed successfully!\n";
    return 0;
}
