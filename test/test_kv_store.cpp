#include "WAL.hpp"
#include "MemTable.hpp"
#include "SSTable.hpp"
#include "KVStore.hpp"
#include <iostream>
#include <cassert>
#include <filesystem>

void test_wal() {
    std::filesystem::path test_dir = "./test_db";
    std::filesystem::remove_all(test_dir);
    
    // Write
    {
        lsm::WAL wal(test_dir);
        wal.Append(std::string("key1"), std::string("value1"));
        wal.Append(std::string("key2"), std::string("value2"));
    }

    // Recover
    {
        lsm::WAL wal(test_dir);
        auto recovered = wal.Recover();
        assert(recovered.size() == 2);
        assert(recovered[0].first == "key1");
        assert(recovered[0].second == "value1");
        assert(recovered[1].first == "key2");
        assert(recovered[1].second == "value2");
        
        wal.Clear();
        auto recovered_after_clear = wal.Recover();
        assert(recovered_after_clear.size() == 0);
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
    assert(!mt.Get("k3").has_value());
    mt.Clear();
    assert(!mt.Get("k1").has_value());
    std::cout << "MemTable tests passed.\n";
}

void test_sstable() {
    std::filesystem::path test_file = "./test_sstable.sst";
    
    // Write
    {
        lsm::MemTable mt;
        mt.Put("a", "apple");
        mt.Put("b", "banana");
        mt.Put("c", "carrot");
        
        lsm::SSTableWriter::Flush(mt, test_file);
    }
    
    // Read
    {
        lsm::SSTableReader reader(test_file);
        assert(reader.Get("a").value() == "apple");
        assert(reader.Get("b").value() == "banana");
        assert(reader.Get("c").value() == "carrot");
        assert(!reader.Get("d").has_value());
        assert(!reader.Get("0").has_value()); // Something smaller than everything
    }
    
    std::filesystem::remove(test_file);
    std::cout << "SSTable tests passed.\n";
}

void test_kvstore() {
    std::filesystem::path test_dir = "./test_kvstore_db";
    std::filesystem::remove_all(test_dir);
    
    {
        lsm::KVStore store(test_dir, 100); // Small limit to force flush
        store.Put("key1", "val1");
        store.Put("key2", "val2");
        store.Put("key3", "val3"); // This should trigger a flush to SSTable
        
        assert(store.Get("key1").value() == "val1");
        assert(store.Get("key2").value() == "val2");
        assert(store.Get("key3").value() == "val3");
    } // Store is destroyed
    
    // Recover
    {
        lsm::KVStore store2(test_dir, 100);
        assert(store2.Get("key1").value() == "val1");
        assert(store2.Get("key2").value() == "val2");
        assert(store2.Get("key3").value() == "val3");
    }
    
    std::filesystem::remove_all(test_dir);
    std::cout << "KVStore Engine API tests passed.\n";
}

int main() {
    std::cout << "Running KV Store Tests...\n";
    test_wal();
    test_memtable();
    test_sstable();
    test_kvstore();
    return 0;
}
