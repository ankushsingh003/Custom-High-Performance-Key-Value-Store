#include "WAL.hpp"
#include <iostream>
#include <cassert>
#include <filesystem>

void test_wal() {
    std::filesystem::path test_dir = "./test_db";
    
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

int main() {
    std::cout << "Running KV Store Tests...\n";
    test_wal();
    return 0;
}
