#pragma once

#include "Concepts.hpp"
#include "WAL.hpp"
#include "MemTable.hpp"
#include "SSTable.hpp"
#include <filesystem>
#include <vector>
#include <memory>
#include <optional>
#include <iostream>
#include <algorithm>
#include <string>
#include <shared_mutex>
#include <mutex>

namespace lsm {

const std::string TOMBSTONE = "__LSM_TOMBSTONE__";

/**
 * @class KVStore
 * @brief The main engine tying together WAL, MemTable, and SSTables.
 */
class KVStore {
public:
    explicit KVStore(const std::filesystem::path& db_dir, size_t memtable_limit = 1024 * 1024) 
        : db_dir_(db_dir), 
          memtable_limit_(memtable_limit),
          wal_(db_dir),
          memtable_()
    {
        std::unique_lock lock(mutex_);
        if (!std::filesystem::exists(db_dir_)) {
            std::filesystem::create_directories(db_dir_);
        }

        // Recover existing SSTables from disk
        DiscoverSSTables();

        // Recover WAL and populate initial MemTable
        auto recovered_wal = wal_.Recover();
        for (const auto& [k, v] : recovered_wal) {
            memtable_.Put(std::string(k.data(), k.size()), std::string(v.data(), v.size()));
        }
        
        // If memtable grew past limit during recovery, flush it
        CheckMemTableFlush();
    }

    /**
     * @brief Inserts or updates a key and its value.
     */
    template <Serializable K, Serializable V>
    void Put(const K& key, const V& value) {
        std::unique_lock lock(mutex_);
        // 1. Write to WAL synchronously to ensure atomicity and durability
        wal_.Append(key, value);

        // 2. Write to MemTable
        auto key_span = to_span(key);
        auto val_span = to_span(value);
        std::string pk(reinterpret_cast<const char*>(key_span.data()), key_span.size());
        std::string pv(reinterpret_cast<const char*>(val_span.data()), val_span.size());

        memtable_.Put(pk, pv);

        // 3. Trigger flush if we cross the allocation threshold
        CheckMemTableFlush();
    }

    /**
     * @brief Deletes a key by writing a tombstone.
     */
    template <Serializable K>
    void Del(const K& key) {
        Put(key, TOMBSTONE);
    }

    /**
     * @brief Retrieves a value for a given key. Checks MemTable then SSTables chronologically.
     */
    template <Serializable K>
    std::optional<std::string> Get(const K& key) const {
        std::shared_lock lock(mutex_);
        auto key_span = to_span(key);
        std::string pk(reinterpret_cast<const char*>(key_span.data()), key_span.size());

        // 1. Quick route: Check active MemTable
        auto mem_val = memtable_.Get(pk);
        if (mem_val.has_value()) {
            if (mem_val.value() == TOMBSTONE) return std::nullopt;
            return mem_val;
        }

        // 2. Read from SSTables, scanning newest first
        for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
            auto sst_val = (*it)->Get(pk);
            if (sst_val.has_value()) {
                if (sst_val.value() == TOMBSTONE) return std::nullopt;
                return sst_val;
            }
        }

        return std::nullopt;
    }

    /**
     * @brief Flushes the active memtable to a new SSTable manually.
     */
    void Flush() {
        if (memtable_.empty()) return;

        std::string sst_name = "sstable_" + std::to_string(next_sstable_id_++) + ".sst";
        std::filesystem::path sst_path = db_dir_ / sst_name;

        // Perform IO intensive sequential background write (synchronous for simplicty in this version)
        SSTableWriter::Flush(memtable_, sst_path);

        sstables_.push_back(std::make_unique<SSTableReader>(sst_path));

        // Note: wal needs clearing, which truncates the log since it's durably written to SSTable
        wal_.Clear();
        // Clear monotonic buffer
        memtable_.Clear();
    }

private:
    std::filesystem::path db_dir_;
    size_t memtable_limit_;
    WAL wal_;
    MemTable memtable_;
    
    std::vector<std::unique_ptr<SSTableReader>> sstables_;
    uint64_t next_sstable_id_{1};

    mutable std::shared_mutex mutex_;

    void DiscoverSSTables() {
        std::vector<std::filesystem::path> sst_paths;
        for (const auto& entry : std::filesystem::directory_iterator(db_dir_)) {
            if (entry.path().extension() == ".sst") {
                sst_paths.push_back(entry.path());
            }
        }
        
        std::sort(sst_paths.begin(), sst_paths.end(), [](const auto& a, const auto& b) {
            std::string sa = a.stem().string();
            std::string sb = b.stem().string();
            try {
                return std::stoi(sa.substr(8)) < std::stoi(sb.substr(8));
            } catch (...) {
                return sa < sb;
            }
        });

        for (const auto& p : sst_paths) {
            sstables_.push_back(std::make_unique<SSTableReader>(p));
            // Correct the ID tracker so next flush makes a new file
            std::string stem = p.stem().string();
            try {
                uint64_t id = std::stoull(stem.substr(8));
                if (id >= next_sstable_id_) next_sstable_id_ = id + 1;
            } catch(...) {}
        }
    }

    void CheckMemTableFlush() {
        if (memtable_.SizeBytes() >= memtable_limit_) {
            Flush();
        }
    }
};

} // namespace lsm
