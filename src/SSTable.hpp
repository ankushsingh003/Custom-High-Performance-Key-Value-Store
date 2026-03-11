#pragma once

#include "MemTable.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdint>
#include <map>

namespace lsm {

/**
 * @class SSTableWriter
 * @brief Flushes MemTable to disk into a Sorted String Table.
 */
class SSTableWriter {
public:
    static void Flush(const MemTable& memtable, const std::filesystem::path& file_path) {
        std::ofstream out(file_path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            throw std::runtime_error("Could not open SSTable for writing.");
        }

        // We will build a simple sparse index of keys to offsets
        std::map<KeyType, uint64_t> sparse_index;
        size_t entry_count = 0;

        for (auto it = memtable.begin(); it != memtable.end(); ++it) {
            uint64_t current_offset = out.tellp();
            const auto& key = it->first;
            const auto& val = it->second;

            // Add every 64th entry to sparse index to balance memory/IO
            if (entry_count % 64 == 0) {
                // To avoid storing pmr string tied to table, convert to std::string
                sparse_index[KeyType(key.data(), key.size())] = current_offset;
            }

            uint64_t key_len = key.size();
            uint64_t val_len = val.size();

            out.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
            out.write(reinterpret_cast<const char*>(key.data()), key_len);

            out.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
            out.write(reinterpret_cast<const char*>(val.data()), val_len);

            entry_count++;
        }

        // Write index block at the end
        uint64_t index_offset = out.tellp();
        
        uint64_t index_size = sparse_index.size();
        out.write(reinterpret_cast<const char*>(&index_size), sizeof(index_size));

        for (const auto& [idx_key, offset] : sparse_index) {
            uint64_t key_len = idx_key.size();
            out.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
            out.write(reinterpret_cast<const char*>(idx_key.data()), key_len);
            out.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
        }

        // Write trailer: 8 bytes for index offset
        out.write(reinterpret_cast<const char*>(&index_offset), sizeof(index_offset));
    }
};

/**
 * @class SSTableReader
 * @brief Reads keys from an SSTable using the sparse index.
 */
class SSTableReader {
public:
    explicit SSTableReader(std::filesystem::path file_path) 
        : file_path_(std::move(file_path)) 
    {
        LoadIndex();
    }

    std::optional<std::string> Get(const std::string& target_key) const {
        if (sparse_index_.empty()) return std::nullopt;

        KeyType key_target(target_key);
        auto it = sparse_index_.upper_bound(key_target);
        if (it != sparse_index_.begin()) {
            --it; // Find the highest key <= target_key
        } else {
            if (it->first > key_target && it->second == 0) {
                return std::nullopt;
            }
        }

        std::ifstream in(file_path_, std::ios::binary);
        in.seekg(it->second);

        while (in.peek() != EOF) {
            if (static_cast<uint64_t>(in.tellg()) >= index_offset_) break; 

            uint64_t key_len;
            if (!in.read(reinterpret_cast<char*>(&key_len), sizeof(key_len))) break;

            std::string key(key_len, '\0');
            in.read(key.data(), key_len);

            uint64_t val_len;
            if (!in.read(reinterpret_cast<char*>(&val_len), sizeof(val_len))) break;

            std::string val(val_len, '\0');
            in.read(val.data(), val_len);

            if (key == target_key) {
                return val;
            } else if (key > target_key) {
                break;
            }
        }
        
        return std::nullopt;
    }

private:
    std::filesystem::path file_path_;
    std::map<KeyType, uint64_t> sparse_index_;
    uint64_t index_offset_{0};

    void LoadIndex() {
        std::ifstream in(file_path_, std::ios::binary);
        if (!in.is_open()) return;

        in.seekg(0, std::ios::end);
        uint64_t file_size = in.tellg();
        if (file_size < sizeof(uint64_t)) return;

        in.seekg(file_size - sizeof(uint64_t), std::ios::beg);
        in.read(reinterpret_cast<char*>(&index_offset_), sizeof(index_offset_));

        in.seekg(index_offset_);
        uint64_t index_size = 0;
        in.read(reinterpret_cast<char*>(&index_size), sizeof(index_size));

        for (uint64_t i = 0; i < index_size; ++i) {
            uint64_t key_len;
            in.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));

            std::string key_str(key_len, '\0');
            in.read(key_str.data(), key_len);
            KeyType key(key_str);

            uint64_t offset;
            in.read(reinterpret_cast<char*>(&offset), sizeof(offset));

            sparse_index_[key] = offset;
        }
    }
};

} // namespace lsm
