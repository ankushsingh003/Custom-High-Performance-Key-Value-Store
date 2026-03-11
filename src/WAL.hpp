#pragma once

#include "Concepts.hpp"
#include <filesystem>
#include <fstream>
#include <cstdint>
#include <vector>
#include <utility>
#include <stdexcept>
#include <iostream>

namespace lsm {

/**
 * @class WAL
 * @brief Write-Ahead Log for guaranteeing durability of writes before reaching SSTable.
 */
class WAL {
public:
    explicit WAL(const std::filesystem::path& db_dir) 
        : log_path_(db_dir / "wal.log") 
    {
        if (!std::filesystem::exists(db_dir)) {
            std::filesystem::create_directories(db_dir);
        }
        
        // Open for append. If file doesn't exist, it will be created.
        out_stream_.open(log_path_, std::ios::app | std::ios::binary);
        if (!out_stream_.is_open()) {
            throw std::runtime_error("Failed to open WAL file for writing.");
        }
    }

    ~WAL() {
        if (out_stream_.is_open()) {
            out_stream_.flush();
            out_stream_.close();
        }
    }

    /**
     * @brief Appends a key-value pair to the log synchronously.
     */
    template <Serializable K, Serializable V>
    void Append(const K& key, const V& value) {
        auto key_span = to_span(key);
        auto val_span = to_span(value);

        uint64_t key_len = key_span.size();
        uint64_t val_len = val_span.size();

        out_stream_.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
        out_stream_.write(reinterpret_cast<const char*>(key_span.data()), key_len);

        out_stream_.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
        out_stream_.write(reinterpret_cast<const char*>(val_span.data()), val_len);

        // Ensure the write is flushed to OS/disk.
        out_stream_.flush();
    }

    /**
     * @brief Clears the log file (usually called after MemTable successfully flushed to SSTable).
     */
    void Clear() {
        if (out_stream_.is_open()) {
            out_stream_.close();
        }
        // Truncate file
        out_stream_.open(log_path_, std::ios::out | std::ios::trunc | std::ios::binary);
        if (!out_stream_.is_open()) {
            throw std::runtime_error("Failed to truncate WAL file.");
        }
    }

    /**
     * @brief Recovers all key-value pairs from the log.
     * @return A vector of parsed key-value pairs.
     */
    std::vector<std::pair<KeyType, ValueType>> Recover() const {
        std::vector<std::pair<KeyType, ValueType>> recovered_data;
        if (!std::filesystem::exists(log_path_) || std::filesystem::file_size(log_path_) == 0) {
            return recovered_data;
        }

        std::ifstream in_stream(log_path_, std::ios::in | std::ios::binary);
        if (!in_stream.is_open()) {
            return recovered_data;
        }

        while (in_stream.peek() != EOF) {
            uint64_t key_len = 0;
            if (!in_stream.read(reinterpret_cast<char*>(&key_len), sizeof(key_len))) break;

            std::string key_str(key_len, '\0');
            in_stream.read(key_str.data(), key_len);

            uint64_t val_len = 0;
            if (!in_stream.read(reinterpret_cast<char*>(&val_len), sizeof(val_len))) break;

            std::string val_str(val_len, '\0');
            in_stream.read(val_str.data(), val_len);

            // Using pmr::string for KeyType and ValueType, which can be constructed from std::string
            recovered_data.emplace_back(KeyType(key_str), ValueType(val_str));
        }

        return recovered_data;
    }

private:
    std::filesystem::path log_path_;
    std::ofstream out_stream_;
};

} // namespace lsm
