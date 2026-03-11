#pragma once

#include "Concepts.hpp"
#include "SSTable.hpp"
#include <filesystem>
#include <vector>
#include <map>
#include <string>
#include <iostream>
#include <fstream>
#include <optional>
#include <memory>
#include <stdexcept>

namespace lsm {

/**
 * @class Compactor
 * @brief Merges multiple SSTables into a single one to reclaim space and optimize reads.
 */
class Compactor {
public:
    /**
     * @brief Merges multiple SSTables and creates a new compacted one.
     * @param sstable_paths List of paths to SSTables in chronological order (oldest first).
     * @param output_path Path to generate the compacted SSTable.
     */
    static void Compact(const std::vector<std::filesystem::path>& sstable_paths, const std::filesystem::path& output_path) {
        // We use an in-memory map to resolve conflicts (last written wins).
        // Since older SSTables are read first, newer SSTables overwrite their keys automatically.
        std::map<std::string, std::string> merged_data;

        for (const auto& path : sstable_paths) {
            std::ifstream in(path, std::ios::binary);
            if (!in.is_open()) continue;

            in.seekg(0, std::ios::end);
            uint64_t file_size = in.tellg();
            if (file_size < sizeof(uint64_t)) continue;

            in.seekg(file_size - sizeof(uint64_t), std::ios::beg);
            uint64_t index_offset;
            in.read(reinterpret_cast<char*>(&index_offset), sizeof(index_offset));

            in.seekg(0, std::ios::beg);

            while (in.peek() != EOF) {
                if (static_cast<uint64_t>(in.tellg()) >= index_offset) break;

                uint64_t key_len;
                if (!in.read(reinterpret_cast<char*>(&key_len), sizeof(key_len))) break;

                std::string key(key_len, '\0');
                in.read(key.data(), key_len);

                uint64_t val_len;
                if (!in.read(reinterpret_cast<char*>(&val_len), sizeof(val_len))) break;

                std::string val(val_len, '\0');
                in.read(val.data(), val_len);

                merged_data[key] = val;
            }
        }

        // Write the merged data directly to the new SSTable
        std::ofstream out(output_path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            throw std::runtime_error("Could not open compacted SSTable for writing.");
        }

        std::map<KeyType, uint64_t> sparse_index;
        size_t entry_count = 0;

        for (const auto& [key, val] : merged_data) {
            uint64_t current_offset = out.tellp();

            if (entry_count % 64 == 0) {
                sparse_index[KeyType(key)] = current_offset;
            }

            uint64_t key_len = key.size();
            uint64_t val_len = val.size();

            out.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
            out.write(reinterpret_cast<const char*>(key.data()), key_len);

            out.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
            out.write(reinterpret_cast<const char*>(val.data()), val_len);

            entry_count++;
        }

        // Write index block
        uint64_t index_offset = out.tellp();
        uint64_t index_size = sparse_index.size();
        out.write(reinterpret_cast<const char*>(&index_size), sizeof(index_size));

        for (const auto& [idx_key, offset] : sparse_index) {
            std::string k(idx_key.data(), idx_key.size());
            uint64_t key_len = k.size();
            out.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
            out.write(reinterpret_cast<const char*>(k.data()), key_len);
            out.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
        }

        out.write(reinterpret_cast<const char*>(&index_offset), sizeof(index_offset));
    }
};

} // namespace lsm
