#pragma once

#include "Concepts.hpp"
#include <map>
#include <memory_resource>
#include <optional>
#include <cstdint>
#include <string>

namespace lsm {

/**
 * @class MemTable
 * @brief In-memory key-value store backed by a polymorphic memory resource for high-performance allocation.
 */
class MemTable {
public:
    // We allocate a large initial buffer, e.g., 1MB or 4MB.
    // The monotonic buffer will grow as needed but prevents fragmentation.
    explicit MemTable(size_t initial_buffer_size = 4 * 1024 * 1024) 
        : buffer_resource_(initial_buffer_size),
          allocator_(&buffer_resource_),
          table_(allocator_),
          approximate_size_(0)
    {}

    /**
     * @brief Inserts or updates a key-value pair.
     */
    void Put(const std::string& key, const std::string& value) {
        // Construct pmr strings using the allocator to store inside the monotonic buffer
        KeyType pmr_key(key, allocator_);
        ValueType pmr_value(value, allocator_);

        auto it = table_.find(pmr_key);
        if (it != table_.end()) {
            // Update existing. Note: in monotonic, old value memory is leaked until reset.
            // This is acceptable because MemTables are flushed and cleared periodically.
            approximate_size_ += pmr_value.size();
            it->second = std::move(pmr_value);
        } else {
            // Size estimation: Key size + Value size + map node overhead pointer size approx (32 bytes per node on 64 bit)
            approximate_size_ += pmr_key.size() + pmr_value.size() + 32; 
            table_.emplace(std::move(pmr_key), std::move(pmr_value));
        }
    }

    /**
     * @brief Retrieves a value for a key if it exists.
     * @return The value as an std::optional<std::string>
     */
    std::optional<std::string> Get(const std::string& key) const {
        // Find using a pmr_key constructed with default allocator just for lookup
        KeyType lookup_key(key); 
        auto it = table_.find(lookup_key);
        if (it != table_.end()) {
            // Return a copy so the caller owns it using standard allocator
            return std::string(it->second.data(), it->second.size());
        }
        return std::nullopt;
    }

    /**
     * @brief Clears the table and resets the memory resource.
     * Normally called after flushing to an SSTable.
     */
    void Clear() {
        table_.clear(); // Call destructors
        buffer_resource_.release(); // Reset monotonic buffer memory to start
        approximate_size_ = 0;
    }

    /**
     * @brief Returns iterator to start of internal map
     */
    auto begin() const { return table_.begin(); }
    
    /**
     * @brief Returns iterator to end of internal map
     */
    auto end() const { return table_.end(); }
    
    /**
     * @brief Returns true if MemTable is empty
     */
    bool empty() const { return table_.empty(); }

    /**
     * @brief Gets the estimated size in bytes currently used.
     */
    size_t SizeBytes() const {
        return approximate_size_;
    }

private:
    std::pmr::monotonic_buffer_resource buffer_resource_;
    std::pmr::polymorphic_allocator<std::byte> allocator_;
    
    // std::pmr::map requires the key, value, and allocator
    using MapType = std::map<KeyType, ValueType, std::less<KeyType>, 
                             std::pmr::polymorphic_allocator<std::pair<const KeyType, ValueType>>>;
    MapType table_;

    size_t approximate_size_;
};

} // namespace lsm
