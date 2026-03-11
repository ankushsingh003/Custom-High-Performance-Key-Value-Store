#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <cmath>
#include <algorithm>

namespace lsm {

/**
 * @class BloomFilter
 * @brief A space-efficient probabilistic data structure to test set membership.
 */
class BloomFilter {
public:
    BloomFilter() : bits_(0), num_hashes_(0) {}

    /**
     * @brief Constructs a bloom filter with specific size and hash count.
     */
    BloomFilter(size_t num_elements, double false_positive_rate) {
        // Optimal m = -(n * ln(p)) / (ln(2)^2)
        size_t m = static_cast<size_t>(- (static_cast<double>(num_elements) * std::log(false_positive_rate)) / (std::log(2) * std::log(2)));
        // Optimal k = (m/n) * ln(2)
        num_hashes_ = static_cast<uint32_t>(std::max(1.0, std::round((static_cast<double>(m) / num_elements) * std::log(2))));
        
        bits_.assign((m + 7) / 8, 0); // Round up to bytes
    }

    /**
     * @brief Adds a key to the filter.
     */
    void Add(const std::string& key) {
        for (uint32_t i = 0; i < num_hashes_; ++i) {
            size_t hash = Hash(key, i);
            size_t bit_idx = hash % (bits_.size() * 8);
            bits_[bit_idx / 8] |= (1 << (bit_idx % 8));
        }
    }

    /**
     * @brief Checks if a key might be in the set.
     */
    bool MayContain(const std::string& key) const {
        if (bits_.empty()) return true; // Empty filter shouldn't block
        
        for (uint32_t i = 0; i < num_hashes_; ++i) {
            size_t hash = Hash(key, i);
            size_t bit_idx = hash % (bits_.size() * 8);
            if (!(bits_[bit_idx / 8] & (1 << (bit_idx % 8)))) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief Serializes the bloom filter to a byte vector.
     */
    std::vector<uint8_t> Serialize() const {
        std::vector<uint8_t> data;
        data.reserve(sizeof(uint32_t) + bits_.size());
        
        // Write num_hashes
        data.insert(data.end(), reinterpret_cast<const uint8_t*>(&num_hashes_), reinterpret_cast<const uint8_t*>(&num_hashes_) + sizeof(uint32_t));
        // Write bit array
        data.insert(data.end(), bits_.begin(), bits_.end());
        
        return data;
    }

    /**
     * @brief Deserializes from a buffer.
     */
    void Deserialize(const std::vector<uint8_t>& data) {
        if (data.size() < sizeof(uint32_t)) return;
        
        std::copy(data.begin(), data.begin() + sizeof(uint32_t), reinterpret_cast<uint8_t*>(&num_hashes_));
        bits_.assign(data.begin() + sizeof(uint32_t), data.end());
    }

private:
    std::vector<uint8_t> bits_;
    uint32_t num_hashes_;

    /**
     * @brief Simple DJB2-inspired hash with seed (salt).
     */
    size_t Hash(const std::string& key, uint32_t seed) const {
        size_t h = 5381 + seed;
        for (char c : key) {
            h = ((h << 5) + h) + static_cast<unsigned char>(c);
        }
        return h;
    }
};

} // namespace lsm
