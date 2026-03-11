#pragma once

#include <concepts>
#include <type_traits>
#include <span>
#include <cstddef>
#include <string>
#include <vector>
#include <memory_resource>

namespace lsm {

/**
 * @concept TriviallyCopyable
 * @brief Ensures the type can be safely copied byte-by-byte.
 */
template <typename T>
concept TriviallyCopyable = std::is_trivially_copyable_v<T>;

/**
 * @concept ContiguousByteContainer
 * @brief Ensures the type is a container of bytes (e.g. string, vector<byte>) and is contiguous.
 */
template <typename T>
concept ContiguousByteContainer = requires(T a) {
    { std::span<const std::byte>(reinterpret_cast<const std::byte*>(std::data(a)), std::size(a) * sizeof(typename T::value_type)) };
};

/**
 * @concept Serializable
 * @brief Ensures the type can be serialized into a byte sequence.
 */
template <typename T>
concept Serializable = TriviallyCopyable<T> || ContiguousByteContainer<T>;

/**
 * @brief Converts a Serializable object to a read-only span of bytes.
 */
template <Serializable T>
std::span<const std::byte> to_span(const T& value) {
    if constexpr (ContiguousByteContainer<T>) {
        return std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(std::data(value)), 
            std::size(value) * sizeof(typename T::value_type)
        );
    } else {
        return std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(&value), 
            sizeof(T)
        );
    }
}

// Default Key and Value types using polymorphic memory resources
// This ensures they can be allocated efficiently from a monotonic_buffer_resource in a MemTable
using KeyType = std::pmr::string;
using ValueType = std::pmr::string;

} // namespace lsm
