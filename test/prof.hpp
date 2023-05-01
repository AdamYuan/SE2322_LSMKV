#pragma once

#include "MurmurHash3.h"
#include <lsm/kv.hpp>

#include <chrono>

template <typename Key> struct Murmur3BloomHasher {
	template <std::size_t Bits, typename Array> inline static void Insert(Array &array, const Key &key) {
		uint32_t hashes[4];
		MurmurHash3_x64_128(&key, sizeof(Key), 1, hashes);
		array[hashes[0] % Bits] = true;
		array[hashes[1] % Bits] = true;
		array[hashes[2] % Bits] = true;
		array[hashes[3] % Bits] = true;
	}
	template <std::size_t Bits, typename Array> inline static bool Exist(const Array &array, const Key &key) {
		uint32_t hashes[4];
		MurmurHash3_x64_128(&key, sizeof(Key), 1, hashes);
		return array[hashes[0] % Bits] && array[hashes[1] % Bits] && array[hashes[2] % Bits] && array[hashes[3] % Bits];
	}
};

template <typename Key>
using StandardBloom = lsm::Bloom<Key, 10240 * 8, Murmur3BloomHasher<Key>>;

template <typename Key> struct StandardTrait : public lsm::KVDefaultTrait<Key, std::string> {
	using Compare = std::less<Key>;
	using Container = lsm::SkipList<Key, lsm::KVMemValue<std::string>, Compare, std::default_random_engine, 1, 2, 32>;
	using KeyFile = lsm::KVCachedBloomKeyFile<Key, StandardTrait, StandardBloom<Key>>;
	constexpr static lsm::size_type kMaxFileSize = 2 * 1024 * 1024;

	constexpr static lsm::KVLevelConfig kLevelConfigs[] = {{2, lsm::KVLevelType::kTiering},
	                                                       {4, lsm::KVLevelType::kLeveling},
	                                                       {8, lsm::KVLevelType::kLeveling},
	                                                       {16, lsm::KVLevelType::kLeveling},
	                                                       {32, lsm::KVLevelType::kLeveling}};
};

using StandardKV = lsm::KV<uint64_t, std::string, StandardTrait<uint64_t>>;

template <typename Func> inline double prof_sec(Func &&func) {
	auto begin = std::chrono::high_resolution_clock::now();
	func();
	auto end = std::chrono::high_resolution_clock::now();
	return std::chrono::duration<double>{end - begin}.count();
}
template <typename Func> inline double prof_ms(Func &&func) { return prof_sec(std::forward<Func>(func)) * 1000.0; }
template <typename Func> inline double prof_us(Func &&func) { return prof_sec(std::forward<Func>(func)) * 1000000.0; }
