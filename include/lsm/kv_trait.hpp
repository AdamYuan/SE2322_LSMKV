#pragma once

#include <optional>

#include "bloom.hpp"
#include "io.hpp"
#include "skiplist.hpp"
#include "type.hpp"

namespace lsm {

enum class KVLevelType { kTiering, kLeveling };
struct KVLevelConfig {
	size_type max_files;
	KVLevelType type;
};
template <typename Key, typename Value, typename CompareType = std::less<Key>> struct KVDefaultTrait {
	using Compare = CompareType;
	using SkipList = ::lsm::SkipList<Key, std::optional<Value>, Compare, std::default_random_engine, 1, 2, 64>;
	using Bloom = ::lsm::Bloom<Key, 10240 * 8>;
	using ValueIO = IO<Value>;
	constexpr static size_type kSingleFileSizeLimit = 2 * 1024 * 1024;

	constexpr static level_type kLevels = 5;
	constexpr static KVLevelConfig kLevelConfigs[] = {
	    {2, KVLevelType::kTiering},   {4, KVLevelType::kLeveling},  {8, KVLevelType::kLeveling},
	    {16, KVLevelType::kLeveling}, {32, KVLevelType::kLeveling},
	};
};

} // namespace lsm
