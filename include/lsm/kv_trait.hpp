#pragma once

#include <optional>

#include "bloom.hpp"
#include "detail/io.hpp"
#include "skiplist.hpp"
#include "type.hpp"

namespace lsm {

template <typename Type> using IO = detail::IO<Type>;
template <typename Key, typename Trait, typename Bloom> using KVKeyFile = detail::KVKeyFile<Key, Trait, Bloom>;

template <typename Key, typename Value, typename CompareType = std::less<Key>> struct KVDefaultTrait {
	using Compare = CompareType;
	using SkipList = lsm::SkipList<Key, KVSkipListValue<Value>, Compare, std::default_random_engine, 1, 2, 64>;
	using KeyFile = lsm::KVKeyFile<Key, KVDefaultTrait, Bloom<Key, 10240 * 8>>;
	using ValueIO = IO<Value>;
	constexpr static size_type kSingleFileSizeLimit = 2 * 1024 * 1024;

	constexpr static level_type kLevels = 5;
	constexpr static KVLevelConfig kLevelConfigs[] = {
	    {2, KVLevelType::kTiering},   {4, KVLevelType::kLeveling},  {8, KVLevelType::kLeveling},
	    {16, KVLevelType::kLeveling}, {32, KVLevelType::kLeveling},
	};
};

} // namespace lsm
