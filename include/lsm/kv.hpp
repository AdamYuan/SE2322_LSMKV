#pragma once

#include "detail/kv.hpp"
#include "kv_trait.hpp"

namespace lsm {

template <typename Key, typename Value, typename Trait = KVDefaultTrait<Key, Value>>
using KV = detail::KV<Key, Value, Trait>;

}