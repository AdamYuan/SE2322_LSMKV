#pragma once

#include "../type.hpp"

#include <cstddef>
#include <list>
#include <stdexcept>
#include <unordered_map>

namespace lsm::detail {

template <typename Key, typename Value, typename KeyHasher = std::hash<Key>> class LRUCache {
private:
	using KVPair = std::pair<Key, Value>;
	using Iterator = typename std::list<KVPair>::iterator;

	std::list<KVPair> m_list;
	std::unordered_map<Key, Iterator, KeyHasher> m_map;
	size_type m_capacity;

public:
	inline explicit LRUCache(size_type capacity) : m_capacity(capacity) {}

	template <typename Creator> inline Value &Push(Key &&key, Creator &&creator) {
		auto it = m_map.find(key);
		if (it == m_map.end()) {
			m_list.emplace_front(key, creator(key));
			m_map.insert({std::move(key), m_list.begin()});
			if (m_map.size() > m_capacity) {
				auto last = m_list.end();
				m_map.erase((--last)->first);
				m_list.pop_back();
			}
			return m_list.front().second;
		} else {
			m_list.splice(m_list.begin(), m_list, it->second);
			return it->second->second;
		}
	}
	template <typename Creator> inline Value &Push(const Key &key, Creator &&creator) {
		return Push(Key(key), creator);
	}
	inline void Clear() {
		m_map.clear();
		m_list.clear();
	}
};

} // namespace lsm::detail
