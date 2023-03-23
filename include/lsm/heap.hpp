#pragma once
#include <algorithm>
#include <vector>

#include "type.hpp"

namespace lsm {

template <typename T, typename Compare = std::less<T>> class MinHeap {
private:
	struct RevCompare {
		inline bool operator()(const T &l, const T &r) const { return Compare{}(r, l); }
	};
	std::vector<T> m_vec;

public:
	inline explicit MinHeap(size_type reserved_size) { m_vec.reserve(reserved_size); }
	inline explicit MinHeap(std::vector<T> &&vec) : m_vec{std::move(vec)} {
		std::make_heap(m_vec.begin(), m_vec.end(), RevCompare{});
	}
	inline size_type GetSize() const { return m_vec.size(); }
	inline bool IsEmpty() const { return m_vec.empty(); }
	inline const T &GetTop() const { return m_vec.front(); }
	inline void Pop() {
		std::pop_heap(m_vec.begin(), m_vec.end(), RevCompare{});
		m_vec.pop_back();
	}
	inline void Push(T &&val) {
		m_vec.push_back(std::move(val));
		std::push_heap(m_vec.begin(), m_vec.end(), RevCompare{});
	}
	inline void Push(const T &val) {
		m_vec.push_back(val);
		std::push_heap(m_vec.begin(), m_vec.end(), RevCompare{});
	}
};

} // namespace lsm
