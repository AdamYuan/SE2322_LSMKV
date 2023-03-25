#pragma once

#include "kv_mem.hpp"
#include "kv_table.hpp"

#include <algorithm>
#include <vector>

namespace lsm {

template <typename Key, typename Value, typename Trait> class KVMerger {
private:
	using KeyCompare = typename Trait::Compare;

	using FileTable = KVFileTable<Key, Value, Trait>;
	using BufferTable = KVBufferTable<Key, Value, Trait>;

	time_type m_time_stamp;

	std::vector<FileTable> m_file_tables;
	std::vector<BufferTable> m_buffer_tables;
	KVTableIteratorHeap<typename FileTable::Iterator> m_file_it_heap;
	KVTableIteratorHeap<typename BufferTable ::Iterator> m_buffer_it_heap;

	KVMemAppender<Key, Value, Trait> m_mem_appender;

	std::vector<BufferTable> m_result_tables;

	template <bool Delete, typename Iterator> inline void push_iterator(const Iterator &it) {
		std::optional<BufferTable> opt_buffer = m_mem_appender.template Append<Delete>(it, m_time_stamp);
		if (opt_buffer.has_value()) {
			++m_time_stamp;
			m_result_tables.push_back(std::move(opt_buffer.value()));
		}
	}

public:
	inline KVMerger(std::vector<FileTable> &&file_tables, std::vector<BufferTable> &&buffer_tables,
	                time_type time_stamp)
	    : m_time_stamp{time_stamp}, m_file_tables{std::move(file_tables)}, m_buffer_tables{std::move(buffer_tables)} {

		m_result_tables.reserve(m_file_tables.size() + m_buffer_tables.size());

		std::vector<typename FileTable::Iterator> file_it_vec;
		file_it_vec.reserve(file_it_vec.size());
		for (const auto &table : m_file_tables)
			file_it_vec.push_back(table.GetBegin());
		m_file_it_heap = KVTableIteratorHeap<typename FileTable::Iterator>{std::move(file_it_vec)};

		std::vector<typename BufferTable::Iterator> buffer_it_vec;
		buffer_it_vec.reserve(buffer_it_vec.size());
		for (const auto &table : m_buffer_tables)
			buffer_it_vec.push_back(table.GetBegin());
		m_buffer_it_heap = KVTableIteratorHeap<typename BufferTable::Iterator>{std::move(buffer_it_vec)};
	}
	template <bool Delete> inline std::vector<BufferTable> Run() {
		while (!m_file_it_heap.IsEmpty() && !m_buffer_it_heap.IsEmpty()) {
			auto file_it = m_file_it_heap.GetTop();
			auto buffer_it = m_buffer_it_heap.GetTop();
			if (KeyCompare{}(file_it.GetKey(), buffer_it.GetKey())) {
				push_iterator<Delete>(file_it);
				m_file_it_heap.Proceed();
			} else if (KeyCompare{}(buffer_it.GetKey(), file_it.GetKey())) {
				push_iterator<Delete>(buffer_it);
				m_buffer_it_heap.Proceed();
			} else {
				if (file_it.GetTimeStamp() > buffer_it.GetTimeStamp())
					push_iterator<Delete>(file_it);
				else
					push_iterator<Delete>(buffer_it);
				m_file_it_heap.Proceed();
				m_buffer_it_heap.Proceed();
			}
		}
		while (!m_file_it_heap.IsEmpty()) {
			push_iterator<Delete>(m_file_it_heap.GetTop());
			m_file_it_heap.Proceed();
		}
		while (!m_buffer_it_heap.IsEmpty()) {
			push_iterator<Delete>(m_buffer_it_heap.GetTop());
			m_buffer_it_heap.Proceed();
		}

		if (!m_mem_appender.IsEmpty())
			m_result_tables.push_back(m_mem_appender.PopBuffer(m_time_stamp));

		return std::move(m_result_tables);
	}
};

} // namespace lsm
