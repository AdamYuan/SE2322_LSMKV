#pragma once

#include "kv_mem.hpp"
#include "kv_table.hpp"

#include <algorithm>
#include <vector>

namespace lsm {

template <typename Key, typename Value, typename Trait, level_type Level> class KVMerger {
private:
	using FileSystem = KVFileSystem<Key, Value, Trait>;

	using KeyCompare = typename Trait::Compare;

	using FileTable = KVFileTable<Key, Value, Trait>;
	using BufferTable = KVBufferTable<Key, Value, Trait>;

	FileSystem *m_p_file_system;

	std::vector<FileTable> m_file_tables;
	std::vector<BufferTable> m_buffer_tables;
	KVTableIteratorHeap<typename FileTable::Iterator> m_file_it_heap;
	KVTableIteratorHeap<typename BufferTable::Iterator> m_buffer_it_heap;

	KVMemAppender<Key, Value, Trait> m_mem_appender;

	std::vector<BufferTable> m_result_tables;

	size_type m_remain_file_count{};

	template <bool Delete, typename Iterator, typename PostFileTableFunc>
	inline void push_iterator(const Iterator &it, PostFileTableFunc &&post_file_table_func) {
		if (m_remain_file_count == 0) {
			std::optional<BufferTable> opt_buffer = m_mem_appender.template Append<Delete>(it);
			if (opt_buffer.has_value())
				m_result_tables.push_back(std::move(opt_buffer.value()));
		} else {
			std::optional<FileTable> opt_file = m_mem_appender.template Append<Delete>(it, m_p_file_system, Level);
			if (opt_file.has_value()) {
				post_file_table_func(std::move(opt_file.value()));
				--m_remain_file_count;
			}
		}
	}

public:
	inline KVMerger(std::vector<FileTable> &&file_tables, std::vector<BufferTable> &&buffer_tables,
	                FileSystem *p_file_system)
	    : m_p_file_system{p_file_system}, m_file_tables{std::move(file_tables)}, m_buffer_tables{
	                                                                                 std::move(buffer_tables)} {

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
	template <typename PostFileTableFunc>
	inline std::vector<BufferTable> Run(size_type file_count, PostFileTableFunc &&post_file_table_func) {
		static constexpr bool kDelete = Level == Trait::kLevels;

		m_remain_file_count = file_count;

		while (!m_file_it_heap.IsEmpty() && !m_buffer_it_heap.IsEmpty()) {
			auto file_it = m_file_it_heap.GetTop();
			auto buffer_it = m_buffer_it_heap.GetTop();
			if (KeyCompare{}(file_it.GetKey(), buffer_it.GetKey())) {
				push_iterator<kDelete>(file_it, post_file_table_func);
				m_file_it_heap.Proceed();
			} else if (KeyCompare{}(buffer_it.GetKey(), file_it.GetKey())) {
				push_iterator<kDelete>(buffer_it, post_file_table_func);
				m_buffer_it_heap.Proceed();
			} else {
				push_iterator<kDelete>(buffer_it, post_file_table_func);
				m_file_it_heap.Proceed();
				m_buffer_it_heap.Proceed();
			}
		}
		while (!m_file_it_heap.IsEmpty()) {
			push_iterator<kDelete>(m_file_it_heap.GetTop(), post_file_table_func);
			m_file_it_heap.Proceed();
		}
		while (!m_buffer_it_heap.IsEmpty()) {
			push_iterator<kDelete>(m_buffer_it_heap.GetTop(), post_file_table_func);
			m_buffer_it_heap.Proceed();
		}

		if (!m_mem_appender.IsEmpty()) {
			if (m_remain_file_count == 0)
				m_result_tables.push_back(m_mem_appender.PopBuffer());
			else
				post_file_table_func(m_mem_appender.PopFile(m_p_file_system, Level));
		}

		return std::move(m_result_tables);
	}
};

} // namespace lsm
