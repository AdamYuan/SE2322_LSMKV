#pragma once

#include <filesystem>
#include <type_traits>

#include "kv_mem.hpp"
#include "kv_merge.hpp"
#include "kv_table.hpp"

namespace lsm {

enum class KVLevelType { kTiering, kLeveling };
struct KVLevelConfig {
	size_type max_files;
	KVLevelType type;
};

} // namespace lsm

namespace lsm::detail {

template <typename Key, typename Value, typename Trait> class KV {
	static_assert(std::is_integral_v<Key>);

private:
	constexpr static level_type kLevels = Trait::kLevels;
	constexpr static const KVLevelConfig *kLevelConfigs = Trait::kLevelConfigs;

	static_assert(kLevels == 0 || kLevelConfigs[0].type == KVLevelType::kTiering);

	using FileSystem = KVFileSystem<Trait>;

	using FileTable = KVFileTable<Key, Value, Trait>;
	using BufferTable = KVBufferTable<Key, Value, Trait>;
	using MemSkipList = KVMemSkipList<Key, Value, Trait>;
	using Compare = typename Trait::Compare;

	MemSkipList m_mem_skiplist;
	std::vector<FileTable> m_levels[kLevels + 1];

	FileSystem m_file_system;

	template <level_type Level> void compaction(std::vector<BufferTable> &&src_buffer_tables) {
		auto &level_vec = m_levels[Level];

		if constexpr (Level < kLevels) {
			if (src_buffer_tables.empty())
				return;

			std::vector<FileTable> src_file_tables;
			if constexpr (kLevelConfigs[Level].type == KVLevelType::kTiering) {
				for (auto &table : level_vec)
					src_file_tables.push_back(std::move(table));
				level_vec.clear();
			} else { // Leveling
				while (level_vec.size() > kLevelConfigs[Level].max_files) {
					auto &table = level_vec.back();
					src_file_tables.push_back(std::move(table));
					level_vec.pop_back();
				}
			}

			auto &next_level_vec = m_levels[Level + 1];

			// Find Overlapped Tables in Next Level
			if constexpr (Level + 1 == kLevels || kLevelConfigs[Level + 1].type == KVLevelType::kLeveling) {
				size_type src_file_table_size = src_file_tables.size();
				next_level_vec.erase(
				    std::remove_if(next_level_vec.begin(), next_level_vec.end(),
				                   [this, &src_file_tables, &src_buffer_tables, src_file_table_size](auto &table) {
					                   const auto check_overlap = [&table](const auto &src_table) {
						                   return table.IsOverlap(src_table);
					                   };
					                   if (std::none_of(src_file_tables.begin(),
					                                    src_file_tables.begin() + src_file_table_size, check_overlap) &&
					                       std::none_of(src_buffer_tables.begin(), src_buffer_tables.end(),
					                                    check_overlap))
						                   return false;
					                   src_file_tables.push_back(std::move(table));
					                   return true;
				                   }),
				    next_level_vec.end());
			}

			std::vector<std::filesystem::path> deleted_file_paths;
			deleted_file_paths.reserve(src_file_tables.size());
			for (const auto &table : src_file_tables)
				deleted_file_paths.push_back(table.GetFilePath());

			size_type max_append_files = 0;
			if constexpr (Level + 1 == kLevels)
				max_append_files = std::numeric_limits<size_type>::max();
			else if constexpr (kLevelConfigs[Level + 1].type == KVLevelType::kLeveling)
				max_append_files = std::max(kLevelConfigs[Level + 1].max_files, (size_type)next_level_vec.size()) -
				                   (size_type)next_level_vec.size();

			std::vector<BufferTable> dst_buffer_tables =
			    KVMerger<Key, Value, Trait, Level + 1>{std::move(src_file_tables), std::move(src_buffer_tables),
			                                           &m_file_system}
			        .Run(max_append_files,
			             [this](FileTable &&file_table) { m_levels[Level + 1].push_back(std::move(file_table)); });

			compaction<Level + 1>(std::move(dst_buffer_tables));

			for (const auto &file_path : deleted_file_paths)
				std::filesystem::remove(file_path);
		}
	}
	inline void compaction_0(BufferTable &&buffer_table) {
		std::vector<BufferTable> table_table_vec;
		table_table_vec.push_back(std::move(buffer_table));
		compaction<0>(std::move(table_table_vec));
	}

	inline bool is_level_0_full() const {
		if constexpr (kLevels > 0)
			return m_levels[0].size() >= kLevelConfigs[0].max_files;
		else
			return false;
	}

public:
	inline explicit KV(std::string_view directory, size_type stream_capacity = 32)
	    : m_file_system{directory, stream_capacity} {
		m_file_system.ForEachFile([this](const std::filesystem::path &file_path, level_type level) {
			auto file_table = FileTable{&m_file_system, file_path, level};
			m_levels[level].push_back(std::move(file_table));
		});
	}

	inline ~KV() {
		if (!m_mem_skiplist.IsEmpty()) {
			if (is_level_0_full())
				compaction_0(m_mem_skiplist.PopBuffer());
			else
				m_mem_skiplist.PopFile(&m_file_system, 0);
		}
	}

	inline void Put(Key key, Value &&value) {
		if (is_level_0_full()) {
			std::optional<BufferTable> opt_buffer_table = m_mem_skiplist.Put(key, std::move(value));
			if (opt_buffer_table.has_value())
				compaction_0(std::move(opt_buffer_table.value()));
		} else {
			std::optional<FileTable> opt_file_table = m_mem_skiplist.Put(key, std::move(value), &m_file_system, 0);
			if (opt_file_table.has_value())
				m_levels[0].push_back(std::move(opt_file_table.value()));
		}
	}

	inline void Put(Key key, const Value &value) { Put(key, Value{value}); }

	inline std::optional<Value> Get(Key key) const {
		auto opt_sl_value = m_mem_skiplist.Get(key);
		if (opt_sl_value.has_value())
			return opt_sl_value.value().GetOptValue();

		for (const auto &level_vec : m_levels) {
			for (size_type i = level_vec.size() - 1; ~i; --i) {
				auto it = level_vec[i].Find(key);
				if (!it.IsValid())
					continue;
				if (it.IsKeyDeleted())
					return std::nullopt;
				return it.ReadValue();
			}
		}
		return std::nullopt;
	}

	template <typename Func> inline void Scan(Key min_key, Key max_key, Func &&func) const {
		KVTableIteratorHeap<typename FileTable::Iterator> iterator_heap;
		{
			std::vector<typename FileTable::Iterator> iterators;
			for (const auto &level_vec : m_levels)
				for (const FileTable &table : level_vec)
					if (table.IsOverlap(min_key, max_key))
						iterators.push_back(table.GetLowerBound(min_key));
			iterator_heap = KVTableIteratorHeap<typename FileTable::Iterator>{std::move(iterators)};
		}
		m_mem_skiplist.Scan(min_key, max_key, [&iterator_heap, &func](Key key, const KVMemValue<Value> &sl_value) {
			while (!iterator_heap.IsEmpty() && Compare{}(iterator_heap.GetTop().GetKey(), key)) {
				const auto &it = iterator_heap.GetTop();
				if (!it.IsKeyDeleted())
					func(it.GetKey(), it.ReadValue());
				iterator_heap.Proceed();
			}
			if (!iterator_heap.IsEmpty() && !Compare{}(key, iterator_heap.GetTop().GetKey()))
				iterator_heap.Proceed();
			if (!sl_value.IsDeleted())
				func(key, sl_value.GetValue());
		});
		while (!iterator_heap.IsEmpty() && !Compare{}(max_key, iterator_heap.GetTop().GetKey())) {
			const auto &it = iterator_heap.GetTop();
			if (!it.IsKeyDeleted())
				func(it.GetKey(), it.ReadValue());
			iterator_heap.Proceed();
		}
	}

	inline bool Delete(Key key) {
		// Check whether the key is already deleted
		auto opt_opt_value = m_mem_skiplist.Get(key);
		if (opt_opt_value.has_value()) {
			if (opt_opt_value.value().IsDeleted())
				return false;
		} else {
			for (const auto &level_vec : m_levels) {
				for (size_type i = level_vec.size() - 1; ~i; --i) {
					auto it = level_vec[i].Find(key);
					if (!it.IsValid())
						continue;
					if (it.IsKeyDeleted())
						return false;
					goto End_Check;
				}
			}
			return false;
		}
	End_Check:
		if (is_level_0_full()) {
			std::optional<BufferTable> opt_buffer_table = m_mem_skiplist.Delete(key);
			if (opt_buffer_table.has_value())
				compaction_0(std::move(opt_buffer_table.value()));
		} else {
			std::optional<FileTable> opt_file_table = m_mem_skiplist.Delete(key, &m_file_system, 0);
			if (opt_file_table.has_value())
				m_levels[0].push_back(std::move(opt_file_table.value()));
		}
		return true;
	}

	inline void Reset() {
		m_mem_skiplist.Reset();
		for (auto &level_vec : m_levels)
			level_vec.clear();
		m_file_system.Reset();
	}
};

} // namespace lsm::detail
