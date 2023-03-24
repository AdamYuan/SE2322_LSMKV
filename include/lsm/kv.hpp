#pragma once

#include <filesystem>
#include <type_traits>

#include "kv_mem.hpp"
#include "kv_merge.hpp"
#include "kv_table.hpp"
#include "kv_trait.hpp"

namespace lsm {

template <typename Key, typename Value, typename Trait = KVDefaultTrait<Key, Value>> class KV {
	static_assert(std::is_integral_v<Key>);

private:
	constexpr static level_type kLevels = Trait::kLevels;
	constexpr static const KVLevelConfig *kLevelConfigs = Trait::kLevelConfigs;

	using FileTable = KVFileTable<Key, Value, Trait>;
	using BufferTable = KVBufferTable<Key, Value, Trait>;
	using MemSkipList = KVMemSkipList<Key, Value, Trait>;
	using Compare = typename Trait::Compare;

	std::filesystem::path m_directory;

	MemSkipList m_mem_skiplist;
	std::vector<FileTable> m_levels[kLevels + 1];
	time_type m_level_time_stamps[kLevels + 1]{};

	inline std::filesystem::path get_level_dir(level_type level) const {
		return m_directory / (std::string{"level-"} + std::to_string(level));
	}
	inline std::filesystem::path get_file_path(uint64_t time_stap, level_type level) const {
		return get_level_dir(level) / (std::to_string(time_stap) + ".sst");
	}

	inline void init_directory() {
		if (!std::filesystem::exists(m_directory))
			std::filesystem::create_directory(m_directory);
		for (level_type level = 0; level <= kLevels; ++level) {
			if (!std::filesystem::exists(get_level_dir(level)))
				std::filesystem::create_directory(get_level_dir(level));
		}
	}
	inline void init_time_stamps() {
		constexpr time_type kUnit = std::numeric_limits<time_type>::max() / (kLevels + 1);
		for (level_type l = 0; l <= kLevels; ++l)
			m_level_time_stamps[l] = kUnit * (kLevels - l);
	}
	template <level_type Level> void compaction(std::vector<BufferTable> &&src_buffer_tables) {
		auto &level_vec = m_levels[Level];

		if constexpr (Level == kLevels) {
			for (auto &buffer_table : src_buffer_tables)
				level_vec.push_back(
				    FileTable{get_file_path(buffer_table.GetTimeStamp(), Level), std::move(buffer_table)});
			return;
		} else {
			if (level_vec.size() + src_buffer_tables.size() <= kLevelConfigs[Level].max_files) {
				for (auto &buffer_table : src_buffer_tables)
					level_vec.push_back(
					    FileTable{get_file_path(buffer_table.GetTimeStamp(), Level), std::move(buffer_table)});
				return;
			}

			std::vector<FileTable> src_file_tables;
			if constexpr (kLevelConfigs[Level].type == KVLevelType::kTiering) {
				for (auto &table : level_vec)
					src_file_tables.push_back(std::move(table));
				level_vec.clear();
			} else { // Leveling
				auto src_buffer_crop_it = src_buffer_tables.end() - (kLevelConfigs[Level].max_files - level_vec.size());
				for (auto it = src_buffer_crop_it; it != src_buffer_tables.end(); ++it) {
					auto &buffer_table = *it;
					level_vec.push_back(
					    FileTable{get_file_path(buffer_table.GetTimeStamp(), Level), std::move(buffer_table)});
				}
				src_buffer_tables.erase(src_buffer_crop_it, src_buffer_tables.end());

				while (level_vec.size() > kLevelConfigs[Level].max_files) {
					auto &table = level_vec.back();
					src_file_tables.push_back(std::move(table));
					level_vec.pop_back();
				}
			}
			// Find Overlapped Tables in Next Level
			if constexpr (Level + 1 == kLevels || kLevelConfigs[Level + 1].type == KVLevelType::kLeveling) {
				size_type src_file_table_size = src_file_tables.size();
				auto &next_level_vec = m_levels[Level + 1];
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

			KVMerger<Key, Value, Trait> merger{std::move(src_file_tables), std::move(src_buffer_tables),
			                                   m_level_time_stamps[Level + 1]};
			auto dst_buffer_tables = merger.template Run<Level + 1 == kLevels>();
			m_level_time_stamps[Level + 1] += dst_buffer_tables.size();

			compaction<Level + 1>(std::move(dst_buffer_tables));

			for (const auto &file_path : deleted_file_paths)
				std::filesystem::remove(file_path);
		}
	}
	inline void store_buffer_table(BufferTable &&buffer_table) {
		std::vector<BufferTable> buffer_tables;
		buffer_tables.push_back(std::move(buffer_table));
		compaction<0>(std::move(buffer_tables));
		/* for (auto &level_vec : m_levels)
		    if (!std::is_sorted(level_vec.begin(), level_vec.end(),
		                        [](const auto &l, const auto &r) { return l.GetTimeStamp() < r.GetTimeStamp(); })) {
		        printf("Not Sorted\n");
		    }*/
	}

public:
	inline explicit KV(std::string_view directory) : m_directory{directory} {
		init_time_stamps();
		init_directory();
		for (const auto &level_dir : std::filesystem::directory_iterator(m_directory)) {
			if (!level_dir.is_directory())
				continue;
			auto level_dir_name = level_dir.path().filename().string();
			if (level_dir_name.size() > 6 && level_dir_name.substr(0, 6) == "level-") {
				level_type level = std::stoull(level_dir_name.substr(6));
				if (level > kLevels)
					continue;
				for (const auto &file : std::filesystem::directory_iterator(level_dir)) {
					if (!file.is_regular_file())
						continue;
					if (!file.path().has_extension() || file.path().extension() != ".sst")
						continue;
					auto file_table = FileTable{file.path()};
					m_level_time_stamps[level] = std::max(m_level_time_stamps[level], file_table.GetTimeStamp() + 1);
					m_levels[level].push_back(std::move(file_table));
				}
			}
		}
		for (auto &level_vec : m_levels)
			std::sort(level_vec.begin(), level_vec.end(),
			          [](const auto &l, const auto &r) { return l.GetTimeStamp() < r.GetTimeStamp(); });
	}

	inline ~KV() {
		if (!m_mem_skiplist.IsEmpty())
			FileTable{get_file_path(m_level_time_stamps[0], 0), m_mem_skiplist.PopBuffer(m_level_time_stamps[0])};
	}

	inline void Put(Key key, Value &&value) {
		std::optional<BufferTable> opt_buffer_table = m_mem_skiplist.Put(key, std::move(value), m_level_time_stamps[0]);
		if (opt_buffer_table.has_value()) {
			++m_level_time_stamps[0];
			store_buffer_table(std::move(opt_buffer_table.value()));
		}
	}

	inline void Put(Key key, const Value &value) { Put(key, Value{value}); }

	inline std::optional<Value> Get(Key key) const {
		auto opt_opt_value = m_mem_skiplist.Get(key);
		if (opt_opt_value.has_value())
			return opt_opt_value.value();

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
		m_mem_skiplist.Scan(min_key, max_key, [&iterator_heap, &func](Key key, const std::optional<Value> &opt_value) {
			while (!iterator_heap.IsEmpty() && Compare{}(iterator_heap.GetTop().GetKey(), key)) {
				const auto &it = iterator_heap.GetTop();
				if (!it.IsKeyDeleted())
					func(it.GetKey(), it.ReadValue());
				iterator_heap.Proceed();
			}
			if (!iterator_heap.IsEmpty() && !Compare{}(key, iterator_heap.GetTop().GetKey()))
				iterator_heap.Proceed();
			if (opt_value.has_value())
				func(key, opt_value.value());
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
			if (!opt_opt_value.value().has_value())
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
		}
	End_Check:
		std::optional<BufferTable> opt_buffer_table = m_mem_skiplist.Delete(key, m_level_time_stamps[0]);
		if (opt_buffer_table.has_value()) {
			++m_level_time_stamps[0];
			store_buffer_table(std::move(opt_buffer_table.value()));
		}
		return true;
	}

	inline void Reset() {
		m_mem_skiplist.Reset();

		for (level_type level = 0; level <= kLevels; ++level)
			m_levels[level].clear();

		if (std::filesystem::exists(m_directory))
			std::filesystem::remove_all(m_directory);

		init_directory();
		init_time_stamps();
	}
};

} // namespace lsm
