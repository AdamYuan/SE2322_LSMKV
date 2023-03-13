#pragma once

#include <cinttypes>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "bloom.hpp"
#include "skiplist.hpp"

#include "lsmkv_io.hpp"

namespace lsm {

using size_type = uint32_t;
using level_type = uint32_t;

enum class LevelType { kTiering, kLeveling };

struct LevelConfig {
	size_type max_files;
	LevelType type;
};

template <typename Key, typename Value, typename CompareType = std::less<Key>> struct DefaultTrait {
	using RandomGenerator = std::default_random_engine;
	using Compare = CompareType;
	using SkipList = ::lsm::SkipList<Key, std::optional<Value>, RandomGenerator, 1, 2, 64, Compare>;
	using Bloom = ::lsm::Bloom<Key, 10240 * 8, 3>;
	using ValueIO = DefaultIO<Value>;
	constexpr static size_type kSingleFileSizeLimit = 2 * 1024 * 1024;

	constexpr static level_type kLevels = 5;
	constexpr static LevelConfig kLevelConfigs[] = {
	    {2, LevelType::kTiering},   {4, LevelType::kLeveling},  {8, LevelType::kLeveling},
	    {16, LevelType::kLeveling}, {32, LevelType::kLeveling},
	};
};

template <typename Key, typename Value, typename Trait = DefaultTrait<Key, Value>> class KV {
	static_assert(std::is_integral_v<Key>);

private:
	using Compare = typename Trait::Compare;

#pragma pack(push, 1)
	struct KeyOffset {
		Key key;
		size_type d_offset;
	};
	struct Header {
		uint64_t time_stamp;
		size_type key_count;
		Key key_min, key_max;
	};
#pragma pack(pop)
	using KeyOffsetIO = DefaultIO<KeyOffset>;
	using HeaderIO = DefaultIO<Header>;
	using ValueIO = typename Trait::ValueIO;
	constexpr static size_type kSingleFileSizeLimit = Trait::kSingleFileSizeLimit;
	constexpr static size_type kFileInitialSize = Trait::Bloom::GetBytes() + HeaderIO::GetSize({});

	constexpr static level_type kLevels = Trait::kLevels;
	constexpr static LevelConfig *kLevelConfigs = Trait::kLevelConfigs;

	inline static size_type make_d_offset(size_type offset, bool deleted) {
		return (offset & 0x7fffffffu) | (deleted ? 0x80000000u : 0u);
	}

	inline static size_type d_offset_get_offset(size_type d_offset) { return d_offset & 0x7fffffffu; }

	inline static bool d_offset_is_deleted(size_type d_offset) { return d_offset >> 31u; }

	std::filesystem::path m_directory;

	uint64_t m_time_stamp;

	typename Trait::SkipList m_skiplist;
	size_type m_sorted_table_size;

	struct SortedKeyTable {
		Header header;
		typename Trait::Bloom bloom;
		std::unique_ptr<KeyOffset[]> keys;
	};
	std::vector<SortedKeyTable> m_key_tables[kLevels + 1];

	inline std::filesystem::path get_level_dir(level_type level) const {
		return m_directory / (std::string{"level-"} + std::to_string(level));
	}

	inline void init_directory() {
		if (!std::filesystem::exists(m_directory))
			std::filesystem::create_directory(m_directory);
		for (level_type level = 0; level <= kLevels; ++level) {
			if (!std::filesystem::exists(get_level_dir(level)))
				std::filesystem::create_directory(get_level_dir(level));
		}
	}

	inline std::filesystem::path get_file_path(uint64_t time_stap, level_type level) const {
		return get_level_dir(level) / (std::to_string(time_stap) + ".sst");
	}

	inline SortedKeyTable load_key_table(const std::filesystem::path &file_path) {
		SortedKeyTable table = {};

		// Load Header
		std::ifstream fin{file_path};
		table.header = HeaderIO::Read(fin);

		m_time_stamp = std::max(table.header.time_stamp + 1, m_time_stamp);

		table.keys = std::make_unique<KeyOffset[]>(table.header.key_count);

		fin.seekg(kFileInitialSize);
		for (size_type key_id = 0; key_id < table.header.key_count; ++key_id)
			table.keys[key_id] = KeyOffsetIO::Read(fin);

		return table;
	}

	inline SortedKeyTable store_mem_table() {
		SortedKeyTable table = {};
		table.header.key_count = m_skiplist.GetSize();
		table.header.time_stamp = m_time_stamp++;
		table.keys = std::make_unique<KeyOffset[]>(table.header.key_count);

		size_type key_id = 0, key_pos = kFileInitialSize,
		          value_pos = key_pos + table.header.key_count * KeyOffsetIO::GetSize({});
		std::ofstream fout{get_file_path(table.header.time_stamp, 0), std::ios::binary};

		// Write Content
		bool set_key_min = false;
		m_skiplist.ForEach([&table, &fout, &key_id, &key_pos, &value_pos,
		                    &set_key_min](const Key &key, const std::optional<Value> &value_opt) {
			if (!set_key_min) {
				set_key_min = true;
				table.header.key_min = key;
			}
			table.header.key_max = key;

			table.keys[key_id].key = key;
			table.keys[key_id].d_offset = make_d_offset(value_pos, !value_opt.has_value());

			fout.seekp(key_pos);
			KeyOffsetIO::Write(fout, table.keys[key_id]);
			key_pos += KeyOffsetIO::GetSize({});

			++key_id;

			if (value_opt.has_value()) {
				fout.seekp(value_pos);
				ValueIO::Write(fout, value_opt.value());
				value_pos += ValueIO::GetSize(value_opt.value());
			}
		});

		// Write Header
		fout.seekp(0);
		HeaderIO::Write(fout, table.header);

		return table;
	}

	inline void pop_mem_table() {
		m_key_tables[0].push_back(store_mem_table());
		m_skiplist.Clear();
		m_sorted_table_size = kFileInitialSize;
	}

	inline bool key_equal(const Key &l, const Key &r) const { return !Compare{}(l, r) && !Compare{}(r, l); }

public:
	inline explicit KV(std::string_view directory)
	    : m_directory{directory}, m_time_stamp{1}, m_skiplist{}, m_sorted_table_size{kFileInitialSize} {
		init_directory();
		for (const auto &level_dir : std::filesystem::directory_iterator(m_directory)) {
			if (!level_dir.is_directory())
				continue;
			auto level_dir_name = level_dir.path().filename().string();
			if (level_dir_name.size() > 6 && level_dir_name.substr(0, 6) == "level-") {
				level_type level = std::stoull(level_dir_name.substr(6));

				for (const auto &file : std::filesystem::directory_iterator(level_dir)) {
					if (!file.is_regular_file())
						continue;
					if (!file.path().has_extension() || file.path().extension() != ".sst")
						continue;
					m_key_tables[level].push_back(load_key_table(file));
				}
			}
		}
	}

	inline ~KV() {
		if (m_skiplist.GetSize())
			store_mem_table();
	}

	inline void Put(Key key, Value &&value) {
		if (m_skiplist.Replace(key, [this, &value](std::optional<Value> *p_opt_value, bool exists) -> bool {
			    size_type new_size = m_sorted_table_size;
			    if (exists) {
				    new_size -= p_opt_value->has_value() ? ValueIO::GetSize(p_opt_value->value()) : 0;
				    new_size += ValueIO::GetSize(value);
			    } else
				    new_size += KeyOffsetIO::GetSize({}) + ValueIO::GetSize(value);

			    if (new_size > kSingleFileSizeLimit)
				    return false;

			    *p_opt_value = std::move(value);
			    m_sorted_table_size = new_size;
			    return true;
		    }))
			return;

		pop_mem_table();
		m_skiplist.Insert(key, std::optional<Value>{std::move(value)});
		m_sorted_table_size += KeyOffsetIO::GetSize({}) + ValueIO::GetSize(value);
	}

	inline void Put(Key key, const Value &value) { Put(key, Value{value}); }

	inline std::optional<Value> Get(Key key) const {
		auto opt_opt_value = m_skiplist.Search(key);
		if (opt_opt_value.has_value())
			return opt_opt_value.value();

		uint64_t target_time_stap = 0u;
		level_type target_level;
		size_type target_d_offset, target_length;

		for (level_type level = 0; level <= kLevels; ++level) {
			for (const SortedKeyTable &table : m_key_tables[level]) {
				const Header &header = table.header;

				if (Compare{}(key, header.key_min) || Compare{}(header.key_max, key) ||
				    header.time_stamp <= target_time_stap)
					continue;

				// Binary Search for the Key
				const KeyOffset *key_begin = table.keys.get(), *key_end = table.keys.get() + header.key_count, *key_it;
				{ // lower bound
					const KeyOffset *first = key_begin;
					size_type count = header.key_count, step;
					while (count > 0) {
						step = count >> 1u;
						key_it = first + step;
						if (Compare{}(key_it->key, key)) {
							first = ++key_it;
							count -= step + 1;
						} else
							count = step;
					}
				}
				if (key_it != key_end && key_equal(key_it->key, key)) {
					target_time_stap = header.time_stamp;
					target_level = level;
					target_d_offset = key_it->d_offset;

					++key_it;
					target_length = key_it == key_end ? -1u : d_offset_get_offset(key_it->d_offset) - target_d_offset;
				}
			}
		}
		if (target_time_stap == 0 || d_offset_is_deleted(target_d_offset))
			return std::nullopt;

		std::filesystem::path file_path = get_file_path(target_time_stap, target_level);
		if (target_length == -1) {
			target_length = std::filesystem::file_size(file_path) - target_d_offset;
		}
		std::ifstream fin{file_path, std::ios::binary};
		fin.seekg(target_d_offset);
		return ValueIO::Read(fin, target_length);
	}

	inline bool Delete(Key key) {
		if (!Get(key).has_value())
			return false;

		if (m_skiplist.Replace(key, [this](std::optional<Value> *p_opt_value, bool exists) -> bool {
			    size_type new_size = m_sorted_table_size;
			    if (exists)
				    new_size -= p_opt_value->has_value() ? ValueIO::GetSize(p_opt_value->value()) : 0;
			    else
				    new_size += KeyOffsetIO::GetSize({});

			    if (new_size > kSingleFileSizeLimit)
				    return false;

			    *p_opt_value = std::nullopt;
			    m_sorted_table_size = new_size;
			    return true;
		    }))
			return true;

		pop_mem_table();
		m_skiplist.Insert(key, std::nullopt);
		m_sorted_table_size += KeyOffsetIO::GetSize({});
		return true;
	}

	inline void Reset() {
		m_skiplist.Clear();
		m_sorted_table_size = kFileInitialSize;

		m_time_stamp = 1;
		for (level_type level = 0; level <= kLevels; ++level)
			m_key_tables[level].clear();
		if (std::filesystem::exists(m_directory))
			std::filesystem::remove_all(m_directory);
		init_directory();
	}
};

} // namespace lsm
