#pragma once

#include <cinttypes>
#include <filesystem>
#include <fstream>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "kv_mem.hpp"
#include "kv_table.hpp"
#include "kv_trait.hpp"

namespace lsm {

template <typename Key, typename Value, typename Trait = KVDefaultTrait<Key, Value>> class KV {
	static_assert(std::is_integral_v<Key>);

private:
	constexpr static auto kLevels = Trait::kLevels;

	using Compare = typename Trait::Compare;
	using KeyOffset = KVKeyOffset<Key>;
	using FileTable = KVFileTable<Key, Value, Trait>;
	using BufferTable = KVBufferTable<Key, Value, Trait>;
	using Mem = KVMem<Key, Value, Trait>;

	std::filesystem::path m_directory;
	time_type m_time_stamp;

	Mem m_mem;
	std::list<FileTable> m_file_tables;
	std::vector<typename std::list<FileTable>::iterator> m_levels[kLevels + 1];

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

	inline static bool key_equal(const Key &l, const Key &r) { return !Compare{}(l, r) && !Compare{}(r, l); }

public:
	inline explicit KV(std::string_view directory) : m_directory{directory}, m_time_stamp{1} {
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
					auto file_table = FileTable{file.path()};
					m_time_stamp = std::max(m_time_stamp, file_table.GetTimeStamp() + 1);
					m_file_tables.push_front(std::move(file_table));
					m_levels[level].push_back(m_file_tables.begin());
				}
			}
		}
		m_file_tables.sort([](const FileTable &l, const FileTable &r) { return l.GetTimeStamp() > r.GetTimeStamp(); });
	}

	inline ~KV() {
		if (!m_mem.IsEmpty())
			FileTable{get_file_path(m_time_stamp, 0), m_mem.PopBuffer(m_time_stamp)};
	}

	inline void Put(Key key, Value &&value) {
		std::optional<BufferTable> opt_buffer_table = m_mem.Put(key, std::move(value), m_time_stamp);
		if (opt_buffer_table.has_value()) {
			auto file_table = FileTable{get_file_path(m_time_stamp, 0), std::move(opt_buffer_table.value())};
			m_file_tables.push_front(std::move(file_table));
			m_levels[0].push_back(m_file_tables.begin());
			++m_time_stamp;
		}
	}

	inline void Put(Key key, const Value &value) { Put(key, Value{value}); }

	inline std::optional<Value> Get(Key key) const {
		auto opt_opt_value = m_mem.Get(key);
		if (opt_opt_value.has_value())
			return opt_opt_value.value();

		for (const FileTable &table : m_file_tables) {
			const KeyOffset *key_it = table.GetKeys().Find(key);
			if (key_it == nullptr)
				continue;

			if (key_it->IsDeleted())
				return std::nullopt;
			const KeyOffset *key_nxt = key_it + 1;
			return table.GetValues().Read(key_it->GetOffset(),
			                              key_nxt == table.GetKeys().GetEnd() ? -1 : key_nxt->GetOffset());
		}
		return std::nullopt;
	}

	inline bool Delete(Key key) {
		// Check whether the key is already deleted
		auto opt_opt_value = m_mem.Get(key);
		if (opt_opt_value.has_value()) {
			if (!opt_opt_value.value().has_value())
				return false;
		} else {
			for (const FileTable &table : m_file_tables) {
				const KeyOffset *key_it = table.GetKeys().Find(key);
				if (key_it == nullptr)
					continue;
				if (key_it->IsDeleted())
					return false;
				break;
			}
		}

		std::optional<BufferTable> opt_buffer_table = m_mem.Delete(key, m_time_stamp);
		if (opt_buffer_table.has_value()) {
			auto file_table = FileTable{get_file_path(m_time_stamp, 0), std::move(opt_buffer_table.value())};
			m_file_tables.push_front(std::move(file_table));
			m_levels[0].push_back(m_file_tables.begin());
			++m_time_stamp;
		}
		return true;
	}

	inline void Reset() {
		m_mem.Reset();

		m_time_stamp = 1;
		for (level_type level = 0; level <= kLevels; ++level)
			m_levels[level].clear();
		m_file_tables.clear();

		if (std::filesystem::exists(m_directory))
			std::filesystem::remove_all(m_directory);
		init_directory();
	}
};

} // namespace lsm
