#pragma once

#include <filesystem>

#include "kv_trait.hpp"
#include "lru_cache.hpp"

namespace lsm {

using KVStreamCache = LRUCache<std::filesystem::path, std::ifstream>;
template <typename Key, typename Value, typename Trait> class KVFileSystem {
private:
	constexpr static level_type kLevels = Trait::kLevels;

	mutable KVStreamCache m_stream_cache;
	std::filesystem::path m_directory;
	time_type m_time_stamp;

	inline void init_directory() {
		if (!std::filesystem::exists(m_directory))
			std::filesystem::create_directory(m_directory);
		for (level_type level = 0; level <= kLevels; ++level) {
			if (!std::filesystem::exists(GetLevelDirectory(level)))
				std::filesystem::create_directory(GetLevelDirectory(level));
		}
	}

public:
	inline KVFileSystem(std::filesystem::path directory, size_type stream_capacity)
	    : m_directory{std::move(directory)}, m_stream_cache{stream_capacity}, m_time_stamp{0} {
		init_directory();
	}

	template <typename Func> inline void ForEachFile(Func &&func) const {
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
					func(file.path(), level);
				}
			}
		}
	}

	inline void MaintainTimeStamp(time_type time_stamp) { m_time_stamp = std::max(time_stamp + 1, m_time_stamp); }
	inline void NextTimeStamp() { ++m_time_stamp; }

	inline std::filesystem::path GetLevelDirectory(level_type level) const {
		return m_directory / (std::string{"level-"} + std::to_string(level));
	}
	inline std::filesystem::path GetFilePath(level_type level) const {
		return GetLevelDirectory(level) / (std::to_string(m_time_stamp) + ".sst");
	}
	inline time_type GetTimeStamp() const { return m_time_stamp; }

	inline KVStreamCache &GetStreamCache() const { return m_stream_cache; }

	inline void Reset() {
		m_stream_cache.Clear();
		if (std::filesystem::exists(m_directory))
			std::filesystem::remove_all(m_directory);
		m_time_stamp = 0;
		init_directory();
	}
};

} // namespace lsm
