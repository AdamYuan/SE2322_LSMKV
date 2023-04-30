#pragma once

#include <filesystem>

#include "io.hpp"
#include "lru_cache.hpp"

#include "../kv_level.hpp"

namespace lsm::detail {

template <typename Trait> class KVFileSystem {
private:
	constexpr static level_type kLevels = sizeof(Trait::kLevelConfigs) / sizeof(KVLevelConfig);

	struct fs_path_hasher {
		std::size_t operator()(const std::filesystem::path &path) const { return hash_value(path); }
	};

	mutable LRUCache<std::filesystem::path, std::ifstream, fs_path_hasher> m_input_stream_cache;
	std::filesystem::path m_directory;
	time_type m_time_stamp;

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

public:
	inline KVFileSystem(std::filesystem::path directory, size_type stream_capacity)
	    : m_directory{std::move(directory)}, m_input_stream_cache{stream_capacity}, m_time_stamp{0} {
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

	inline time_type GetTimeStamp() const { return m_time_stamp; }

	inline void MaintainTimeStamp(time_type time_stamp) { m_time_stamp = std::max(time_stamp + 1, m_time_stamp); }

	inline std::ifstream &GetFileStream(const std::filesystem::path &file_path, size_type pos) const {
		std::ifstream &ret = m_input_stream_cache.Push(file_path, [](const std::filesystem::path &path) {
			return std::ifstream{path, std::ios::binary};
		});
		return ret.seekg(pos), ret;
	}
	template <typename Writer> inline void CreateFile(level_type level, Writer &&writer) {
		std::filesystem::path file_path = get_level_dir(level) / (std::to_string(m_time_stamp) + ".sst");
		std::ofstream fout{file_path, std::ios::binary};
		IO<time_type>::Write(fout, m_time_stamp);
		writer(fout, std::move(file_path));
		++m_time_stamp;
	}

	inline void Reset() {
		m_input_stream_cache.Clear();
		if (std::filesystem::exists(m_directory))
			std::filesystem::remove_all(m_directory);
		m_time_stamp = 0;
		init_directory();
	}
};

} // namespace lsm::detail
