#pragma once

#include <utility>

#include "buf_stream.hpp"
#include "io.hpp"
#include "kv_trait.hpp"
#include "lru_cache.hpp"
#include "type.hpp"

namespace lsm {

template <typename Value, typename Trait> class KVValueBuffer {
private:
	using ValueIO = typename Trait::ValueIO;

	std::unique_ptr<byte[]> m_bytes;
	size_type m_size{};

public:
	inline KVValueBuffer() = default;
	inline KVValueBuffer(std::unique_ptr<byte[]> &&bytes, size_type size) : m_bytes{std::move(bytes)}, m_size{size} {}

	inline size_type GetSize() const { return m_size; }
	inline Value Read(size_type begin, size_type len) const {
		IBufStream bin{(const char *)m_bytes.get(), begin};
		return ValueIO::Read(bin, len);
	}
	inline void CopyData(size_type begin, size_type len, char *dst) const {
		auto src = (const char *)m_bytes.get();
		std::copy(src + begin, src + begin + len, dst);
	}
	inline const byte *GetData() const { return m_bytes.get(); }
};

template <typename Value, typename Trait> class KVValueFile {
private:
	using FileSystem = KVFileSystem<Trait>;
	using ValueIO = typename Trait::ValueIO;

	FileSystem *m_p_file_system{};
	std::filesystem::path m_file_path;
	size_type m_offset{}, m_size{};

public:
	inline KVValueFile() = default;
	inline KVValueFile(FileSystem *p_file_system, std::filesystem::path file_path, size_type offset, size_type size)
	    : m_p_file_system{p_file_system}, m_file_path{std::move(file_path)}, m_offset{offset}, m_size{size} {}

	inline const std::filesystem::path &GetFilePath() const { return m_file_path; }

	inline size_type GetSize() const { return m_size; }
	inline Value Read(size_type begin, size_type len) const {
		std::ifstream &fin = m_p_file_system->GetInputStream(m_file_path);
		fin.seekg(m_offset + begin);
		return ValueIO::Read(fin, len);
	}
	inline void CopyData(size_type begin, size_type len, char *dst) const {
		std::ifstream &fin = m_p_file_system->GetInputStream(m_file_path);
		fin.seekg(m_offset + begin);
		fin.read(dst, len);
	}
};

} // namespace lsm
