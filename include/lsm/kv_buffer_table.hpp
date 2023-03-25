#pragma once

#include "io.hpp"
#include "kv_trait.hpp"
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
	using ValueIO = typename Trait::ValueIO;

	mutable std::ifstream m_file_stream;
	size_type m_offset{}, m_size{};

public:
	inline KVValueFile() = default;
	inline KVValueFile(std::ifstream &&file_stream, size_type offset, size_type size)
	    : m_file_stream{std::move(file_stream)}, m_offset{offset}, m_size{size} {}

	inline size_type GetSize() const { return m_size; }
	inline Value Read(size_type begin, size_type len) const {
		m_file_stream.seekg(m_offset + begin);
		return ValueIO::Read(m_file_stream, len);
	}
	inline void CopyData(size_type begin, size_type len, char *dst) const {
		m_file_stream.seekg(m_offset + begin);
		m_file_stream.read(dst, len);
	}
};

} // namespace lsm
