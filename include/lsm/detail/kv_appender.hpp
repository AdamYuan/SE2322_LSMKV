#pragma once

#include <optional>
#include <vector>

#include "kv_filesystem.hpp"
#include "kv_table.hpp"

namespace lsm::detail {

template <typename Key, typename Value, typename Trait> class KVAppender {
private:
	using FileSystem = KVFileSystem<Trait>;
	using BufferTable = KVBufferTable<Key, Value, Trait>;
	using FileTable = KVFileTable<Key, Value, Trait>;
	using KeyOffset = KVKeyOffset<Key>;

	constexpr static size_type kMaxFileSize = Trait::kMaxFileSize;
	constexpr static size_type kInitialFileSize = sizeof(time_type) + Trait::KeyFile::GetHeaderSize();

	std::vector<KeyOffset> m_key_offset_vec;
	std::unique_ptr<byte[]> m_value_buffer;
	size_type m_value_buffer_size{}, m_value_buffer_cap{};

	size_type m_file_size{kInitialFileSize};

	inline void reset_value_buffer() {
		m_value_buffer_size = 0;
		m_value_buffer_cap = kMaxFileSize - kInitialFileSize;
		if (!m_value_buffer)
			m_value_buffer = std::unique_ptr<byte[]>(new byte[kMaxFileSize - kInitialFileSize]);
	}
	inline void ensure_value_buffer_cap(size_type size) {
		if (size <= m_value_buffer_cap)
			return;
		auto new_buffer = std::unique_ptr<byte[]>(new byte[size]);
		std::copy(m_value_buffer.get(), m_value_buffer.get() + m_value_buffer_size, new_buffer.get());
		m_value_buffer = std::move(new_buffer);
	}

	template <typename Table, bool Delete, typename Iterator, typename PopFunc>
	inline std::optional<Table> append(const Iterator &it, PopFunc &&pop_func) {
		if constexpr (Delete) {
			if (it.IsKeyDeleted())
				return std::nullopt;
		}
		size_type value_size = it.GetValueSize();
		size_type new_size = m_file_size + sizeof(KeyOffset) + value_size;
		if (m_file_size == kInitialFileSize || new_size <= kMaxFileSize) {
			m_file_size = new_size;
			m_key_offset_vec.emplace_back(it.GetKey(), m_value_buffer_size, it.IsKeyDeleted());
			if (value_size) {
				ensure_value_buffer_cap(m_value_buffer_size + value_size);
				it.CopyValueData((char *)m_value_buffer.get() + m_value_buffer_size);
				m_value_buffer_size += value_size;
			}
			return std::nullopt;
		}
		Table ret = pop_func();
		Reset();
		m_file_size += sizeof(KeyOffset) + value_size;
		m_key_offset_vec.emplace_back(it.GetKey(), m_value_buffer_size, it.IsKeyDeleted());
		if (value_size) {
			ensure_value_buffer_cap(value_size);
			it.CopyValueData((char *)m_value_buffer.get());
			m_value_buffer_size = value_size;
		}
		return std::optional<Table>{std::move(ret)};
	}

public:
	inline KVAppender() { reset_value_buffer(); }
	inline void Reset() {
		m_file_size = kInitialFileSize;
		m_key_offset_vec.clear();
		reset_value_buffer();
	}
	inline BufferTable PopBuffer() {
		auto key_buffer = std::unique_ptr<KeyOffset[]>(new KeyOffset[m_key_offset_vec.size()]);
		std::copy(m_key_offset_vec.begin(), m_key_offset_vec.end(), key_buffer.get());
		auto ret = BufferTable{KVKeyBuffer<Key, Trait>{std::move(key_buffer), (size_type)m_key_offset_vec.size()},
		                       KVValueBuffer<Value, Trait>{std::move(m_value_buffer), m_value_buffer_size}};
		m_value_buffer = nullptr;
		return ret;
	}
	inline FileTable PopFile(FileSystem *p_file_system, level_type level) {
		auto key_buffer = std::unique_ptr<KeyOffset[]>(new KeyOffset[m_key_offset_vec.size()]);
		std::copy(m_key_offset_vec.begin(), m_key_offset_vec.end(), key_buffer.get());
		return FileTable{
			p_file_system, KVKeyBuffer<Key, Trait>{std::move(key_buffer), (size_type)m_key_offset_vec.size()},
			[this](std::ofstream &fout) { fout.write((const char *)m_value_buffer.get(), m_value_buffer_size); },
			m_value_buffer_size, level};
	}
	template <bool Delete, typename Iterator> inline std::optional<BufferTable> Append(const Iterator &it) {
		return append<BufferTable, Delete>(it, [this]() { return PopBuffer(); });
	}
	template <bool Delete, typename Iterator>
	inline std::optional<FileTable> Append(const Iterator &it, FileSystem *p_file_system, level_type level) {
		return append<FileTable, Delete>(it, [this, p_file_system, level]() { return PopFile(p_file_system, level); });
	}
	inline bool IsEmpty() const { return m_key_offset_vec.empty(); }
};

}
