#pragma once

#include <vector>

#include "buf_stream.hpp"
#include "kv_filesystem.hpp"
#include "kv_table.hpp"
#include "kv_trait.hpp"

namespace lsm {

template <typename Key, typename Value, typename Trait> class KVMemSkipList {
private:
	using FileSystem = KVFileSystem<Trait>;
	using BufferTable = KVBufferTable<Key, Value, Trait>;
	using FileTable = KVFileTable<Key, Value, Trait>;
	using ValueIO = typename Trait::ValueIO;
	using KeyOffset = KVKeyOffset<Key>;

	constexpr static size_type kMaxFileSize = Trait::kSingleFileSizeLimit;
	constexpr static size_type kInitialFileSize =
	    sizeof(time_type) + sizeof(size_type) + sizeof(Key) * 2 + IO<typename Trait::Bloom>::GetSize({});

	typename Trait::SkipList m_skiplist;
	size_type m_file_size{kInitialFileSize};

	template <typename Table, typename PopFunc>
	inline std::optional<Table> put(Key key, Value &&value, PopFunc &&pop_func) {
		size_type value_size = ValueIO::GetSize(value);
		if (m_skiplist.Replace(key,
		                       [this, &value, value_size](KVSkipListValue<Value> *p_sl_value, bool exists) -> bool {
			                       size_type new_size = m_file_size;
			                       if (exists) {
				                       new_size -= p_sl_value->GetSize();
				                       new_size += value_size;
			                       } else
				                       new_size += sizeof(KeyOffset) + value_size;
			                       if (m_file_size != kInitialFileSize && new_size > kMaxFileSize)
				                       return false;
			                       *p_sl_value = {std::move(value), value_size};
			                       m_file_size = new_size;
			                       return true;
		                       }))
			return std::nullopt;

		Table ret = pop_func();
		Reset();
		m_file_size += sizeof(KeyOffset) + value_size;
		m_skiplist.Insert(key, {std::move(value), value_size});
		return std::optional<Table>{std::move(ret)};
	}

	template <typename Table, typename PopFunc> inline std::optional<Table> del(Key key, PopFunc &&pop_func) {
		if (m_skiplist.Replace(key, [this](KVSkipListValue<Value> *p_sl_value, bool exists) -> bool {
			    size_type new_size = m_file_size;
			    if (exists)
				    new_size -= p_sl_value->GetSize();
			    else
				    new_size += sizeof(KeyOffset);
			    if (m_file_size != kInitialFileSize && new_size > kMaxFileSize)
				    return false;
			    *p_sl_value = {};
			    m_file_size = new_size;
			    return true;
		    }))
			return std::nullopt;

		Table ret = pop_func();
		Reset();
		m_file_size += sizeof(KeyOffset);
		m_skiplist.Insert(key, {});
		return std::optional<Table>{std::move(ret)};
	}

public:
	inline void Reset() {
		m_skiplist.Clear();
		m_file_size = kInitialFileSize;
	}
	inline BufferTable PopBuffer() {
		auto key_buffer = std::unique_ptr<KVKeyOffset<Key>[]>(new KVKeyOffset<Key>[m_skiplist.GetSize()]);

		size_type value_size = m_file_size - kInitialFileSize - m_skiplist.GetSize() * sizeof(KeyOffset);
		auto value_buffer = std::unique_ptr<byte[]>(new byte[value_size]);
		OBufStream value_stream{(char *)value_buffer.get()};

		size_type key_id = 0;
		m_skiplist.ForEach(
		    [&key_buffer, &key_id, &value_stream](const Key &key, const KVSkipListValue<Value> &sl_value) {
			    key_buffer[key_id++] = KeyOffset{key, value_stream.pos, sl_value.IsDeleted()};
			    if (!sl_value.IsDeleted())
				    ValueIO::Write(value_stream, sl_value.GetValue());
		    });

		return BufferTable{KVKeyBuffer<Key, Trait>{std::move(key_buffer), m_skiplist.GetSize()},
		                   KVValueBuffer<Value, Trait>{std::move(value_buffer), value_size}};
	}
	inline FileTable PopFile(FileSystem *p_file_system, level_type level) {
		auto key_buffer = std::unique_ptr<KVKeyOffset<Key>[]>(new KVKeyOffset<Key>[m_skiplist.GetSize()]);

		size_type value_size = m_file_size - kInitialFileSize - m_skiplist.GetSize() * sizeof(KeyOffset);

		{
			size_type key_id = 0, value_pos = 0;
			m_skiplist.ForEach(
			    [&key_buffer, &key_id, &value_pos](const Key &key, const KVSkipListValue<Value> &sl_value) {
				    key_buffer[key_id++] = KeyOffset{key, value_pos, sl_value.IsDeleted()};
				    value_pos += sl_value.GetSize();
			    });
		}

		const auto value_writer = [this](std::ofstream &stream) {
			m_skiplist.ForEach([&stream](const Key &key, const KVSkipListValue<Value> &sl_value) {
				if (!sl_value.IsDeleted())
					ValueIO::Write(stream, sl_value.GetValue());
			});
		};

		return FileTable{p_file_system, KVKeyBuffer<Key, Trait>{std::move(key_buffer), m_skiplist.GetSize()},
		                 value_writer, value_size, level};
	}
	inline std::optional<BufferTable> Put(Key key, Value &&value) {
		return put<BufferTable>(key, std::move(value), [this]() { return PopBuffer(); });
	}
	inline std::optional<BufferTable> Put(Key key, const Value &value) { return Put(key, Value{value}); }

	inline std::optional<FileTable> Put(Key key, Value &&value, FileSystem *p_file_system, level_type level) {
		return put<FileTable>(key, std::move(value),
		                      [this, p_file_system, level]() { return PopFile(p_file_system, level); });
	}
	inline std::optional<FileTable> Put(Key key, const Value &value, FileSystem *p_file_system, level_type level) {
		return Put(key, Value(value), p_file_system, level);
	}

	inline std::optional<BufferTable> Delete(Key key) {
		return del<BufferTable>(key, [this]() { return PopBuffer(); });
	}
	inline std::optional<FileTable> Delete(Key key, FileSystem *p_file_system, level_type level) {
		return del<FileTable>(key, [this, p_file_system, level]() { return PopFile(p_file_system, level); });
	}

	template <typename Func> inline void Scan(Key min_key, Key max_key, Func &&func) const {
		m_skiplist.Scan(min_key, max_key, std::forward<Func>(func));
	}
	inline std::optional<KVSkipListValue<Value>> Get(Key key) const { return m_skiplist.Search(key); }
	inline bool IsEmpty() const { return m_skiplist.IsEmpty(); }
};

template <typename Key, typename Value, typename Trait> class KVMemAppender {
private:
	using FileSystem = KVFileSystem<Trait>;
	using BufferTable = KVBufferTable<Key, Value, Trait>;
	using FileTable = KVFileTable<Key, Value, Trait>;
	using KeyOffset = KVKeyOffset<Key>;

	constexpr static size_type kMaxFileSize = Trait::kSingleFileSizeLimit;
	constexpr static size_type kInitialFileSize =
	    sizeof(time_type) + sizeof(size_type) + sizeof(Key) * 2 + IO<typename Trait::Bloom>::GetSize({});

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
	inline KVMemAppender() { reset_value_buffer(); }
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

} // namespace lsm