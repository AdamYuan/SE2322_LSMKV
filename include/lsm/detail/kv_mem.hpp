#pragma once

#include <optional>
#include <vector>

#include "buf_stream.hpp"
#include "kv_filesystem.hpp"
#include "kv_table.hpp"

namespace lsm {

template <typename Value> class KVMemValue {
private:
	std::optional<Value> m_opt_value;
	size_type m_size;

public:
	KVMemValue(Value &&value, size_type size) : m_opt_value{std::move(value)}, m_size{size} {}
	KVMemValue() : m_opt_value{}, m_size{0} {}
	inline size_type GetSize() const { return m_size; }
	inline bool IsDeleted() const { return !m_opt_value.has_value(); }
	inline const Value &GetValue() const { return m_opt_value.value(); }
	inline const std::optional<Value> &GetOptValue() const { return m_opt_value; }
};

} // namespace lsm

namespace lsm::detail {

template <typename Key, typename Value, typename Trait> class KVMemSkipList {
private:
	using FileSystem = KVFileSystem<Trait>;
	using BufferTable = KVBufferTable<Key, Value, Trait>;
	using FileTable = KVFileTable<Key, Value, Trait>;
	using ValueIO = typename Trait::ValueIO;
	using KeyOffset = KVKeyOffset<Key>;

	constexpr static size_type kMaxFileSize = Trait::kSingleFileSizeLimit;
	constexpr static size_type kInitialFileSize = sizeof(time_type) + Trait::KeyFile::GetHeaderSize();

	typename Trait::SkipList m_skiplist;
	size_type m_file_size{kInitialFileSize};

	template <typename Table, typename PopFunc>
	inline std::optional<Table> put(Key key, Value &&value, PopFunc &&pop_func) {
		size_type value_size = ValueIO::GetSize(value);
		if (m_skiplist.Replace(key, [this, &value, value_size](KVMemValue<Value> *p_sl_value, bool exists) -> bool {
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
		if (m_skiplist.Replace(key, [this](KVMemValue<Value> *p_sl_value, bool exists) -> bool {
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
		m_skiplist.ForEach([&key_buffer, &key_id, &value_stream](const Key &key, const KVMemValue<Value> &sl_value) {
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
			m_skiplist.ForEach([&key_buffer, &key_id, &value_pos](const Key &key, const KVMemValue<Value> &sl_value) {
				key_buffer[key_id++] = KeyOffset{key, value_pos, sl_value.IsDeleted()};
				value_pos += sl_value.GetSize();
			});
		}

		const auto value_writer = [this](std::ofstream &stream) {
			m_skiplist.ForEach([&stream](const Key &key, const KVMemValue<Value> &sl_value) {
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
	inline std::optional<KVMemValue<Value>> Get(Key key) const { return m_skiplist.Search(key); }
	inline bool IsEmpty() const { return m_skiplist.IsEmpty(); }
};

} // namespace lsm::detail