#pragma once

#include <vector>

#include "kv_table.hpp"
#include "kv_trait.hpp"

namespace lsm {

template <typename Key, typename Value, typename Trait> class KVMemSkipList {
private:
	using Buffer = KVBufferTable<Key, Value, Trait>;
	using ValueIO = typename Trait::ValueIO;
	using KeyOffset = KVKeyOffset<Key>;

	constexpr static size_type kMaxFileSize = Trait::kSingleFileSizeLimit;
	constexpr static size_type kInitialFileSize =
	    sizeof(time_type) + sizeof(size_type) + sizeof(Key) * 2 + IO<typename Trait::Bloom>::GetSize({});

	typename Trait::SkipList m_skiplist;
	size_type m_sorted_table_size{kInitialFileSize};

public:
	inline void Reset() {
		m_skiplist.Clear();
		m_sorted_table_size = kInitialFileSize;
	}
	inline Buffer PopBuffer(time_type time_stamp) {
		auto key_buffer = std::unique_ptr<KVKeyOffset<Key>[]>(new KVKeyOffset<Key>[m_skiplist.GetSize()]);

		size_type value_size = m_sorted_table_size - kInitialFileSize - m_skiplist.GetSize() * sizeof(KeyOffset);
		auto value_buffer = std::unique_ptr<byte[]>(new byte[value_size]);
		OBufStream value_stream{(char *)value_buffer.get()};

		size_type key_id = 0;
		m_skiplist.ForEach(
		    [&key_buffer, &key_id, &value_stream](const Key &key, const std::optional<Value> &value_opt) {
			    key_buffer[key_id++] = KeyOffset{key, value_stream.pos, !value_opt.has_value()};
			    if (value_opt.has_value())
				    ValueIO::Write(value_stream, value_opt.value());
		    });

		return Buffer{KVKeyTable<Key, Trait>{time_stamp, std::move(key_buffer), m_skiplist.GetSize()},
		              KVValueBuffer<Value, Trait>{std::move(value_buffer), value_size}};
	}
	inline std::optional<Buffer> Put(Key key, Value &&value, time_type time_stamp) {
		if (m_skiplist.Replace(key, [this, &value](std::optional<Value> *p_opt_value, bool exists) -> bool {
			    size_type new_size = m_sorted_table_size;
			    if (exists) {
				    new_size -= p_opt_value->has_value() ? ValueIO::GetSize(p_opt_value->value()) : 0;
				    new_size += ValueIO::GetSize(value);
			    } else
				    new_size += sizeof(KeyOffset) + ValueIO::GetSize(value);
			    if (new_size > kMaxFileSize)
				    return false;
			    *p_opt_value = std::move(value);
			    m_sorted_table_size = new_size;
			    return true;
		    }))
			return std::nullopt;

		auto ret = PopBuffer(time_stamp);
		Reset();
		m_sorted_table_size += sizeof(KeyOffset) + ValueIO::GetSize(value);
		m_skiplist.Insert(key, std::optional<Value>{std::move(value)});
		return {std::move(ret)};
	}
	inline std::optional<Buffer> Put(Key key, const Value &value, time_type time_stamp) {
		return Put(key, Value{value}, time_stamp);
	}
	inline std::optional<Buffer> Delete(Key key, time_type time_stamp) {
		if (m_skiplist.Replace(key, [this](std::optional<Value> *p_opt_value, bool exists) -> bool {
			    size_type new_size = m_sorted_table_size;
			    if (exists)
				    new_size -= p_opt_value->has_value() ? ValueIO::GetSize(p_opt_value->value()) : 0;
			    else
				    new_size += sizeof(KeyOffset);
			    if (new_size > kMaxFileSize)
				    return false;
			    *p_opt_value = std::nullopt;
			    m_sorted_table_size = new_size;
			    return true;
		    }))
			return std::nullopt;

		auto ret = PopBuffer(time_stamp);
		Reset();
		m_sorted_table_size += sizeof(KeyOffset);
		m_skiplist.Insert(key, std::nullopt);
		return {std::move(ret)};
	}
	inline std::optional<std::optional<Value>> Get(Key key) const { return m_skiplist.Search(key); }
	inline bool IsEmpty() const { return m_skiplist.IsEmpty(); }
};

template <typename Key, typename Value, typename Trait> class KVMemAppender {
private:
	using BufferTable = KVBufferTable<Key, Value, Trait>;
	using KeyOffset = KVKeyOffset<Key>;

	constexpr static size_type kMaxFileSize = Trait::kSingleFileSizeLimit;
	constexpr static size_type kInitialFileSize =
	    sizeof(time_type) + sizeof(size_type) + sizeof(Key) * 2 + IO<typename Trait::Bloom>::GetSize({});

	std::vector<KeyOffset> m_key_offset_vec;
	std::vector<byte> m_value_buffer;
	size_type m_sorted_table_size{kInitialFileSize};

public:
	inline KVMemAppender() { m_value_buffer.reserve(kMaxFileSize); }
	inline void Reset() {
		m_sorted_table_size = kInitialFileSize;
		m_key_offset_vec.clear();
		m_value_buffer.clear();
	}
	inline BufferTable PopBuffer(time_type time_stamp) {
		auto key_buffer = std::unique_ptr<KeyOffset[]>(new KeyOffset[m_key_offset_vec.size()]);
		std::copy(m_key_offset_vec.begin(), m_key_offset_vec.end(), key_buffer.get());
		auto value_buffer = std::unique_ptr<byte[]>(new byte[m_value_buffer.size()]);
		std::copy(m_value_buffer.begin(), m_value_buffer.end(), value_buffer.get());
		return BufferTable{
		    KVKeyTable<Key, Trait>{time_stamp, std::move(key_buffer), (size_type)m_key_offset_vec.size()},
		    KVValueBuffer<Value, Trait>{std::move(value_buffer), (size_type)m_value_buffer.size()}};
	}
	template <bool Delete, typename Iterator>
	inline std::optional<BufferTable> Append(const Iterator &it, time_type time_stamp) {
		if constexpr (Delete) {
			if (it.IsKeyDeleted())
				return std::nullopt;
		}
		size_type value_size = it.GetValueSize();
		size_type new_size = m_sorted_table_size + sizeof(KeyOffset) + value_size;
		if (new_size <= kMaxFileSize) {
			m_sorted_table_size = new_size;
			m_key_offset_vec.emplace_back(it.GetKey(), m_value_buffer.size(), it.IsKeyDeleted());
			if (value_size) {
				size_type prev_size = m_value_buffer.size();
				m_value_buffer.resize(prev_size + value_size);
				it.CopyValueData((char *)m_value_buffer.data() + prev_size);
			}
			return std::nullopt;
		}
		auto ret = PopBuffer(time_stamp);
		Reset();
		m_sorted_table_size += sizeof(KeyOffset) + value_size;
		m_key_offset_vec.emplace_back(it.GetKey(), m_value_buffer.size(), it.IsKeyDeleted());
		if (value_size) {
			m_value_buffer.resize(value_size);
			it.CopyValueData((char *)m_value_buffer.data());
		}
		return {std::move(ret)};
	}
	inline bool IsEmpty() const { return m_key_offset_vec.empty(); }
};

} // namespace lsm