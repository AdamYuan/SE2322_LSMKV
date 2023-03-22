#pragma once

#include "kv_table.hpp"
#include "kv_trait.hpp"

namespace lsm {

template <typename Key, typename Value, typename Trait> class KVMem {
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
		auto key_buffer = std::make_unique<KVKeyOffset<Key>[]>(m_skiplist.GetSize());

		size_type value_size = m_sorted_table_size - kInitialFileSize - m_skiplist.GetSize() * sizeof(KeyOffset);
		auto value_buffer = std::make_unique<byte[]>(value_size);
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

} // namespace lsm