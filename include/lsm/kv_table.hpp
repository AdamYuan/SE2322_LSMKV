#pragma once

#include <filesystem>
#include <fstream>
#include <utility>

#include "kv_key_table.hpp"
#include "kv_value_table.hpp"

namespace lsm {

template <typename Key, typename Value, typename Trait, typename Table> class KVTableIterator {
private:
	const Table *m_p_table;
	const KVKeyOffset<Key> *m_p_key_offset;
	time_type m_time_stamp;

public:
	inline KVTableIterator(const Table *p_table, const KVKeyOffset<Key> *p_key_offset)
	    : m_p_table{p_table}, m_p_key_offset{p_key_offset}, m_time_stamp{m_p_table->GetTimeStamp()} {}
	inline time_type GetTimeStamp() const { return m_time_stamp; }
	inline bool IsValid() const { return m_p_key_offset != m_p_table->m_keys.GetEnd(); }
	inline bool IsKeyDeleted() const { return m_p_key_offset->IsDeleted(); }
	inline Key GetKey() const { return m_p_key_offset->GetKey(); }
	inline size_type GetValueSize() const {
		const KVKeyOffset<Key> *p_nxt = m_p_key_offset + 1;
		return (p_nxt == m_p_table->m_keys.GetEnd() ? m_p_table->m_values.GetSize() : p_nxt->GetOffset()) -
		       m_p_key_offset->GetOffset();
	}
	inline Value ReadValue() const { return m_p_table->m_values.Read(m_p_key_offset->GetOffset(), GetValueSize()); }
	inline void CopyValueData(char *dst) const {
		m_p_table->m_values.CopyData(m_p_key_offset->GetOffset(), GetValueSize(), dst);
	}
	inline void Proceed() { ++m_p_key_offset; }
};

template <typename Iterator> class KVTableIteratorHeap;
template <typename Key, typename Value, typename Trait, typename Table>
class KVTableIteratorHeap<KVTableIterator<Key, Value, Trait, Table>> {
private:
	using Iterator = KVTableIterator<Key, Value, Trait, Table>;
	using KeyCompare = typename Trait::Compare;
	struct Compare {
		inline bool operator()(const Iterator &l, const Iterator &r) const {
			return KeyCompare{}(l.GetKey(), r.GetKey()) ||
			       (!KeyCompare{}(r.GetKey(), l.GetKey()) && l.GetTimeStamp() > r.GetTimeStamp());
		}
	};
	struct RevCompare {
		inline bool operator()(const Iterator &l, const Iterator &r) const { return Compare{}(r, l); }
	};
	std::vector<Iterator> m_vec;

public:
	inline KVTableIteratorHeap() = default;
	inline explicit KVTableIteratorHeap(std::vector<Iterator> &&vec) : m_vec{std::move(vec)} {
		std::make_heap(m_vec.begin(), m_vec.end(), RevCompare{});
	}
	inline bool IsEmpty() const { return m_vec.empty(); }
	inline Iterator GetTop() const { return m_vec.front(); }
	inline void Proceed() {
		Key key = m_vec.front().GetKey();
		do {
			std::pop_heap(m_vec.begin(), m_vec.end(), RevCompare{});
			m_vec.back().Proceed();
			if (m_vec.back().IsValid())
				std::push_heap(m_vec.begin(), m_vec.end(), RevCompare{});
			else
				m_vec.pop_back();
		} while (!m_vec.empty() && !KeyCompare{}(key, m_vec.front().GetKey()));
	}
};

template <typename Key, typename Value, typename Trait, typename KeyTable, typename ValueTable> class KVTableBase {
protected:
	KeyTable m_keys;
	ValueTable m_values;

	template <typename, typename, typename, typename> friend class KVTableIterator;

public:
	using Iterator = KVTableIterator<Key, Value, Trait, KVTableBase>;

	inline time_type GetTimeStamp() const { return m_keys.GetTimeStamp(); }
	inline Key GetMinKey() const { return m_keys.GetMin(); }
	inline Key GetMaxKey() const { return m_keys.GetMax(); }
	inline size_type GetKeyCount() const { return m_keys.GetCount(); }
	inline Iterator Find(Key key) const { return Iterator{this, m_keys.Find(key)}; }
	inline Iterator GetBegin() const { return Iterator{this, m_keys.GetBegin()}; }
	inline Iterator GetLowerBound(Key key) const { return Iterator{this, m_keys.GetLowerBound(key)}; }

	inline bool IsOverlap(Key min_key, Key max_key) const {
		using Compare = typename Trait::Compare;
		return !(Compare{}(GetMaxKey(), min_key) || Compare{}(max_key, GetMinKey()));
	}
	template <typename Table> inline bool IsOverlap(const Table &table) const {
		return IsOverlap(table.GetMinKey(), table.GetMaxKey());
	}
};

template <typename Key, typename Value, typename Trait>
class KVBufferTable final
    : public KVTableBase<Key, Value, Trait, KVKeyBuffer<Key, Trait>, KVValueBuffer<Value, Trait>> {
private:
	using Base = KVTableBase<Key, Value, Trait, KVKeyBuffer<Key, Trait>, KVValueBuffer<Value, Trait>>;
	using KeyBuffer = KVKeyBuffer<Key, Trait>;
	using ValueBuffer = KVValueBuffer<Value, Trait>;

	template <typename, typename, typename> friend class KVFileTable;

public:
	inline KVBufferTable(KeyBuffer &&keys, ValueBuffer &&values) {
		Base::m_keys = std::move(keys);
		Base::m_values = std::move(values);
	}
};

template <typename Key, typename Value, typename Trait>
class KVFileTable final : public KVTableBase<Key, Value, Trait, KVKeyFile<Key, Trait>, KVValueFile<Value, Trait>> {
private:
	using Base = KVTableBase<Key, Value, Trait, KVKeyFile<Key, Trait>, KVValueFile<Value, Trait>>;
	using KeyFile = KVKeyFile<Key, Trait>;
	using ValueFile = KVValueFile<Value, Trait>;

public:
	inline KVFileTable(const std::filesystem::path &file_path, KVBufferTable<Key, Value, Trait> &&buffer) {
		Base::m_keys = KeyFile{std::move(buffer.m_keys)};
		{
			std::ofstream fout{file_path, std::ios::binary};
			IO<KeyFile>::Write(fout, Base::m_keys);
			fout.write((char *)buffer.m_values.GetData(), buffer.m_values.GetSize());
		}
		Base::m_values = ValueFile{file_path, std::ifstream{file_path, std::ios::binary},
		                           IO<KeyFile>::GetSize(Base::m_keys), buffer.m_values.GetSize()};
	}
	inline explicit KVFileTable(const std::filesystem::path &file_path) {
		std::ifstream fin{file_path, std::ios::binary};
		Base::m_keys = IO<KeyFile>::Read(fin);
		size_type value_offset = IO<KeyFile>::GetSize(Base::m_keys);
		size_type value_size = std::filesystem::file_size(file_path) - value_offset;
		Base::m_values = ValueFile{file_path, std::move(fin), value_offset, value_size};
	}
	inline const std::filesystem::path &GetFilePath() const { return Base::m_values.GetFilePath(); }
};

} // namespace lsm
