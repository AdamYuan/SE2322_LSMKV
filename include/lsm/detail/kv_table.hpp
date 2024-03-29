#pragma once

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <utility>

#include "kv_filesystem.hpp"
#include "kv_key_table.hpp"
#include "kv_value_table.hpp"

namespace lsm::detail {

template <typename Key, typename Value, typename Trait, typename Table> class KVTableIterator {
private:
	using KeyIndex = typename decltype(((const Table *)0)->m_keys)::Index;
	const Table *m_p_table;
	KeyIndex m_key_index;

	inline KVKeyOffset<Key> get_key_offset(KeyIndex index) const { return m_p_table->m_keys.GetKeyOffset(index); }
	inline KVKeyOffset<Key> cur_key_offset() const { return get_key_offset(m_key_index); }

public:
	inline KVTableIterator(const Table *p_table, KeyIndex key_index) : m_p_table{p_table}, m_key_index{key_index} {}
	inline const Table &GetTable() const { return *m_p_table; }
	inline bool IsValid() const { return m_key_index != m_p_table->m_keys.GetEnd(); }
	inline bool IsKeyDeleted() const { return cur_key_offset().IsDeleted(); }
	inline Key GetKey() const { return cur_key_offset().GetKey(); }
	inline size_type GetValueSize() const {
		KeyIndex nxt = m_key_index + 1;
		return (nxt == m_p_table->m_keys.GetEnd() ? m_p_table->m_values.GetSize() : get_key_offset(nxt).GetOffset()) -
		       cur_key_offset().GetOffset();
	}
	inline Value ReadValue() const { return m_p_table->m_values.Read(cur_key_offset().GetOffset(), GetValueSize()); }
	inline void CopyValueData(char *dst) const {
		m_p_table->m_values.CopyData(cur_key_offset().GetOffset(), GetValueSize(), dst);
	}
	inline void Proceed() { ++m_key_index; }
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
			       (!KeyCompare{}(r.GetKey(), l.GetKey()) && l.GetTable().IsPrior(r.GetTable()));
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

template <typename DerivedTable, typename Key, typename Value, typename Trait, typename KeyTable, typename ValueTable>
class KVTableBase {
protected:
	KeyTable m_keys;
	ValueTable m_values;

	template <typename, typename, typename, typename> friend class KVTableIterator;

	inline DerivedTable *derived_this() { return static_cast<DerivedTable *>(this); }
	inline const DerivedTable *derived_this() const { return static_cast<const DerivedTable *>(this); }

public:
	using Iterator = KVTableIterator<Key, Value, Trait, DerivedTable>;

	inline Key GetMinKey() const { return m_keys.GetMin(); }
	inline Key GetMaxKey() const { return m_keys.GetMax(); }
	inline size_type GetKeyCount() const { return m_keys.GetCount(); }
	inline Iterator Find(Key key) const { return Iterator{derived_this(), m_keys.Find(key)}; }
	inline Iterator GetBegin() const { return Iterator{derived_this(), m_keys.GetBegin()}; }
	inline Iterator GetLowerBound(Key key) const { return Iterator{derived_this(), m_keys.GetLowerBound(key)}; }

	inline bool IsOverlap(Key min_key, Key max_key) const {
		using Compare = typename Trait::Compare;
		return !(Compare{}(GetMaxKey(), min_key) || Compare{}(max_key, GetMinKey()));
	}
	template <typename Table> inline bool IsOverlap(const Table &table) const {
		return IsOverlap(table.GetMinKey(), table.GetMaxKey());
	}
};

template <typename Key, typename Value, typename Trait>
class KVBufferTable final : public KVTableBase<KVBufferTable<Key, Value, Trait>, Key, Value, Trait,
                                               KVKeyBuffer<Key, Trait>, KVValueBuffer<Value, Trait>> {
private:
	using Base = KVTableBase<KVBufferTable, Key, Value, Trait, KVKeyBuffer<Key, Trait>, KVValueBuffer<Value, Trait>>;
	using KeyBuffer = KVKeyBuffer<Key, Trait>;
	using ValueBuffer = KVValueBuffer<Value, Trait>;

	template <typename, typename, typename> friend class KVFileTable;

public:
	inline static bool IsPrior(const KVBufferTable &) { return false; }
	inline KVBufferTable(KeyBuffer &&keys, ValueBuffer &&values) {
		Base::m_keys = std::move(keys);
		Base::m_values = std::move(values);
	}
};

template <typename Key, typename Value, typename Trait>
class KVFileTable final : public KVTableBase<KVFileTable<Key, Value, Trait>, Key, Value, Trait, typename Trait::KeyFile,
                                             KVValueFile<Value, Trait>> {
private:
	using FileSystem = KVFileSystem<Trait>;
	using KeyFile = typename Trait::KeyFile;
	using ValueFile = KVValueFile<Value, Trait>;

	time_type m_time_stamp{};
	level_type m_level{};

public:
	// inline time_type GetTimeStamp() const { return m_time_stamp; }
	inline bool IsPrior(const KVFileTable &r) const {
		return m_level < r.m_level || (m_level == r.m_level && m_time_stamp > r.m_time_stamp);
	}
	template <typename ValueWriter>
	inline KVFileTable(FileSystem *p_file_system, KVKeyBuffer<Key, Trait> &&key_buffer, ValueWriter &&value_writer,
	                   size_type value_size, level_type level)
	    : m_level{level}, m_time_stamp{p_file_system->GetTimeStamp()} {
		p_file_system->CreateFile(level, [this, p_file_system, value_size, &key_buffer,
		                                  &value_writer](std::ofstream &fout, const std::filesystem::path &file_path) {
			this->m_keys = KeyFile{fout, std::move(key_buffer), p_file_system, file_path};
			value_writer(fout);
			this->m_values =
			    ValueFile{p_file_system, file_path, (size_type)sizeof(time_type) + this->m_keys.GetSize(), value_size};
		});
	}
	inline explicit KVFileTable(FileSystem *p_file_system, const std::filesystem::path &file_path, level_type level)
	    : m_level{level} {
		std::ifstream &fin = p_file_system->GetFileStream(file_path, 0);
		m_time_stamp = IO<time_type>::Read(fin);
		this->m_keys = KeyFile{fin, p_file_system, file_path};
		size_type value_offset = this->m_keys.GetSize() + (size_type)sizeof(time_type);
		size_type value_size = std::filesystem::file_size(file_path) - value_offset;
		this->m_values = ValueFile{p_file_system, file_path, value_offset, value_size};

		p_file_system->MaintainTimeStamp(m_time_stamp);
	}
	inline const std::filesystem::path &GetFilePath() const { return this->m_values.GetFilePath(); }
};

} // namespace lsm::detail
