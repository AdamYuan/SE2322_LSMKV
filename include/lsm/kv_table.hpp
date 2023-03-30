#pragma once

#include <filesystem>
#include <fstream>
#include <utility>

#include "kv_filesystem.hpp"
#include "kv_key_table.hpp"
#include "kv_value_table.hpp"

namespace lsm {

template <typename Key, typename Value, typename Trait, typename Table> class KVTableIterator {
private:
	const Table *m_p_table;
	const KVKeyOffset<Key> *m_p_key_offset;

public:
	inline KVTableIterator(const Table *p_table, const KVKeyOffset<Key> *p_key_offset)
	    : m_p_table{p_table}, m_p_key_offset{p_key_offset} {}
	inline const Table &GetTable() const { return *m_p_table; }
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
	inline static bool IsPrior(const KVBufferTable &) { return true; }
	inline KVBufferTable(KeyBuffer &&keys, ValueBuffer &&values) {
		Base::m_keys = std::move(keys);
		Base::m_values = std::move(values);
	}
};

template <typename Key, typename Value, typename Trait>
class KVFileTable final : public KVTableBase<KVFileTable<Key, Value, Trait>, Key, Value, Trait, KVKeyFile<Key, Trait>,
                                             KVValueFile<Value, Trait>> {
private:
	using Base = KVTableBase<KVFileTable, Key, Value, Trait, KVKeyFile<Key, Trait>, KVValueFile<Value, Trait>>;
	using FileSystem = KVFileSystem<Trait>;
	using KeyFile = KVKeyFile<Key, Trait>;
	using ValueFile = KVValueFile<Value, Trait>;

	time_type m_time_stamp{};
	level_type m_level{};

public:
	inline time_type GetTimeStamp() const { return m_time_stamp; }
	inline bool IsPrior(const KVFileTable &r) const {
		return m_level < r.m_level || (m_level == r.m_level && m_time_stamp > r.m_time_stamp);
	}
	inline KVFileTable(FileSystem *p_file_system, KVBufferTable<Key, Value, Trait> &&buffer, level_type level)
	    : m_level{level}, m_time_stamp{p_file_system->GetTimeStamp()} {
		Base::m_keys = KeyFile{std::move(buffer.m_keys)};
		auto [time_stamp, file_path] = p_file_system->CreateFile(level, [this, &buffer](std::ofstream &fout) {
			IO<KeyFile>::Write(fout, Base::m_keys);
			fout.write((char *)buffer.m_values.GetData(), buffer.m_values.GetSize());
		});
		m_time_stamp = time_stamp;
		Base::m_values =
		    ValueFile{p_file_system, file_path, IO<KeyFile>::GetSize(Base::m_keys) + (size_type)sizeof(time_type),
		              buffer.m_values.GetSize()};
	}
	template <typename ValueWriter>
	inline KVFileTable(FileSystem *p_file_system, KVKeyBuffer<Key, Trait> &&key_buffer, ValueWriter &&value_writer,
	                   size_type value_size, level_type level)
	    : m_level{level} {
		Base::m_keys = KeyFile{std::move(key_buffer)};
		auto [time_stamp, file_path] = p_file_system->CreateFile(level, [this, &value_writer](std::ofstream &fout) {
			IO<KeyFile>::Write(fout, Base::m_keys);
			value_writer(fout);
		});
		m_time_stamp = time_stamp;
		Base::m_values = ValueFile{p_file_system, file_path,
		                           IO<KeyFile>::GetSize(Base::m_keys) + (size_type)sizeof(time_type), value_size};
	}
	inline explicit KVFileTable(FileSystem *p_file_system, const std::filesystem::path &file_path, level_type level)
	    : m_level{level} {
		std::ifstream &fin = p_file_system->GetFileStream(file_path);
		fin.seekg(0);
		m_time_stamp = IO<time_type>::Read(fin);
		Base::m_keys = IO<KeyFile>::Read(fin);
		size_type value_offset = IO<KeyFile>::GetSize(Base::m_keys) + (size_type)sizeof(time_type);
		size_type value_size = std::filesystem::file_size(file_path) - value_offset;
		Base::m_values = ValueFile{p_file_system, file_path, value_offset, value_size};

		p_file_system->MaintainTimeStamp(m_time_stamp);
	}
	inline const std::filesystem::path &GetFilePath() const { return Base::m_values.GetFilePath(); }
};

} // namespace lsm
