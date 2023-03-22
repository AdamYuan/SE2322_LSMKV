#pragma once

#include <filesystem>
#include <utility>
#include <variant>

#include "io.hpp"
#include "kv_trait.hpp"

namespace lsm {

#pragma pack(push, 1)
template <typename Key> class KVKeyOffset {
private:
	Key m_key{};
	size_type m_d_offset{};

public:
	inline KVKeyOffset() = default;
	inline KVKeyOffset(Key key, size_type offset, bool deleted)
	    : m_key{key}, m_d_offset{(offset & 0x7fffffffu) | (deleted ? 0x80000000u : 0u)} {}
	inline Key GetKey() const { return m_key; }
	inline size_type GetOffset() const { return m_d_offset & 0x7fffffffu; }
	inline bool IsDeleted() const { return m_d_offset >> 31u; }
};
#pragma pack(pop)

template <typename Key, typename Trait> class KVKeyTable {
private:
	using KeyOffset = KVKeyOffset<Key>;
	using Compare = typename Trait::Compare;

	time_type m_time_stamp{};
	size_type m_count{};
	Key m_min, m_max;

	typename Trait::Bloom m_bloom;
	std::unique_ptr<KeyOffset[]> m_keys;

	template <typename> friend class IO;

public:
	inline KVKeyTable() = default;
	inline KVKeyTable(time_type time_stamp, std::unique_ptr<KeyOffset[]> &&keys, size_type count)
	    : m_time_stamp{time_stamp}, m_keys{std::move(keys)}, m_count{count} {
		for (size_type i = 0; i < count; ++i)
			m_bloom.Insert(m_keys[i].GetKey());
		m_min = m_keys[0].GetKey();
		m_max = m_keys[count - 1].GetKey();
	}
	inline time_type GetTimeStamp() const { return m_time_stamp; }
	inline size_type GetCount() const { return m_count; }
	inline Key GetMin() const { return m_min; }
	inline Key GetMax() const { return m_max; }
	inline const auto &GetBloom() const { return m_bloom; }
	inline bool IsExcluded(Key key) const {
		return Compare{}(key, m_min) || Compare{}(m_max, key) || !m_bloom.Exist(key);
	}
	inline const KeyOffset *GetBegin() const { return m_keys.get(); }
	inline const KeyOffset *GetEnd() const { return m_keys.get() + m_count; }
	inline const KeyOffset *Find(Key key) const {
		if (IsExcluded(key))
			return nullptr;
		const KeyOffset *first = GetBegin(), *key_it;
		size_type count = m_count, step;
		while (count > 0) {
			step = count >> 1u;
			key_it = first + step;
			if (Compare{}(key_it->GetKey(), key)) {
				first = ++key_it;
				count -= step + 1;
			} else
				count = step;
		}
		return key_it == GetEnd() || Compare{}(key_it->GetKey(), key) || Compare{}(key, key_it->GetKey()) ? nullptr
		                                                                                                  : key_it;
	}
};

template <typename Key, typename Trait> struct IO<KVKeyTable<Key, Trait>> {
	inline static constexpr size_type GetSize(const KVKeyTable<Key, Trait> &keys) {
		return sizeof(time_type) + sizeof(size_type) + sizeof(Key) * 2 + IO<typename Trait::Bloom>::GetSize({}) +
		       sizeof(KVKeyOffset<Key>) * keys.GetCount();
	}
	template <typename Stream> inline static void Write(Stream &ostr, const KVKeyTable<Key, Trait> &keys) {
		IO<time_type>::Write(ostr, keys.m_time_stamp);
		IO<size_type>::Write(ostr, keys.m_count);
		IO<Key>::Write(ostr, keys.m_min);
		IO<Key>::Write(ostr, keys.m_max);
		IO<typename Trait::Bloom>::Write(ostr, keys.m_bloom);
		ostr.write((const char *)keys.m_keys.get(), keys.m_count * sizeof(KVKeyOffset<Key>));
	}
	template <typename Stream> inline static KVKeyTable<Key, Trait> Read(Stream &istr, size_type = -1) {
		KVKeyTable<Key, Trait> table = {};
		table.m_time_stamp = IO<time_type>::Read(istr);
		table.m_count = IO<size_type>::Read(istr);
		table.m_min = IO<Key>::Read(istr);
		table.m_max = IO<Key>::Read(istr);
		table.m_bloom = IO<typename Trait::Bloom>::Read(istr);
		table.m_keys = std::make_unique<KVKeyOffset<Key>[]>(table.m_count);
		istr.read((char *)table.m_keys.get(), table.m_count * sizeof(KVKeyOffset<Key>));
		return table;
	}
};

template <typename Value, typename Trait> class KVValueBuffer {
private:
	using ValueIO = typename Trait::ValueIO;

	std::unique_ptr<byte[]> m_bytes;
	size_type m_size;

public:
	inline KVValueBuffer(std::unique_ptr<byte[]> &&bytes, size_type size) : m_bytes{std::move(bytes)}, m_size{size} {}

	inline size_type GetSize() const { return m_size; }
	inline Value Read(size_type begin, size_type end = -1) const {
		size_type len = (~end ? end : m_size) - begin;
		IBufStream bin{(const char *)m_bytes.get(), begin};
		return ValueIO::Read(bin, len);
	}
	inline const byte *GetData() const { return m_bytes.get(); }
};

template <typename Value, typename Trait> class KVValueFile {
private:
	using ValueIO = typename Trait::ValueIO;

	std::filesystem::path m_file_path;
	size_type m_offset{}, m_size{};

public:
	inline KVValueFile() = default;
	inline KVValueFile(std::filesystem::path file_path, size_type offset, size_type size)
	    : m_file_path{std::move(file_path)}, m_offset{offset}, m_size{size} {}

	inline size_type GetSize() const { return m_size; }
	inline Value Read(size_type begin, size_type end = -1) const {
		size_type len = ((~end) ? end : m_size) - begin;
		std::ifstream fin{m_file_path, std::ios::binary};
		fin.seekg(m_offset + begin);
		return ValueIO::Read(fin, len);
	}
};

template <typename Key, typename Value, typename Trait> class KVBufferTable {
private:
	using KeyTable = KVKeyTable<Key, Trait>;
	using ValueBuffer = KVValueBuffer<Value, Trait>;

	KeyTable m_keys;
	ValueBuffer m_values;

	template <typename, typename, typename> friend class KVFileTable;

public:
	inline KVBufferTable(KeyTable &&keys, ValueBuffer &&values)
	    : m_keys{std::move(keys)}, m_values{std::move(values)} {}

	inline time_type GetTimeStamp() const { return m_keys.GetTimeStamp(); }
	inline const auto &GetKeys() const { return m_keys; }
	inline const auto &GetValues() const { return m_values; }
};

template <typename Key, typename Value, typename Trait> class KVFileTable {
private:
	using KeyTable = KVKeyTable<Key, Trait>;
	using ValueFile = KVValueFile<Value, Trait>;

	KeyTable m_keys;
	ValueFile m_values;

public:
	inline KVFileTable(const std::filesystem::path &file_path, KVBufferTable<Key, Value, Trait> &&buffer)
	    : m_values{file_path, IO<KeyTable>::GetSize(buffer.GetKeys()), buffer.GetValues().GetSize()} {
		m_keys = std::move(buffer.m_keys);
		std::ofstream fout{file_path, std::ios::binary};
		IO<KeyTable>::Write(fout, m_keys);
		fout.write((char *)buffer.GetValues().GetData(), buffer.GetValues().GetSize());
	}
	inline explicit KVFileTable(const std::filesystem::path &file_path) {
		std::ifstream fin{file_path, std::ios::binary};
		m_keys = IO<KeyTable>::Read(fin);
		size_type value_offset = IO<KeyTable>::GetSize(m_keys);
		size_type value_size = std::filesystem::file_size(file_path) - value_offset;
		m_values = ValueFile{file_path, value_offset, value_size};
	}

	inline time_type GetTimeStamp() const { return m_keys.GetTimeStamp(); }
	inline const KeyTable &GetKeys() const { return m_keys; }
	inline const ValueFile &GetValues() const { return m_values; }
};

} // namespace lsm
