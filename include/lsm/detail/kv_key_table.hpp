#pragma once

#include "../bloom.hpp"
#include "../type.hpp"
#include "io.hpp"

namespace lsm::detail {

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

template <typename Key> class KVKeyTableBase {
protected:
	size_type m_count{};
	Key m_min, m_max;

public:
	inline KVKeyTableBase() = default;
	inline KVKeyTableBase(Key min, Key max, size_type count) : m_count{count}, m_min{min}, m_max{max} {}

	inline size_type GetCount() const { return m_count; }
	inline Key GetMin() const { return m_min; }
	inline Key GetMax() const { return m_max; }
};

template <typename Derived, typename Key, typename Compare> class KVCachedKeyTableBase : public KVKeyTableBase<Key> {
protected:
	using KeyOffset = KVKeyOffset<Key>;
	using Base = KVKeyTableBase<Key>;

	std::unique_ptr<KeyOffset[]> m_keys;

public:
	using Index = const KeyOffset *;

	inline KVCachedKeyTableBase() = default;
	inline KVCachedKeyTableBase(std::unique_ptr<KeyOffset[]> &&keys, size_type count)
	    : Base(keys[0].GetKey(), keys[count - 1].GetKey(), count) {
		m_keys = std::move(keys);
	}

	inline Index GetBegin() const { return m_keys.get(); }
	inline Index GetEnd() const { return m_keys.get() + Base::m_count; }
	inline Index GetLowerBound(Key key) const {
		const KeyOffset *first = GetBegin(), *key_it;
		size_type count = Base::m_count, step;
		while (count > 0) {
			step = count >> 1u;
			key_it = first + step;
			if (Compare{}(key_it->GetKey(), key)) {
				first = ++key_it;
				count -= step + 1;
			} else
				count = step;
		}
		return key_it;
	}
	inline Index Find(Key key) const {
		if (static_cast<const Derived *>(this)->IsExcluded(key))
			return GetEnd();
		const KeyOffset *key_it = GetLowerBound(key);
		return Compare{}(key_it->GetKey(), key) || Compare{}(key, key_it->GetKey()) ? GetEnd() : key_it;
	}
	inline static KeyOffset GetKeyOffset(Index index) { return *index; }
};

template <typename Key, typename Trait>
class KVKeyBuffer final : public KVCachedKeyTableBase<KVKeyBuffer<Key, Trait>, Key, typename Trait::Compare> {
private:
	using KeyOffset = KVKeyOffset<Key>;
	using Compare = typename Trait::Compare;
	using Base = KVCachedKeyTableBase<KVKeyBuffer, Key, Compare>;

	template <typename, typename, typename> friend class KVKeyFile;

public:
	inline KVKeyBuffer() = default;
	inline KVKeyBuffer(std::unique_ptr<KeyOffset[]> &&keys, size_type count) : Base{std::move(keys), count} {}
	inline bool IsExcluded(Key key) const { return Compare{}(key, Base::GetMin()) || Compare{}(Base::GetMax(), key); }
};

template <typename Key, typename Trait, typename Bloom>
class KVKeyFile final : public KVCachedKeyTableBase<KVKeyFile<Key, Trait, Bloom>, Key, typename Trait::Compare> {
private:
	using Compare = typename Trait::Compare;
	using Base = KVCachedKeyTableBase<KVKeyFile, Key, Compare>;

	Bloom m_bloom;

	template <typename> friend class IO;

public:
	inline KVKeyFile() = default;
	inline explicit KVKeyFile(KVKeyBuffer<Key, Trait> &&key_buffer)
	    : Base(std::move(key_buffer.m_keys), key_buffer.GetCount()) {
		for (size_type i = 0; i < Base::m_count; ++i)
			m_bloom.Insert(Base::m_keys[i].GetKey());
	}
	inline bool IsExcluded(Key key) const {
		return Compare{}(key, Base::GetMin()) || Compare{}(Base::GetMax(), key) || !m_bloom.Exist(key);
	}
};

template <typename Key, typename Trait, typename Bloom> struct IO<KVKeyFile<Key, Trait, Bloom>> {
	inline static constexpr size_type GetSize() { return sizeof(size_type) + sizeof(Key) * 2 + IO<Bloom>::GetSize({}); }
	inline static constexpr size_type GetSize(const KVKeyFile<Key, Trait, Bloom> &keys) {
		return sizeof(size_type) + sizeof(Key) * 2 + IO<Bloom>::GetSize({}) +
		       sizeof(KVKeyOffset<Key>) * keys.GetCount();
	}
	template <typename Stream> inline static void Write(Stream &ostr, const KVKeyFile<Key, Trait, Bloom> &keys) {
		IO<size_type>::Write(ostr, keys.m_count);
		IO<Key>::Write(ostr, keys.m_min);
		IO<Key>::Write(ostr, keys.m_max);
		IO<Bloom>::Write(ostr, keys.m_bloom);
		ostr.write((const char *)keys.m_keys.get(), keys.m_count * sizeof(KVKeyOffset<Key>));
	}
	template <typename Stream> inline static KVKeyFile<Key, Trait, Bloom> Read(Stream &istr, size_type = -1) {
		KVKeyFile<Key, Trait, Bloom> table = {};
		table.m_count = IO<size_type>::Read(istr);
		table.m_min = IO<Key>::Read(istr);
		table.m_max = IO<Key>::Read(istr);
		table.m_bloom = IO<Bloom>::Read(istr);
		table.m_keys = std::unique_ptr<KVKeyOffset<Key>[]>(new KVKeyOffset<Key>[table.m_count]);
		istr.read((char *)table.m_keys.get(), table.m_count * sizeof(KVKeyOffset<Key>));
		return table;
	}
};

} // namespace lsm::detail