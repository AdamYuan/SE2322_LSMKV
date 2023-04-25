#pragma once

#include <utility>

#include "../bloom.hpp"
#include "../type.hpp"
#include "io.hpp"
#include "kv_filesystem.hpp"

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

template <typename Key, typename Trait> class KVKeyTableBase {
private:
	using Compare = typename Trait::Compare;

protected:
	size_type m_count{};
	Key m_min{}, m_max{};

public:
	inline KVKeyTableBase() = default;
	inline KVKeyTableBase(Key min, Key max, size_type count) : m_count{count}, m_min{min}, m_max{max} {}

	inline size_type GetCount() const { return m_count; }
	inline Key GetMin() const { return m_min; }
	inline Key GetMax() const { return m_max; }

	inline bool IsMinMaxExcluded(Key key) const {
		return Compare{}(key, this->GetMin()) || Compare{}(this->GetMax(), key);
	}
	inline bool IsExtraExcluded(Key) const { return false; }
};

template <typename Derived, typename Key, typename Trait>
class KVCachedKeyTableBase : public KVKeyTableBase<Key, Trait> {
protected:
	using Compare = typename Trait::Compare;
	using KeyOffset = KVKeyOffset<Key>;

	std::unique_ptr<KeyOffset[]> m_keys;

public:
	using Index = const KeyOffset *;

	inline KVCachedKeyTableBase() = default;
	inline KVCachedKeyTableBase(std::unique_ptr<KeyOffset[]> &&keys, size_type count)
	    : KVKeyTableBase<Key, Trait>(keys[0].GetKey(), keys[count - 1].GetKey(), count) {
		m_keys = std::move(keys);
	}

	inline Index GetBegin() const { return m_keys.get(); }
	inline Index GetEnd() const { return m_keys.get() + this->m_count; }
	inline Index GetLowerBound(Key key) const {
		const KeyOffset *first = GetBegin(), *key_it;
		size_type count = this->m_count, step;
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
		if (this->IsMinMaxExcluded(key) || static_cast<const Derived *>(this)->IsExtraExcluded(key))
			return GetEnd();
		const KeyOffset *key_it = GetLowerBound(key);
		return Compare{}(key_it->GetKey(), key) || Compare{}(key, key_it->GetKey()) ? GetEnd() : key_it;
	}
	inline static KeyOffset GetKeyOffset(Index index) { return *index; }
};

template <typename Derived, typename Key, typename Trait>
class KVUncachedKeyTableBase : public KVKeyTableBase<Key, Trait> {
protected:
	using Compare = typename Trait::Compare;
	using KeyOffset = KVKeyOffset<Key>;

	KVFileSystem<Trait> *m_p_file_system{};
	std::filesystem::path m_file_path;

	mutable KeyOffset m_cached_key_offset;
	mutable size_type m_cached_index = -1;

public:
	using Index = size_type;

	inline KVUncachedKeyTableBase() = default;
	inline KVUncachedKeyTableBase(KVFileSystem<Trait> *p_file_system, std::filesystem::path file_path)
	    : m_p_file_system{p_file_system}, m_file_path{std::move(file_path)} {}
	inline KVUncachedKeyTableBase(KVFileSystem<Trait> *p_file_system, std::filesystem::path file_path, Key min, Key max,
	                              size_type count)
	    : KVKeyTableBase<Key, Trait>(min, max, count), m_p_file_system{p_file_system},
	      m_file_path{std::move(file_path)} {}

	inline Index GetBegin() const { return 0; }
	inline Index GetEnd() const { return this->m_count; }
	inline Index GetLowerBound(Key key) const {
		std::ifstream &fin = m_p_file_system->GetFileStream(m_file_path, sizeof(time_type) + Derived::GetHeaderSize());
		for (Index i = 0; i < this->m_count; ++i) {
			KeyOffset key_offset = IO<KeyOffset>::Read(fin);
			if (!Compare{}(key_offset.GetKey(), key))
				return i;
		}
		return this->m_count;
	}
	inline Index Find(Key key) const {
		if (this->IsMinMaxExcluded(key) || static_cast<const Derived *>(this)->IsExtraExcluded(key))
			return GetEnd();
		std::ifstream &fin = m_p_file_system->GetFileStream(m_file_path, sizeof(time_type) + Derived::GetHeaderSize());
		for (Index i = 0; i < this->m_count; ++i) {
			KeyOffset key_offset = IO<KeyOffset>::Read(fin);
			if (!Compare{}(key_offset.GetKey(), key))
				return Compare{}(key, key_offset.GetKey()) ? this->m_count : i;
		}
		return this->m_count;
	}
	inline KeyOffset GetKeyOffset(Index index) const {
		if (m_cached_index == index)
			return m_cached_key_offset;
		m_cached_index = index;
		m_cached_key_offset = IO<KeyOffset>::Read(m_p_file_system->GetFileStream(
		    m_file_path, sizeof(time_type) + Derived::GetHeaderSize() + index * (sizeof(KeyOffset))));
		return m_cached_key_offset;
	}
};

template <typename Key, typename Trait>
class KVKeyBuffer final : public KVCachedKeyTableBase<KVKeyBuffer<Key, Trait>, Key, Trait> {
private:
	using KeyOffset = KVKeyOffset<Key>;
	using Compare = typename Trait::Compare;
	using Base = KVCachedKeyTableBase<KVKeyBuffer, Key, Trait>;

	template <typename, typename, typename> friend class KVCachedBloomKeyFile;
	template <typename, typename> friend class KVCachedKeyFile;
	template <typename, typename, typename> friend class KVUncachedBloomKeyFile;
	template <typename, typename> friend class KVUncachedKeyFile;

public:
	inline KVKeyBuffer() = default;
	inline KVKeyBuffer(std::unique_ptr<KeyOffset[]> &&keys, size_type count) : Base{std::move(keys), count} {}
};

template <typename Derived, typename Key> class KVKeyFileBase {
protected:
public:
	inline constexpr size_type GetSize() const {
		return Derived::GetHeaderSize() + sizeof(KVKeyOffset<Key>) * static_cast<const Derived *>(this)->GetCount();
	}
};

template <typename Key, typename Trait>
class KVUncachedKeyFile final : public KVUncachedKeyTableBase<KVUncachedKeyFile<Key, Trait>, Key, Trait>,
                                public KVKeyFileBase<KVUncachedKeyFile<Key, Trait>, Key> {
private:
	using Compare = typename Trait::Compare;

public:
	inline KVUncachedKeyFile() = default;
	template <typename Stream>
	inline KVUncachedKeyFile(Stream &ostr, KVKeyBuffer<Key, Trait> &&key_buffer, KVFileSystem<Trait> *p_file_system,
	                         const std::filesystem::path &file_path)
	    : KVUncachedKeyTableBase<KVUncachedKeyFile, Key, Trait>(p_file_system, file_path, key_buffer.GetMin(),
	                                                            key_buffer.GetMax(), key_buffer.GetCount()) {
		IO<size_type>::Write(ostr, this->m_count);
		IO<Key>::Write(ostr, this->m_min);
		IO<Key>::Write(ostr, this->m_max);
		ostr.write((const char *)key_buffer.m_keys.get(), this->m_count * sizeof(KVKeyOffset<Key>));
	}

	template <typename Stream>
	inline KVUncachedKeyFile(Stream &istr, KVFileSystem<Trait> *p_file_system, const std::filesystem::path &file_path)
	    : KVUncachedKeyTableBase<KVUncachedKeyFile, Key, Trait>(p_file_system, file_path) {
		this->m_count = IO<size_type>::Read(istr);
		this->m_min = IO<Key>::Read(istr);
		this->m_max = IO<Key>::Read(istr);
	}

	inline static constexpr size_type GetHeaderSize() { return sizeof(size_type) + sizeof(Key) * 2; }
};

template <typename Key, typename Trait, typename Bloom>
class KVUncachedBloomKeyFile final
    : public KVUncachedKeyTableBase<KVUncachedBloomKeyFile<Key, Trait, Bloom>, Key, Trait>,
      public KVKeyFileBase<KVUncachedBloomKeyFile<Key, Trait, Bloom>, Key> {
private:
	using Compare = typename Trait::Compare;

	Bloom m_bloom;

public:
	inline KVUncachedBloomKeyFile() = default;
	template <typename Stream>
	inline KVUncachedBloomKeyFile(Stream &ostr, KVKeyBuffer<Key, Trait> &&key_buffer,
	                              KVFileSystem<Trait> *p_file_system, const std::filesystem::path &file_path)
	    : KVUncachedKeyTableBase<KVUncachedBloomKeyFile, Key, Trait>(p_file_system, file_path, key_buffer.GetMin(),
	                                                                 key_buffer.GetMax(), key_buffer.GetCount()) {
		for (size_type i = 0; i < this->m_count; ++i)
			m_bloom.Insert(key_buffer.m_keys[i].GetKey());
		IO<size_type>::Write(ostr, this->m_count);
		IO<Key>::Write(ostr, this->m_min);
		IO<Key>::Write(ostr, this->m_max);
		IO<Bloom>::Write(ostr, m_bloom);
		ostr.write((const char *)key_buffer.m_keys.get(), this->m_count * sizeof(KVKeyOffset<Key>));
	}

	template <typename Stream>
	inline KVUncachedBloomKeyFile(Stream &istr, KVFileSystem<Trait> *p_file_system,
	                              const std::filesystem::path &file_path)
	    : KVUncachedKeyTableBase<KVUncachedBloomKeyFile, Key, Trait>(p_file_system, file_path) {
		this->m_count = IO<size_type>::Read(istr);
		this->m_min = IO<Key>::Read(istr);
		this->m_max = IO<Key>::Read(istr);
		this->m_bloom = IO<Bloom>::Read(istr);
	}

	inline bool IsExtraExcluded(Key key) const { return !m_bloom.Exist(key); }
	inline static constexpr size_type GetHeaderSize() {
		return sizeof(size_type) + sizeof(Key) * 2 + IO<Bloom>::GetSize({});
	}
};

template <typename Key, typename Trait, typename Bloom>
class KVCachedBloomKeyFile final : public KVCachedKeyTableBase<KVCachedBloomKeyFile<Key, Trait, Bloom>, Key, Trait>,
                                   public KVKeyFileBase<KVCachedBloomKeyFile<Key, Trait, Bloom>, Key> {
private:
	using Compare = typename Trait::Compare;

	Bloom m_bloom;

public:
	inline KVCachedBloomKeyFile() = default;

	template <typename Stream>
	inline KVCachedBloomKeyFile(Stream &ostr, KVKeyBuffer<Key, Trait> &&key_buffer, KVFileSystem<Trait> *,
	                            const std::filesystem::path &)
	    : KVCachedKeyTableBase<KVCachedBloomKeyFile, Key, Trait>(std::move(key_buffer.m_keys), key_buffer.GetCount()) {
		for (size_type i = 0; i < this->m_count; ++i)
			m_bloom.Insert(this->m_keys[i].GetKey());
		IO<size_type>::Write(ostr, this->m_count);
		IO<Key>::Write(ostr, this->m_min);
		IO<Key>::Write(ostr, this->m_max);
		IO<Bloom>::Write(ostr, m_bloom);
		ostr.write((const char *)this->m_keys.get(), this->m_count * sizeof(KVKeyOffset<Key>));
	}
	template <typename Stream>
	inline KVCachedBloomKeyFile(Stream &istr, KVFileSystem<Trait> *, const std::filesystem::path &) {
		this->m_count = IO<size_type>::Read(istr);
		this->m_min = IO<Key>::Read(istr);
		this->m_max = IO<Key>::Read(istr);
		this->m_bloom = IO<Bloom>::Read(istr);
		this->m_keys = std::unique_ptr<KVKeyOffset<Key>[]>(new KVKeyOffset<Key>[this->m_count]);
		istr.read((char *)this->m_keys.get(), this->m_count * sizeof(KVKeyOffset<Key>));
	}
	inline bool IsExtraExcluded(Key key) const { return !m_bloom.Exist(key); }
	inline static constexpr size_type GetHeaderSize() {
		return sizeof(size_type) + sizeof(Key) * 2 + IO<Bloom>::GetSize({});
	}
};

template <typename Key, typename Trait>
class KVCachedKeyFile final : public KVCachedKeyTableBase<KVCachedKeyFile<Key, Trait>, Key, Trait>,
                              public KVKeyFileBase<KVCachedKeyFile<Key, Trait>, Key> {
private:
	using Compare = typename Trait::Compare;

public:
	inline KVCachedKeyFile() = default;

	template <typename Stream>
	inline KVCachedKeyFile(Stream &ostr, KVKeyBuffer<Key, Trait> &&key_buffer, KVFileSystem<Trait> *,
	                       const std::filesystem::path &)
	    : KVCachedKeyTableBase<KVCachedKeyFile, Key, Trait>(std::move(key_buffer.m_keys), key_buffer.GetCount()) {
		IO<size_type>::Write(ostr, this->m_count);
		IO<Key>::Write(ostr, this->m_min);
		IO<Key>::Write(ostr, this->m_max);
		ostr.write((const char *)this->m_keys.get(), this->m_count * sizeof(KVKeyOffset<Key>));
	}

	template <typename Stream>
	inline KVCachedKeyFile(Stream &istr, KVFileSystem<Trait> *, const std::filesystem::path &) {
		this->m_count = IO<size_type>::Read(istr);
		this->m_min = IO<Key>::Read(istr);
		this->m_max = IO<Key>::Read(istr);
		this->m_keys = std::unique_ptr<KVKeyOffset<Key>[]>(new KVKeyOffset<Key>[this->m_count]);
		istr.read((char *)this->m_keys.get(), this->m_count * sizeof(KVKeyOffset<Key>));
	}

	inline static constexpr size_type GetHeaderSize() { return sizeof(size_type) + sizeof(Key) * 2; }
};

} // namespace lsm::detail