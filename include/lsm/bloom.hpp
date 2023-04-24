#pragma once

#include <functional>
#include <random>

#include "detail/io.hpp"
#include "type.hpp"

namespace lsm {

template <typename Key, size_type Hashes, typename InitialHash = std::hash<Key>,
          typename Transformer = std::default_random_engine>
struct BloomDefaultHasher {
	template <size_type Bits, typename Array> inline static void Insert(Array &array, const Key &key) {
		size_type h = InitialHash{}(key);
		array[h % Bits] = true;

		Transformer trans{~h};
		std::uniform_int_distribution<size_type> distr{0, Bits - 1};
		for (size_type i = 1; i != Hashes; ++i) {
			h = distr(trans);
			array[h] = true;
		}
	}
	template <size_type Bits, typename Array> inline static bool Exist(const Array &array, const Key &key) {
		size_type h = InitialHash{}(key);
		if (array[h % Bits] == false)
			return false;

		Transformer trans{~h};
		std::uniform_int_distribution<size_type> distr{0, Bits - 1};
		for (size_type i = 1; i != Hashes; ++i)
			if (array[distr(trans)] == false)
				return false;
		return true;
	}
};

template <typename Key, size_type Bits, typename Hasher = BloomDefaultHasher<Key, 3>> class Bloom {
private:
	static constexpr size_type kU64Count = (Bits >> 6u) + ((Bits & 63u) ? 1 : 0);
	uint64_t m_bits[kU64Count]{};

	class Wrapper {
	private:
		uint64_t &m_target;
		size_type m_offset;

	public:
		inline Wrapper(uint64_t &target, size_type offset) : m_target{target}, m_offset{offset} {}
		inline Wrapper &operator=(bool b) {
			m_target |= b ? (1ULL << m_offset) : 0;
			return *this;
		}
	};

	template <typename> friend struct detail::IO;

public:
	inline bool operator[](size_type idx) const { return m_bits[idx >> 6u] & (1ULL << (idx & 63ULL)); }
	inline Wrapper operator[](size_type idx) { return {m_bits[idx >> 6u], idx & 63u}; }

	inline Bloom() = default;
	inline void Insert(const Key &key) { Hasher::template Insert<Bits>(*this, key); }
	inline bool Exist(const Key &key) const { return Hasher::template Exist<Bits>(*this, key); }
};

namespace detail {
template <typename Key, size_type Bits, typename Hasher> struct IO<Bloom<Key, Bits, Hasher>> {
	inline static constexpr size_type GetSize(const Bloom<Key, Bits, Hasher> &bloom) { return sizeof(bloom.m_bits); }
	template <typename Stream> inline static void Write(Stream &ostr, const Bloom<Key, Bits, Hasher> &bloom) {
		ostr.write((const char *)bloom.m_bits, sizeof(bloom.m_bits));
	}
	template <typename Stream> inline static Bloom<Key, Bits, Hasher> Read(Stream &istr, size_type = 0) {
		Bloom<Key, Bits, Hasher> bloom;
		istr.read((char *)bloom.m_bits, sizeof(bloom.m_bits));
		return bloom;
	}
};
} // namespace detail

} // namespace lsm
