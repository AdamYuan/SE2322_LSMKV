#ifndef BLOOM_HPP
#define BLOOM_HPP

#include <bitset>
#include <functional>
#include <random>

#include "io.hpp"
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
	std::bitset<Bits> m_bits;

public:
	inline static constexpr size_type GetBits() { return Bits; }
	// inline static constexpr size_type GetBytes() { return (Bits / 8) + (Bits % 8 ? 1 : 0); }

	inline Bloom() = default;
	inline void Insert(const Key &key) { Hasher::template Insert<Bits>(m_bits, key); }
	inline bool Exist(const Key &key) const { return Hasher::template Exist<Bits>(m_bits, key); }
	inline std::bitset<Bits> &GetBitset() { return m_bits; }
	inline const std::bitset<Bits> &GetBitset() const { return m_bits; }
};

template <typename Key, size_type Bits, typename Hasher> struct DefaultIO<Bloom<Key, Bits, Hasher>> {
private:
	inline static constexpr size_type get_u64s() { return (Bits >> 6u) + ((Bits & 63u) ? 1 : 0); }

public:
	inline static constexpr size_type GetSize(const Bloom<Key, Bits, Hasher> &) {
		return get_u64s() << 3u; // times 8
	}
	template <typename Stream> inline static void Write(Stream &ostr, const Bloom<Key, Bits, Hasher> &bloom) {
		for (size_type i = 0; i < get_u64s(); ++i) {
			uint64_t u64 = ((bloom.GetBitset() >> (i << 6u)) & std::bitset<Bits>(0xffffffffffffffffULL)).to_ullong();
			DefaultIO<uint64_t>::Write(ostr, u64);
		}
	}
	template <typename Stream> inline static Bloom<Key, Bits, Hasher> Read(Stream &istr, size_type = 0) {
		Bloom<Key, Bits, Hasher> bloom{};
		for (size_type i = 0; i < get_u64s(); ++i) {
			uint64_t u64 = DefaultIO<uint64_t>::Read(istr);
			bloom.GetBitset() |= (std::bitset<Bits>(u64) << (i << 6u));
		}
		return bloom;
	}
};

} // namespace lsm

#endif
