#ifndef BLOOM_HPP
#define BLOOM_HPP

#include <bitset>
#include <functional>
#include <random>

#include "io.hpp"

namespace lsm {

template <typename Key, std::size_t Hashes, typename InitialHash = std::hash<Key>,
          typename Transformer = std::default_random_engine>
struct DefaultBloomHasher {
	template <std::size_t Bits, typename Array> inline static void Insert(Array &array, const Key &key) {
		std::size_t h = InitialHash{}(key);
		array[h % Bits] = true;

		Transformer trans{~h};
		std::uniform_int_distribution<std::size_t> distr{0, Bits - 1};
		for (std::size_t i = 1; i != Hashes; ++i) {
			h = distr(trans);
			array[h] = true;
		}
	}
	template <std::size_t Bits, typename Array> inline static bool Exist(const Array &array, const Key &key) {
		std::size_t h = InitialHash{}(key);
		if (array[h % Bits] == false)
			return false;

		Transformer trans{~h};
		std::uniform_int_distribution<std::size_t> distr{0, Bits - 1};
		for (std::size_t i = 1; i != Hashes; ++i)
			if (array[distr(trans)] == false)
				return false;
		return true;
	}
};

template <typename Key, std::size_t Bits, typename Hasher = DefaultBloomHasher<Key, 3>> class Bloom {
private:
	std::bitset<Bits> m_bits;

public:
	inline static constexpr std::size_t GetBits() { return Bits; }
	// inline static constexpr std::size_t GetBytes() { return (Bits / 8) + (Bits % 8 ? 1 : 0); }

	inline Bloom() = default;
	inline void Insert(const Key &key) { Hasher::template Insert<Bits>(m_bits, key); }
	inline bool Exist(const Key &key) const { return Hasher::template Exist<Bits>(m_bits, key); }
	inline std::bitset<Bits> &GetBitset() { return m_bits; }
	inline const std::bitset<Bits> &GetBitset() const { return m_bits; }
};

template <typename Key, std::size_t Bits, typename Hasher> struct DefaultIO<Bloom<Key, Bits, Hasher>> {
private:
	inline static constexpr std::size_t get_u64s() { return (Bits >> 6u) + ((Bits & 63u) ? 1 : 0); }

public:
	inline static constexpr std::size_t GetSize(const Bloom<Key, Bits, Hasher> &) {
		return get_u64s() << 3u; // Div 64, times 8
	}
	template <typename Stream> inline static void Write(Stream &ostr, const Bloom<Key, Bits, Hasher> &bloom) {
		for (std::size_t i = get_u64s() - 1; ~i; --i)
			DefaultIO<uint64_t>::Write(
			    ostr, ((bloom.GetBitset() >> (i << 6u)) & std::bitset<Bits>{0xffffffffffffffffULL}).to_ullong());
	}
	template <typename Stream> inline static Bloom<Key, Bits, Hasher> Read(Stream &istr, std::size_t length = 0) {
		Bloom<Key, Bits, Hasher> bloom;
		for (std::size_t i = 0; i < get_u64s(); ++i) {
			uint64_t u64 = DefaultIO<uint64_t>::Read(istr);
			bloom.GetBitset() |= u64;
			bloom.GetBitset() <<= 64u;
		}
		return bloom;
	}
};

} // namespace lsm

#endif
