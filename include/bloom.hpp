#ifndef BLOOM_HPP
#define BLOOM_HPP

#include <bitset>
#include <functional>
#include <random>

namespace lsm {

template <typename Key, std::size_t Bits, std::size_t Hashes, typename Hasher = std::hash<Key>,
          typename Transformer = std::default_random_engine>
class Bloom {
private:
	std::bitset<Bits> m_bits;
	Hasher m_hasher;

public:
	static_assert(Hashes >= 1 && Bits >= 1);
	inline static constexpr std::size_t GetBits() { return Bits; }
	inline static constexpr std::size_t GetBytes() { return (Bits / 8) + (Bits % 8 ? 1 : 0); }

	inline Bloom(Hasher hasher = Hasher()) : m_hasher{hasher} {}
	inline void Insert(const Key &key) {
		std::size_t h = m_hasher(key);
		m_bits[h % Bits] = true;

		Transformer trans{~h};
		std::uniform_int_distribution<std::size_t> distr{0, Bits - 1};
		for (std::size_t i = 1; i != Hashes; ++i) {
			h = distr(trans);
			m_bits[h] = true;
		}
	}
	inline bool Exist(const Key &key) {
		std::size_t h = m_hasher(key);
		if (m_bits[h % Bits] == false)
			return false;

		Transformer trans{~h};
		std::uniform_int_distribution<std::size_t> distr{0, Bits - 1};
		for (std::size_t i = 1; i != Hashes; ++i)
			if (m_bits[distr(trans)] == false)
				return false;
		return true;
	}
};

} // namespace lsm

#endif
