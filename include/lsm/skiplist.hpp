#ifndef SKIPLIST_HPP
#define SKIPLIST_HPP

#include <cinttypes>
#include <optional>
#include <random>

#include "type.hpp"

namespace lsm {

template <typename Key, typename Value, typename RandomGenerator = std::default_random_engine, //
          uint32_t Prob = 1u, uint32_t ProbDiv = 2u, uint32_t MaxLevel = 64, typename Compare = std::less<Key>>
class SkipList {
private:
	RandomGenerator m_rand_gen;
	level_type m_level;
	size_type m_size;

#pragma pack(push, 1)
	struct Node;
	struct Block {
		Node **forward;
		level_type level;
		inline explicit Block(level_type level) : level{level}, forward{new Node *[level]()} {}
		inline void Clear() {
			for (level_type i = 0; i < level; ++i)
				if (forward[i] && (i == 0 || forward[i] != forward[i - 1]) && level >= forward[i]->blk.level)
					delete forward[i];
			std::fill(forward, forward + level, nullptr);
		}
		inline ~Block() {
			Clear();
			delete[] forward;
		}
	} m_head;
	struct Node {
		Block blk;
		Key key;
		Value value;
		inline Node(Key &&key, Value &&value, level_type level)
		    : key{std::move(key)}, value{std::move(value)}, blk{level} {}
		inline ~Node() = default;
	};
#pragma pack(pop)

	inline bool forward_key_less(const Block *blk, level_type l, const Key &key) const {
		return blk->forward[l] && Compare{}(blk->forward[l]->key, key);
	}
	inline bool key_equal(const Key &l, const Key &r) const { return !Compare{}(l, r) && !Compare{}(r, l); }

	inline level_type random_level() {
		std::uniform_int_distribution<uint32_t> distr{0, ProbDiv - 1};
		level_type level = 1;
		while (level != MaxLevel && distr(m_rand_gen) < Prob)
			++level;
		return level;
	}

	template <typename Func> inline void for_each_impl(Func &&func) const {
		for (const Node *node = m_head.forward[0]; node; node = node->blk.forward[0])
			func(node->key, node->value);
	}

	inline const Node *search_impl(const Key &key) const {
		const Block *blk = &m_head;
		for (level_type l = m_level - 1; ~l; --l)
			while (forward_key_less(blk, l, key))
				blk = &blk->forward[l]->blk;
		const Node *node = blk->forward[0];
		return node && key_equal(node->key, key) ? node : nullptr;
	}

	template <typename Replacer> inline bool replace_impl(Key &&key, Replacer &&replacer) {
		Block *prev_blk[MaxLevel];

		{ // Search and set prev_blk array
			Block *blk = &m_head;
			for (level_type l = m_level - 1; ~l; --l) {
				while (forward_key_less(blk, l, key))
					blk = &blk->forward[l]->blk;
				prev_blk[l] = blk;
			}
			Node *node = blk->forward[0];
			if (node && key_equal(node->key, key)) {
				return replacer(&(node->value), true);
			}
		}

		Value val{};
		if (replacer(&val, false)) {
			level_type ins_level = random_level();
			for (; m_level < ins_level; ++m_level)
				prev_blk[m_level] = &m_head;

			Node *ins_node = new Node{std::move(key), std::move(val), ins_level};
			for (level_type l = 0; l != ins_level; ++l) {
				ins_node->blk.forward[l] = prev_blk[l]->forward[l];
				prev_blk[l]->forward[l] = ins_node;
			}
			++m_size;
			return true;
		}
		return false;
	}

	inline void insert_impl(Key &&key, Value &&value) {
		Block *prev_blk[MaxLevel];

		{ // Search and set prev_blk array
			Block *blk = &m_head;
			for (level_type l = m_level - 1; ~l; --l) {
				while (forward_key_less(blk, l, key))
					blk = &blk->forward[l]->blk;
				prev_blk[l] = blk;
			}
			Node *node = blk->forward[0];
			if (node && key_equal(node->key, key)) {
				node->value = std::move(value);
				return;
			}
		}

		level_type ins_level = random_level();
		for (; m_level < ins_level; ++m_level)
			prev_blk[m_level] = &m_head;

		Node *ins_node = new Node{std::move(key), std::move(value), ins_level};
		for (level_type l = 0; l != ins_level; ++l) {
			ins_node->blk.forward[l] = prev_blk[l]->forward[l];
			prev_blk[l]->forward[l] = ins_node;
		}
		++m_size;
	}

public:
	inline explicit SkipList(typename RandomGenerator::result_type seed = 0)
	    : m_rand_gen{seed}, m_head{MaxLevel}, m_level{0}, m_size{0} {}
	inline ~SkipList() = default;

	inline void Clear() {
		m_head.Clear();
		m_level = 0;
		m_size = 0;
	}

	inline std::optional<Value> Search(const Key &key) const {
		const Node *node = search_impl(key);
		return node ? node->value : std::optional<Value>{};
	}
	inline void Insert(Key &&key, Value &&value) { insert_impl(std::move(key), std::move(value)); }
	inline void Insert(Key &&key, const Value &value) { insert_impl(std::move(key), Value(value)); }
	inline void Insert(const Key &key, Value &&value) { insert_impl(Key(key), std::move(value)); }
	inline void Insert(const Key &key, const Value &value) { insert_impl(Key(key), Value(value)); }
	template <typename Replacer> inline bool Replace(Key &&key, Replacer &&replacer) {
		return replace_impl(std::move(key), std::forward<Replacer>(replacer));
	}
	template <typename Replacer> inline bool Replace(const Key &key, Replacer &&replacer) {
		return replace_impl(Key(key), std::forward<Replacer>(replacer));
	}
	inline size_type GetSize() const { return m_size; }
	inline level_type GetLevel() const { return m_level; }
	template <typename Func> inline void ForEach(Func &&func) const { for_each_impl(std::forward<Func>(func)); }
};

} // namespace lsm

#endif
