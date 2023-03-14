#pragma once

#include <cstdint>
#include <iostream>
#include <string>

#include <lsm/kv.hpp>

#include "MurmurHash3.h"
template <typename Key> struct Murmur3BloomHasher {
	template <std::size_t Bits, typename Array> inline static void Insert(Array &array, const Key &key) {
		uint32_t hashes[4];
		MurmurHash3_x64_128(&key, sizeof(Key), 1, hashes);
		array[hashes[0] % Bits] = true;
		array[hashes[1] % Bits] = true;
		array[hashes[2] % Bits] = true;
		array[hashes[3] % Bits] = true;
	}
	template <std::size_t Bits, typename Array> inline static bool Exist(const Array &array, const Key &key) {
		uint32_t hashes[4];
		MurmurHash3_x64_128(&key, sizeof(Key), 1, hashes);
		return array[hashes[0] % Bits] && array[hashes[1] % Bits] && array[hashes[2] % Bits] && array[hashes[3] % Bits];
	}
};

template <typename Key, typename Value> struct MyTrait {
	using Compare = std::less<Key>;
	using SkipList = lsm::SkipList<Key, std::optional<Value>, std::default_random_engine, 1, 2, 64, Compare>;
	using Bloom = lsm::Bloom<Key, 10240 * 8, Murmur3BloomHasher<Key>>;
	using ValueIO = lsm::DefaultIO<Value>;
	constexpr static lsm::size_type kSingleFileSizeLimit = 2 * 1024 * 1024;

	constexpr static lsm::level_type kLevels = 5;
	constexpr static lsm::LevelConfig kLevelConfigs[] = {
	    {2, lsm::LevelType::kTiering},   {4, lsm::LevelType::kLeveling},  {8, lsm::LevelType::kLeveling},
	    {16, lsm::LevelType::kLeveling}, {32, lsm::LevelType::kLeveling},
	};
};

class Test {
protected:
	uint64_t nr_tests;
	uint64_t nr_passed_tests;
	uint64_t nr_phases;
	uint64_t nr_passed_phases;

#define EXPECT(exp, got) expect(exp, got, __FILE__, __LINE__)
	template <typename T>
	void expect(const std::optional<T> &exp, const std::optional<T> &got, const std::string &file, int line) {
		++nr_tests;
		if (exp == got) {
			++nr_passed_tests;
			return;
		}
		if (verbose) {
			std::cerr << "TEST Error @" << file << ":" << line;
			if (exp.has_value())
				std::cerr << ", expected " << exp.value();
			else
				std::cerr << ", expected ";

			if (got.has_value())
				std::cerr << ", got " << got.value() << std::endl;
			else
				std::cerr << ", got " << std::endl;
		}
	}

	template <typename T> void expect(const T &exp, const std::optional<T> &got, const std::string &file, int line) {
		++nr_tests;
		if (exp == got) {
			++nr_passed_tests;
			return;
		}
		if (verbose) {
			std::cerr << "TEST Error @" << file << ":" << line;
			std::cerr << ", expected " << exp;

			if (got.has_value())
				std::cerr << ", got " << got.value() << std::endl;
			else
				std::cerr << ", got " << std::endl;
		}
	}

	template <typename T> void expect(const T &exp, const T &got, const std::string &file, int line) {
		++nr_tests;
		if (exp == got) {
			++nr_passed_tests;
			return;
		}
		if (verbose) {
			std::cerr << "TEST Error @" << file << ":" << line;
			std::cerr << ", expected " << exp;

			std::cerr << ", got " << got << std::endl;
		}
	}

	void phase() {
		// Report
		std::cout << "  Phase " << (nr_phases + 1) << ": ";
		std::cout << nr_passed_tests << "/" << nr_tests << " ";

		// Count
		++nr_phases;
		if (nr_tests == nr_passed_tests) {
			++nr_passed_phases;
			std::cout << "[PASS]" << std::endl;
		} else
			std::cout << "[FAIL]" << std::endl;

		std::cout.flush();

		// Reset
		nr_tests = 0;
		nr_passed_tests = 0;
	}

	void report(void) {
		std::cout << nr_passed_phases << "/" << nr_phases << " passed.";
		std::cout << std::endl;
		std::cout.flush();

		nr_phases = 0;
		nr_passed_phases = 0;
	}

	lsm::KV<uint64_t, std::string, MyTrait<uint64_t, std::string>> store;
	bool verbose;

public:
	explicit Test(const std::string &dir, bool v = true) : store(dir), verbose(v) {
		nr_tests = 0;
		nr_passed_tests = 0;
		nr_phases = 0;
		nr_passed_phases = 0;
	}

	virtual void start_test(void *args = NULL) { std::cout << "No test is implemented." << std::endl; }
};
