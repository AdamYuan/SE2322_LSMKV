#include <iostream>

#include "prof.hpp"

#include <matplot/matplot.h>

template <typename Key, lsm::size_type TieringSize, lsm::size_type SizeRatio>
struct LevelTrait : public StandardTrait<Key> {
	using KeyFile = lsm::KVCachedBloomKeyFile<Key, LevelTrait, lsm::Bloom<Key, 10240 * 8, Murmur3BloomHasher<Key>>>;
	inline constexpr static lsm::KVLevelConfig kLevelConfigs[] = {
	    {TieringSize, lsm::KVLevelType::kTiering},
	    {TieringSize * SizeRatio, lsm::KVLevelType::kLeveling},
	    {TieringSize * SizeRatio * SizeRatio, lsm::KVLevelType::kLeveling},
	    {TieringSize * SizeRatio * SizeRatio * SizeRatio, lsm::KVLevelType::kLeveling},
	    {TieringSize * SizeRatio * SizeRatio * SizeRatio * SizeRatio, lsm::KVLevelType::kLeveling},
	};
};

constexpr lsm::size_type kDataSize = 8 * 1024, kCount = 128 * 1024 * 1024 / kDataSize;
const std::string kValue(kDataSize, 's');

struct ProfResult {
	double put_us, get_us;
};

template <lsm::size_type TieringSize, lsm::size_type SizeRatio> ProfResult prof_level_config() {
	printf("TS = %d, Ratio = %d\n", TieringSize, SizeRatio);
	using KV = lsm::KV<uint64_t, std::string, LevelTrait<uint64_t, TieringSize, SizeRatio>>;
	KV kv{"data"};
	kv.Reset();

	ProfResult ret = {};
	ret.put_us = prof_us([&kv]() {
		             for (auto i = 0; i < kCount; ++i)
			             kv.Put(i, kValue);
	             }) /
	             (double)kCount;
	printf("PUT: %.20lf us\n", ret.put_us);
	ret.get_us = prof_us([&kv]() {
		             for (auto i = 0; i < kCount; ++i)
			             kv.Get(i);
	             }) /
	             (double)kCount;
	printf("GET: %.20lf us\n", ret.get_us);

	return ret;
}

template <lsm::size_type MaxTieringSize, lsm::size_type MaxSizeRatio, lsm::size_type TieringSize = 1,
          lsm::size_type SizeRatio = 1>
void fill_prof_results(ProfResult results[MaxTieringSize + 1][MaxSizeRatio + 1]) {
	results[TieringSize][SizeRatio] = prof_level_config<TieringSize, SizeRatio>();
	if constexpr (TieringSize < MaxTieringSize)
		fill_prof_results<MaxTieringSize, MaxSizeRatio, TieringSize + 1, SizeRatio>(results);
	else if constexpr (SizeRatio < MaxSizeRatio)
		fill_prof_results<MaxTieringSize, MaxSizeRatio, 1, SizeRatio + 1>(results);
}

int main() {
	constexpr lsm::size_type kMaxTieringSize = 5, kMaxSizeRatio = 5;
	ProfResult prof_results[kMaxTieringSize + 1][kMaxSizeRatio + 1]{};
	fill_prof_results<kMaxTieringSize, kMaxSizeRatio>(prof_results);

	std::vector<std::vector<double>> put_map(kMaxTieringSize), get_map(kMaxTieringSize), del_map(kMaxTieringSize);
	for (auto i = 0; i < kMaxTieringSize; ++i) {
		for (auto j = 1; j <= kMaxSizeRatio; ++j) {
			put_map[i].push_back(prof_results[i + 1][j].put_us);
			get_map[i].push_back(prof_results[i + 1][j].get_us);
		}
	}

	matplot::ylabel("Tiering Level Size");
	matplot::xlabel("Size Ratio");

	matplot::heatmap(put_map);
	matplot::title("Put() Latency (μs)");
	matplot::show();

	matplot::heatmap(get_map);
	matplot::title("Get() Latency (μs)");
	matplot::show();
}