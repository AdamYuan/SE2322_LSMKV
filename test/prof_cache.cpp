#include <iostream>

#include "prof.hpp"

#include <matplot/matplot.h>

template <typename Key> struct UncachedTrait : public StandardTrait<Key> {
	using KeyFile = lsm::KVUncachedKeyFile<Key, UncachedTrait>;
	constexpr static lsm::size_type kMaxFileSize = 2 * 1024 * 1024;
};
using UncachedKV = lsm::KV<uint64_t, std::string, UncachedTrait<uint64_t>>;

template <typename Key> struct UncachedBloomTrait : public StandardTrait<Key> {
	using KeyFile = lsm::KVUncachedBloomKeyFile<Key, UncachedBloomTrait, StandardBloom<Key>>;
	constexpr static lsm::size_type kMaxFileSize = 2 * 1024 * 1024;
};
using UncachedBloomKV = lsm::KV<uint64_t, std::string, UncachedBloomTrait<uint64_t>>;

template <typename Key> struct CachedTrait : public StandardTrait<Key> {
	using KeyFile = lsm::KVCachedKeyFile<Key, CachedTrait>;
	constexpr static lsm::size_type kMaxFileSize = 2 * 1024 * 1024;
};
using CachedKV = lsm::KV<uint64_t, std::string, CachedTrait<uint64_t>>;

constexpr lsm::size_type kDataSize = 2 * 1024, kCount = 64 * 1024 * 1024 / kDataSize;
const std::string kValue(kDataSize, 's');

struct ProfResult {
	double hit100_us, hit50_us;
};

template <typename KV> inline ProfResult prof_get_us() {
	KV kv{"data"};
	kv.Reset();
	for (auto i = 0; i < kCount; ++i)
		kv.Put(i << 1u, kValue);
	ProfResult ret = {};
	ret.hit100_us = prof_us([&kv] {
		                for (auto i = 0; i < kCount; ++i)
			                kv.Get(i << 1u);
	                }) /
	                (double)kCount;
	ret.hit50_us = prof_us([&kv] {
		               for (auto i = 0; i < kCount * 2; ++i)
			               kv.Get(i);
	               }) /
	               (double)kCount / 2.0;
	std::cout << typeid(KV).name() << " latency (100 hit, us): " << ret.hit100_us
	          << " latency (50 hit, us): " << ret.hit50_us << std::endl;
	return ret;
}

void plot(const std::vector<double> &get_us_vec, unsigned hit_rate) {
	auto x = std::vector{1, 2, 3, 4};
	auto bar = matplot::bar(x, get_us_vec);
	matplot::title("Hit Rate: " + std::to_string(hit_rate) + "%");
	matplot::ylabel("Latency (μs)");
	matplot::gca()->x_axis().ticklabels({
	    "None",
	    "Bloom",
	    "Cached",
	    "Cached+Bloom",
	});

	std::vector<double> label_x;
	std::vector<double> label_y;
	std::vector<std::string> labels;
	for (size_t i = 0; i < x.size(); ++i) {
		label_x.emplace_back(bar->x_end_point(0, i) - 0.1);
		label_y.emplace_back(get_us_vec[i] + 0.5);
		labels.emplace_back(matplot::num2str(get_us_vec[i], "%.3f"));
	}
	matplot::hold(true);
	matplot::text(label_x, label_y, labels);
	matplot::show();

	matplot::hold(false);
}

int main() {
	std::vector prof_vec = {
	    prof_get_us<UncachedKV>(),
	    prof_get_us<UncachedBloomKV>(),
	    prof_get_us<CachedKV>(),
	    prof_get_us<StandardKV>(),
	};

	{
		std::vector<double> get_us_vec;
		get_us_vec.reserve(prof_vec.size());
		for (const auto &i : prof_vec)
			get_us_vec.push_back(i.hit100_us);
		plot(get_us_vec, 100);
	}
	{
		std::vector<double> get_us_vec;
		get_us_vec.reserve(prof_vec.size());
		for (const auto &i : prof_vec)
			get_us_vec.push_back(i.hit50_us);
		plot(get_us_vec, 50);
	}
}
