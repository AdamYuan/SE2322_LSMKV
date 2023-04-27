#include <iostream>

#include "prof.hpp"

#include <matplot/matplot.h>

template <typename Key> struct UncachedTrait : public StandardTrait<Key> {
	using KeyFile = lsm::KVUncachedKeyFile<Key, UncachedTrait>;
	constexpr static lsm::size_type kMaxFileSize = 2 * 1024 * 1024 - 10240;
};
using UncachedKV = lsm::KV<uint64_t, std::string, UncachedTrait<uint64_t>>;

template <typename Key> struct CachedTrait : public StandardTrait<Key> {
	using KeyFile = lsm::KVCachedKeyFile<Key, CachedTrait>;
	constexpr static lsm::size_type kMaxFileSize = 2 * 1024 * 1024 - 10240;
};
using CachedKV = lsm::KV<uint64_t, std::string, CachedTrait<uint64_t>>;

constexpr lsm::size_type kDataSize = 2 * 1024, kCount = 64 * 1024 * 1024 / kDataSize;
const std::string kValue(kDataSize, 's');

template <typename KV> inline double prof_get_us() {
	KV kv{"data"};
	for (auto i = 0; i < kCount; ++i)
		kv.Put(i, kValue);
	double us = prof_us([&kv] {
		            for (auto i = 0; i < kCount; ++i)
			            kv.Get(i);
	            }) /
	            (double)kCount;
	kv.Reset();
	std::cout << typeid(KV).name() << " latency (us): " << us << std::endl;
	return us;
}

int main() {
	std::vector<double> get_us_vec = {
	    prof_get_us<UncachedKV>(),
	    prof_get_us<CachedKV>(),
	    prof_get_us<StandardKV>(),
	};

	auto x = std::vector{1, 2, 3};
	auto bar = matplot::bar(x, get_us_vec);
	matplot::ylabel("Latency (μs)");
	matplot::gca()->x_axis().ticklabels({
	    "Uncached",
	    "Cached",
	    "Cached + Bloom",
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
