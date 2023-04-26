#include <lsm/kv.hpp>

#include "prof.hpp"

#include "MurmurHash3.h"
#include <matplot/matplot.h>

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

template <typename Key> struct StandardTrait : public lsm::KVDefaultTrait<Key, std::string> {
	using Compare = std::less<Key>;
	using Container = lsm::SkipList<Key, lsm::KVMemValue<std::string>, Compare, std::default_random_engine, 1, 2, 32>;
	using KeyFile = lsm::KVCachedBloomKeyFile<Key, StandardTrait, lsm::Bloom<Key, 10240 * 8, Murmur3BloomHasher<Key>>>;
	constexpr static lsm::size_type kMaxFileSize = 2 * 1024 * 1024;

	constexpr static lsm::level_type kLevels = 5;
	constexpr static lsm::KVLevelConfig kLevelConfigs[] = {{2, lsm::KVLevelType::kTiering},
	                                                       {4, lsm::KVLevelType::kLeveling},
	                                                       {8, lsm::KVLevelType::kLeveling},
	                                                       {16, lsm::KVLevelType::kLeveling},
	                                                       {32, lsm::KVLevelType::kLeveling}};
};

using StandardKV = lsm::KV<uint64_t, std::string, StandardTrait<uint64_t>>;

int main() {
	constexpr lsm::size_type kDataSize[] = {2 * 1024, 4 * 1024, 6 * 1024, 8 * 1024};

	std::vector<double> put_us_vec, get_seq_us_vec, get_rnd_us_vec, del_us_vec;
	std::vector<double> put_tp_vec, get_seq_tp_vec, get_rnd_tp_vec, del_tp_vec;

	StandardKV kv{"data"};
	for (lsm::size_type sz : kDataSize) {
		kv.Reset();

		std::string value(sz, 's');

		lsm::size_type count = 128 * 1024 * 1024 / sz;

		printf("SZ = %u, CNT = %u\n", sz, count);
		{
			double put_avg_sec = sec([count, &kv, &value]() {
				                     for (auto i = 0; i < count; ++i) {
					                     kv.Put(i, value);
				                     }
			                     }) /
			                     (double)count;
			printf("PUT: %.20lf sec\n", put_avg_sec);
			put_us_vec.push_back(put_avg_sec * 1000000.0);
			put_tp_vec.push_back(1.0 / put_avg_sec);
		}
		{
			double get_seq_avg_sec = sec([count, &kv]() {
				                         for (auto i = 0; i < count; ++i) {
					                         kv.Get(i);
				                         }
			                         }) /
			                         (double)count;
			printf("GET(SEQ): %.20lf sec\n", get_seq_avg_sec);
			get_seq_us_vec.push_back(get_seq_avg_sec * 1000000.0);
			get_seq_tp_vec.push_back(1.0 / get_seq_avg_sec);
		}
		{
			std::vector<int> samp(count);
			for (int i = 0; i < count; ++i)
				samp[i] = i;
			std::shuffle(samp.begin(), samp.end(), std::mt19937{});

			double get_rnd_avg_sec = sec([count, &samp, &kv]() {
				                         for (auto i = 0; i < count; ++i) {
					                         kv.Get(samp[i]);
				                         }
			                         }) /
			                         (double)count;
			printf("GET(RND): %.20lf sec\n", get_rnd_avg_sec);
			get_rnd_us_vec.push_back(get_rnd_avg_sec * 1000000.0);
			get_rnd_tp_vec.push_back(1.0 / get_rnd_avg_sec);
		}
		{
			double del_avg_sec = sec([count, &kv]() {
				                     for (auto i = 0; i < count; ++i) {
					                     kv.Delete(i);
				                     }
			                     }) /
			                     (double)count;
			printf("DEL: %.20lf sec\n", del_avg_sec);
			del_us_vec.push_back(del_avg_sec * 1000000.0);
			del_tp_vec.push_back(1.0 / del_avg_sec);
		}
	}

	std::vector<double> x(std::size(kDataSize));
	for (int i = 0; i < std::size(kDataSize); ++i)
		x[i] = kDataSize[i] / 1024.0;
	std::vector<std::vector<double>> us_y = {put_us_vec, get_seq_us_vec, get_rnd_us_vec, del_us_vec};
	std::vector<std::vector<double>> tp_y = {put_tp_vec, get_seq_tp_vec, get_rnd_tp_vec, del_tp_vec};

	matplot::bar(x, us_y);
	matplot::legend({"Put", "Get (SEQ)", "Get (RAND)", "Delete"});
	matplot::xlabel("Data Size (KiB)");
	matplot::ylabel("Latency (μs)");
	matplot::show();

	matplot::bar(x, tp_y);
	matplot::legend({"Put", "Get (SEQ)", "Get (RAND)", "Delete"});
	matplot::xlabel("Data Size (KiB)");
	matplot::ylabel("Throughput (call/sec)");
	matplot::show();

	// matplot::show();
	return 0;
}