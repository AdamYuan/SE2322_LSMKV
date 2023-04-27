#include <iostream>

#include "prof.hpp"

#include <matplot/matplot.h>

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
			double put_avg_sec = prof_sec([count, &kv, &value]() {
				                     for (auto i = 0; i < count; ++i) {
					                     kv.Put(i, value);
				                     }
			                     }) /
			                     (double)count;
			printf("PUT: %.20lf prof_sec\n", put_avg_sec);
			put_us_vec.push_back(put_avg_sec * 1000000.0);
			put_tp_vec.push_back(1.0 / put_avg_sec);
		}
		{
			double get_seq_avg_sec = prof_sec([count, &kv]() {
				                         for (auto i = 0; i < count; ++i) {
					                         kv.Get(i);
				                         }
			                         }) /
			                         (double)count;
			printf("GET(SEQ): %.20lf prof_sec\n", get_seq_avg_sec);
			get_seq_us_vec.push_back(get_seq_avg_sec * 1000000.0);
			get_seq_tp_vec.push_back(1.0 / get_seq_avg_sec);
		}
		{
			std::vector<int> samp(count);
			for (int i = 0; i < count; ++i)
				samp[i] = i;
			std::shuffle(samp.begin(), samp.end(), std::mt19937{});

			double get_rnd_avg_sec = prof_sec([count, &samp, &kv]() {
				                         for (auto i = 0; i < count; ++i) {
					                         kv.Get(samp[i]);
				                         }
			                         }) /
			                         (double)count;
			printf("GET(RND): %.20lf prof_sec\n", get_rnd_avg_sec);
			get_rnd_us_vec.push_back(get_rnd_avg_sec * 1000000.0);
			get_rnd_tp_vec.push_back(1.0 / get_rnd_avg_sec);
		}
		{
			double del_avg_sec = prof_sec([count, &kv]() {
				                     for (auto i = 0; i < count; ++i) {
					                     kv.Delete(i);
				                     }
			                     }) /
			                     (double)count;
			printf("DEL: %.20lf prof_sec\n", del_avg_sec);
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

	return 0;
}