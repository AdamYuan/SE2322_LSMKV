#include "prof.hpp"

#include <deque>
#include <matplot/matplot.h>

constexpr lsm::size_type kDataSize = 8192, kCount = 128 * 1024 * 1024 / kDataSize, kWindowSize = 2048;
const std::string kValue(kDataSize, 's');

int main() {
	std::vector<double> x(kCount);
	for (unsigned i = 0; i < kCount; ++i)
		x[i] = (i + 1) * kDataSize / 1024.0 / 1024.0;
	std::vector<double> y;
	y.reserve(kCount);

	std::deque<long double> window;

	long double sum = 0.0;
	StandardKV kv{"data"};
	kv.Reset();
	for (int i = 0; i < kCount; ++i) {
		double sec = prof_sec([&kv, i]() { kv.Put(i, kValue); });
		sum += sec;
		window.push_back(sec);
		if (window.size() > kWindowSize) {
			sum -= window.front();
			window.pop_front();
		}
		y.push_back(double((long double)window.size() / sum));
		printf("%d\n", i);
	}

	matplot::plot(x, y);
	matplot::xlabel("Total Data Size (MiB)");
	matplot::ylabel("Throughput (Put()/sec)");
	matplot::show();
}