#include <iostream>

#include "lsm/kv.hpp"

int main() {
	lsm::KV<uint64_t, std::string> lsmkv("data_dir");
	lsmkv.Reset();
	lsmkv.Put(123, "12312932189woidfjaslkafdjs");
	printf("%s\n", lsmkv.Get(123).value().c_str());
	return 0;
}
