#include <cstdint>
#include <iostream>
#include <string>

#include "test.hpp"

class CorrectnessTest : public Test {
private:
	const uint64_t SIMPLE_TEST_MAX = 512;
	const uint64_t LARGE_TEST_MAX = 1024 * 16;

	void regular_test(uint64_t max) {
		uint64_t i;

		store.Reset();

		// Test a single key
		EXPECT(std::optional<std::string>{}, store.Get(1));
		store.Put(1, "SE");
		EXPECT(std::string{"SE"}, store.Get(1));
		EXPECT(true, store.Delete(1));
		EXPECT(std::optional<std::string>{}, store.Get(1));
		EXPECT(false, store.Delete(1));

		phase();

		// Test multiple key-value pairs
		for (i = 0; i < max; ++i) {
			store.Put(i, std::string(i + 1, 's'));
			EXPECT(std::string(i + 1, 's'), store.Get(i));
		}
		phase();

		// Test after all insertions
		for (i = 0; i < max; ++i)
			EXPECT(std::string(i + 1, 's'), store.Get(i));
		phase();

		// Test scan
		/* std::list<std::pair<uint64_t, std::string>> list_ans;
		std::list<std::pair<uint64_t, std::string>> list_stu;

		for (i = 0; i < max / 2; ++i) {
		    list_ans.emplace_back(std::make_pair(i, std::string(i + 1, 's')));
		}

		store.scan(0, max / 2 - 1, list_stu);
		EXPECT(list_ans.size(), list_stu.size());

		auto ap = list_ans.begin();
		auto sp = list_stu.begin();
		while (ap != list_ans.end()) {
		    if (sp == list_stu.end()) {
		        EXPECT((*ap).first, -1);
		        EXPECT((*ap).second, not_found);
		        ap++;
		    } else {
		        EXPECT((*ap).first, (*sp).first);
		        EXPECT((*ap).second, (*sp).second);
		        ap++;
		        sp++;
		    }
		} */

		phase();

		// Test deletions
		for (i = 0; i < max; i += 2)
			EXPECT(true, store.Delete(i));

		for (i = 0; i < max; ++i)
			EXPECT((i & 1) ? std::make_optional(std::string(i + 1, 's')) : std::nullopt, store.Get(i));

		for (i = 1; i < max; ++i)
			EXPECT(bool(i & 1), store.Delete(i));

		phase();

		report();
	}

public:
	explicit CorrectnessTest(const std::string &dir, bool v = true) : Test(dir, v) {}

	void start_test(void *args = NULL) override {
		std::cout << "KVStore Correctness Test" << std::endl;

		store.Reset();

		std::cout << "[Simple Test]" << std::endl;
		regular_test(SIMPLE_TEST_MAX);

		store.Reset();

		std::cout << "[Large Test]" << std::endl;
		regular_test(LARGE_TEST_MAX);
	}
};

int main(int argc, char *argv[]) {
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
	std::cout << "  -v: print extra info for failed tests [currently ";
	std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
	std::cout << std::endl;
	std::cout.flush();

	CorrectnessTest test("./data", verbose);

	test.start_test();

	return 0;
}
