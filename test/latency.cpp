#include <lsm/kv.hpp>

#include "MurmurHash3.h"
#include <lz4.h>
#include <matplot/matplot.h>
#include <snappy.h>

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

struct SnappyStringIO {
	inline static lsm::size_type GetSize(const std::string &str) {
		std::string compressed;
		snappy::Compress(str.data(), str.length(), &compressed);
		return compressed.length();
	}
	template <typename Stream> inline static void Write(Stream &ostr, const std::string &str) {
		std::string compressed;
		snappy::Compress(str.data(), str.length(), &compressed);
		ostr.write(compressed.data(), compressed.length());
	}
	template <typename Stream> inline static std::string Read(Stream &istr, lsm::size_type length) {
		std::string compressed, str;
		compressed.resize(length);
		istr.read(compressed.data(), length);
		snappy::Uncompress(compressed.data(), length, &str);
		return std::move(str);
	}
};

template <int Acceleration> struct LZ4StringIO {
	inline static lsm::size_type GetSize(const std::string &str) {
		auto max_compressed_size = LZ4_compressBound((int)str.length());
		auto compressed_buffer = std::unique_ptr<char[]>(new char[max_compressed_size]);
		return sizeof(lsm::size_type) + LZ4_compress_fast(str.data(), compressed_buffer.get(), (int)str.length(),
		                                                  max_compressed_size, Acceleration);
	}
	template <typename Stream> inline static void Write(Stream &ostr, const std::string &str) {
		auto max_compressed_size = LZ4_compressBound((int)str.length());
		auto compressed_buffer = std::unique_ptr<char[]>(new char[max_compressed_size]);
		auto compressed_size = LZ4_compress_fast(str.data(), compressed_buffer.get(), (int)str.length(),
		                                         max_compressed_size, Acceleration);
		lsm::detail::IO<lsm::size_type>::Write(ostr, str.length());
		ostr.write(compressed_buffer.get(), compressed_size);
	}
	template <typename Stream> inline static std::string Read(Stream &istr, lsm::size_type compressed_length) {
		lsm::size_type length = lsm::detail::IO<lsm::size_type>::Read(istr);
		compressed_length -= sizeof(lsm::size_type);
		auto compressed_buffer = std::unique_ptr<char[]>(new char[compressed_length]);
		istr.read(compressed_buffer.get(), compressed_length);

		std::string str;
		str.resize(length);
		LZ4_decompress_safe(compressed_buffer.get(), str.data(), (int)compressed_length, (int)length);
		return std::move(str);
	}
};

template <typename Key> struct MyStringTrait : public lsm::KVDefaultTrait<Key, std::string> {
	using Compare = std::less<Key>;
	using Container = lsm::SkipList<Key, lsm::KVMemValue<std::string>, Compare, std::default_random_engine, 1, 2, 32>;
	using KeyFile = lsm::KVCachedBloomKeyFile<Key, MyStringTrait, lsm::Bloom<Key, 10240 * 8, Murmur3BloomHasher<Key>>>;
	// using KeyFile = lsm::KVCachedKeyFile<Key, MyStringTrait>;
	// using KeyFile = lsm::KVUncachedKeyFile<Key, MyStringTrait>;
	// using ValueIO = SnappyStringIO; // LZ4StringIO<4000>;
	constexpr static lsm::size_type kSingleFileSizeLimit = 2 * 1024 * 1024;

	constexpr static lsm::level_type kLevels = 5;
	constexpr static lsm::KVLevelConfig kLevelConfigs[] = {
	    {2, lsm::KVLevelType::kTiering},   {4, lsm::KVLevelType::kLeveling},  {8, lsm::KVLevelType::kLeveling},
	    {16, lsm::KVLevelType::kLeveling}, {32, lsm::KVLevelType::kLeveling},
	};
};

int main() {
	matplot::show();
	return 0;
}