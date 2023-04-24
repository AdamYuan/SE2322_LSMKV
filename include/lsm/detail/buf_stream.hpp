#pragma once

#include <memory>

#include "../type.hpp"

namespace lsm::detail {

// TODO: Use std::streambuf + std::istream / std::ostream
struct IBufStream {
	const char *buffer;
	size_type pos;
	inline void read(char *dst, size_type len) {
		std::copy(buffer + pos, buffer + pos + len, dst);
		pos += len;
	}
};
struct OBufStream {
	char *buffer;
	size_type pos;
	inline void write(const char *src, size_type len) {
		std::copy(src, src + len, buffer + pos);
		pos += len;
	}
};

} // namespace lsm::detail
