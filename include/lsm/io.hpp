#pragma once

#include <memory>
#include <streambuf>
#include <string>

#include "type.hpp"

namespace lsm {

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

template <typename Type> struct IO {
	inline static constexpr size_type GetSize(const Type &) { return sizeof(Type); }
	template <typename Stream> inline static void Write(Stream &ostr, const Type &val) {
		const char *src = (const char *)(&val);
		ostr.write(src, sizeof(Type));
	}
	template <typename Stream> inline static Type Read(Stream &istr, size_type = sizeof(Type)) {
		Type val;
		istr.read((char *)(&val), sizeof(Type));
		return val;
	}
};

template <> struct IO<std::string> {
	inline static size_type GetSize(const std::string &str) { return str.length(); }
	template <typename Stream> inline static void Write(Stream &ostr, const std::string &str) {
		ostr.write(str.data(), str.length());
	}
	template <typename Stream> inline static std::string Read(Stream &istr, size_type length) {
		std::string str;
		str.resize(length);
		istr.read(str.data(), length);
		return str;
	}
};

} // namespace lsm
