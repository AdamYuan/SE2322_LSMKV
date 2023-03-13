#pragma once

#include <cinttypes>
#include <memory>
#include <streambuf>
#include <string>

namespace lsm {

template <typename Type> struct DefaultIO {
	inline static constexpr std::size_t GetSize(const Type &) { return sizeof(Type); }
	template <typename Stream> inline static void Write(Stream &ostr, const Type &val) {
		const char *src = (const char *)(&val);
		ostr.write(src, sizeof(Type));
	}
	template <typename Stream> inline static Type Read(Stream &istr, std::size_t length = sizeof(Type)) {
		Type val;
		istr.read((char *)(&val), sizeof(Type));
		return val;
	}
};

template <> struct DefaultIO<std::string> {
	inline static std::size_t GetSize(const std::string &str) { return str.length(); }
	template <typename Stream> inline static void Write(Stream &ostr, const std::string &str) {
		ostr.write(str.data(), str.length());
	}
	template <typename Stream> inline static std::string Read(Stream &istr, std::size_t length) {
		std::string str;
		str.resize(length);
		istr.read(str.data(), length);
		return str;
	}
};

} // namespace lsm
