#pragma once

#include <chrono>

template <typename Func> inline double ms(Func &&func) {
	auto begin = std::chrono::high_resolution_clock::now();
	func();
	auto end = std::chrono::high_resolution_clock::now();
	return std::chrono::duration<double>{end - begin}.count() * 1000.0;
}

template <typename Func> inline double sec(Func &&func) {
	auto begin = std::chrono::high_resolution_clock::now();
	func();
	auto end = std::chrono::high_resolution_clock::now();
	return std::chrono::duration<double>{end - begin}.count();
}
