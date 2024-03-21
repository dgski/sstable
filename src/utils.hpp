#pragma once

#include <chrono>
#include <ctype.h>
#include <functional>

namespace utils {
  template<typename F>
  auto benchmark(F&& func, size_t iterations) {
    auto start = std::chrono::high_resolution_clock::now();
    auto result = func();
    for (size_t i = 0; i < iterations-1; ++i) {
      func();
    }
    auto end = std::chrono::high_resolution_clock::now();
    return std::make_pair(
      result,
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / iterations);
  }
} // namespace utils