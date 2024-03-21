#pragma once

#include <chrono>
#include <ctype.h>
#include <functional>
#include <iostream>
#include <mutex>

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

  std::string createRandomString(size_t length) {
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
      result.push_back('a' + rand() % 26);
    }
    return result;
  }

  std::vector<std::pair<std::string, std::string>> createRandomEntries(
    size_t count, size_t keyLength, size_t valueLength)
  {
    std::vector<std::pair<std::string, std::string>> result;
    result.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      result.emplace_back(createRandomString(keyLength), createRandomString(valueLength));
    }
    return result;
  }

  template<typename T>
  class ProtectedResource {
    T _resource;
    std::mutex _mutex;
  public:
    template<typename... Args>
    ProtectedResource(Args&&... args) : _resource(std::forward<Args>(args)...) {}

    class ProtectedResourceHandle {
      T& _resource;
      std::unique_lock<std::mutex> _lock;
    public:
      ProtectedResourceHandle(T& resource, std::mutex& mutex)
        : _resource(resource), _lock(mutex) {}
      ProtectedResourceHandle(ProtectedResourceHandle&&) = default;
      ProtectedResourceHandle& operator=(ProtectedResourceHandle&&) = default;
      ProtectedResourceHandle(const ProtectedResourceHandle&) = delete;
      ProtectedResourceHandle& operator=(const ProtectedResourceHandle&) = delete;
      T& operator*() { return _resource; }
      T* operator->() { return &_resource; }
    };
    auto access() {
      return ProtectedResourceHandle(_resource, _mutex);
    }
  };

} // namespace utils