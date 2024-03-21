#pragma once

#include <chrono>
#include <ctype.h>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <span>

#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>


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

  template<typename T>
  class ReadOnlyFileMappedArray {
    boost::interprocess::file_mapping _file;
    boost::interprocess::mapped_region _region;
    std::span<T> _data;
  public:
    ReadOnlyFileMappedArray() {}
    ReadOnlyFileMappedArray(std::string_view path) {
      remap(path);
    }
    void remap(std::string_view path) {
      _file = boost::interprocess::file_mapping(path.data(), boost::interprocess::read_only);
      _region = boost::interprocess::mapped_region(_file, boost::interprocess::read_only);
      _data = std::span<T>(reinterpret_cast<T*>(_region.get_address()), _region.get_size() / sizeof(T));
    }
    auto begin() { return _data.begin(); }
    auto begin() const { return _data.begin(); }
    auto end() { return _data.end(); }
    auto end() const { return _data.end(); }
    auto size() const { return _data.size(); }
    auto empty() const { return _data.empty(); }
    auto& operator[](size_t index) { return _data[index]; }
    auto& operator[](size_t index) const { return _data[index]; }
  };

  // Iterate each line in the buffer
  // stop iteration by returning false from the callback
  template<typename Func>
  void forEachLine(std::string_view contents, Func&& func) {
    auto it = contents.begin();
    while (it != contents.end()) {
      const auto lineEnd = std::find(it, contents.end(), '\n');
      if (!func(std::string_view(&*it, lineEnd - it))) {
        return;
      }
      it = lineEnd + 1;
    }
  }

  // Iterate each key-value pair in the buffer
  // stop iteration by returning false from the callback
  void forEachKeyValue(
    std::string_view contents,
    std::function<bool(std::string_view, std::string_view)> func)
  {
    forEachLine(contents, [&](std::string_view line) {
      const auto keyEndPos = line.find('\0');
      if (keyEndPos != std::string_view::npos) {
        return func(line.substr(0, keyEndPos), line.substr(keyEndPos + 1));
      }
      return true;
    });
  }

} // namespace utils