#pragma once

#include <chrono>
#include <ctype.h>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <fstream>

#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

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
  auto data() { return _data.data(); }
  auto data() const { return _data.data(); }
};

struct Record {
  std::string_view key;
  std::string_view value;
};

static void writeRecordToFile(std::ofstream& file, const Record& record) {
  const size_t keySize = record.key.size();
  file.write(reinterpret_cast<const char*>(&keySize), sizeof(size_t));
  file.write(record.key.data(), record.key.size());

  const size_t valueSize = record.value.size();
  file.write(reinterpret_cast<const char*>(&valueSize), sizeof(size_t));
  file.write(record.value.data(), record.value.size());
}

class RecordIteration {
  std::string_view _contents;
public:
  RecordIteration(std::string_view contents) : _contents(contents) {}
  std::optional<Record> next() {
    if (_contents.empty()) {
      return std::nullopt;
    }
    Record result;
    size_t keySize, valueSize;
    std::memcpy(&keySize, _contents.data(), sizeof(size_t));
    result.key = std::string_view(_contents.data() + sizeof(size_t), keySize);
    _contents.remove_prefix(sizeof(size_t) + keySize);
    std::memcpy(&valueSize, _contents.data(), sizeof(size_t));
    result.value = std::string_view(_contents.data() + sizeof(size_t), valueSize);
    _contents.remove_prefix(sizeof(size_t) + valueSize);
    return result;
  }
};

struct StringHash {
  using is_transparent = void;
  size_t operator()(const std::string& key) const {
    return std::hash<std::string>()(key);
  }
  size_t operator()(const std::string_view& key) const {
    return std::hash<std::string_view>()(key);
  }
};

struct StringEquals {
  using is_transparent = void;
  bool operator()(const std::string& lhs, const std::string& rhs) const {
    return lhs == rhs;
  }
  bool operator()(const std::string_view& lhs, const std::string_view& rhs) const {
    return lhs == rhs;
  }
  bool operator()(const std::string& lhs, const std::string_view& rhs) const {
    return lhs == rhs;
  }
  bool operator()(const std::string_view& lhs, const std::string& rhs) const {
    return lhs == rhs;
  }
};

template<typename Value>
using StringKeyHashTable = boost::unordered_flat_map<
  std::string, Value,
  StringHash,
  StringEquals>;

} // namespace utils