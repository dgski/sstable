#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <fstream>
#include <vector>
#include <map>
#include <cstring>

#include "Utils.hpp"

class Index {
  static constexpr size_t KEY_SLICE_SIZE = 8;
  utils::StringKeyHashTable<size_t> _index;
public:
  void add(std::string_view key, size_t pos) {
    const auto keySlice = (key.size() < KEY_SLICE_SIZE) ?
      key :
      key.substr(0, KEY_SLICE_SIZE - 1);
    _index.emplace(keySlice, pos);
  }
  std::optional<size_t> find(std::string_view key) {
    const auto keySlice = (key.size() < KEY_SLICE_SIZE) ?
      key :
      key.substr(0, KEY_SLICE_SIZE - 1);
    if (auto it = _index.find(keySlice); it != _index.end()) {
      return it->second;
    }
    return std::nullopt;
  }
  void clear() { _index.clear(); }
  bool empty() const { return _index.empty(); }
};

class CommittedStorage {
  const std::string _path;
  utils::ReadOnlyFileMappedArray<char> _file;
  Index _index;
  utils::BloomFilter _bloomFilter;

  void remapFileArray() {
    if (std::filesystem::exists(_path) && std::filesystem::file_size(_path) != 0) {
      _file.remap(_path);
    }
  }
public:
  CommittedStorage(std::string_view path) : _path(path) {
    remapFileArray();
    utils::RecordIteration it(std::string_view(_file.begin(), _file.end()));
    while (auto record = it.next()) {
      _bloomFilter.add(record->key);
      _index.add(record->key, record->position);
    }
  }

  bool get(std::string& output, std::string_view key) {
    if (!_bloomFilter.contains(key)) {
      return false;
    }
    const auto positionInFile = _index.find(key);
    if (!positionInFile) {
      return false;
    }

    utils::RecordIteration records(std::string_view(_file.begin() + *positionInFile, _file.end()));
    while (auto record = records.next()) {
      if (record->key == key) {
        if (record->value == "\0") {
          return false;
        }
        output = record->value;
        return true;
      }
    }
    return false;
  }

  template<typename ContainerType>
  void add(ContainerType& incoming) {
    _index.clear();
    _bloomFilter.clear();
    std::ofstream tmp(_path + ".tmp", std::ios::out | std::ios::binary);
    const auto write = [&](std::string_view key, std::string_view value) {
      _bloomFilter.add(key);
      _index.add(key, tmp.tellp());
      utils::writeRecordToFile<false /*flush*/>(tmp, {key, value});
    };

    utils::RecordIteration it(std::string_view(_file.begin(), _file.end()));
    while (auto record = it.next()) {
      if (auto it = incoming.find(record->key); it != incoming.end()) {
        write(it->first, it->second);
        incoming.erase(it);
      } else {
        write(record->key, record->value);
      }
    }

    thread_local std::vector<std::pair<std::string_view, std::string_view>> remaining;
    remaining.reserve(incoming.size());
    remaining.assign(incoming.begin(), incoming.end());
    std::sort(remaining.begin(), remaining.end());
    for (const auto& [key, value] : remaining) {
      write(key, value);
    }

    tmp.flush();
    tmp.close();
    std::filesystem::rename(_path + ".tmp", _path);
    remapFileArray();
  }

  static void merge(
    std::string_view path,
    std::string_view newerPath,
    std::string_view olderPath)
  {
    std::ofstream ouput(path.data(), std::ios::out | std::ios::binary);
    utils::ReadOnlyFileMappedArray<char> newerFile(newerPath);
    utils::ReadOnlyFileMappedArray<char> olderFile(olderPath);
    utils::RecordIteration newerIt(std::string_view(newerFile.begin(), newerFile.end()));
    utils::RecordIteration olderIt(std::string_view(olderFile.begin(), olderFile.end()));

    auto newer = newerIt.next();
    auto older = olderIt.next();
    while (true) {
      if (!newer && !older) {
        break;
      }
      if (!newer) {
        utils::writeRecordToFile<false /*flush*/>(ouput, {older->key, older->value});
        older = olderIt.next();
        continue;
      }
      if (!older) {
        utils::writeRecordToFile<false /*flush*/>(ouput, {newer->key, newer->value});
        newer = newerIt.next();
        continue;
      }
      if (newer->key < older->key) {
        utils::writeRecordToFile<false /*flush*/>(ouput, {newer->key, newer->value});
        newer = newerIt.next();
      } else if (newer->key > older->key) {
        utils::writeRecordToFile<false /*flush*/>(ouput, {older->key, older->value});
        older = olderIt.next();
      } else {
        utils::writeRecordToFile<false /*flush*/>(ouput, {newer->key, newer->value});
        newer = newerIt.next();
        older = olderIt.next();
      }
    }

    return;
  }
};