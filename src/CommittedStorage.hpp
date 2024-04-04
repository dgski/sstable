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

  static void merge(
    std::string_view path,
    std::string_view newerPath,
    std::string_view olderPath)
  {
    std::ifstream newerFile(newerPath.data(), std::ios::binary);
    std::ifstream olderFile(olderPath.data(), std::ios::binary);
    std::ofstream ouput(path.data(), std::ios::binary);
    utils::RecordStreamIteration newerIt(newerFile);
    utils::RecordStreamIteration olderIt(olderFile);

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

  static void logToSegment(std::string_view segmentPath, std::string_view logPath) {
    thread_local std::vector<utils::Record> records;
    records.clear();

    std::ifstream input(logPath.data(), std::ios::binary);
    utils::RecordStreamIteration it(input);
    while (auto record = it.next()) {
      records.push_back({record->key, record->value});
    }

    std::sort(records.begin(), records.end(), [](const auto& a, const auto& b) {
      return a.key < b.key;
    });

    std::ofstream output(segmentPath.data(), std::ios::binary);
    for (const auto& record : records) {
      utils::writeRecordToFile<false /*flush*/>(output, record);
    }
  }
};