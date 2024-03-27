#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <fstream>
#include <vector>
#include <map>
#include <cstring>

#include "Utils.hpp"

class CommittedStorage {
  const std::string _path;
  utils::ReadOnlyFileMappedArray<char> _file;
  static constexpr size_t INDEX_KEY_SIZE = 10;
  struct IndexEntry { char key[INDEX_KEY_SIZE]; size_t pos; };
  std::vector<IndexEntry> _index;

  utils::BloomFilter _bloomFilter;

  void addToIndex(std::string_view key, size_t pos) {
    _bloomFilter.add(key);
    auto& currentKeyIndex = _index.emplace_back();
    std::memcpy(currentKeyIndex.key, key.data(), INDEX_KEY_SIZE - 1);
    currentKeyIndex.key[INDEX_KEY_SIZE - 1] = '\0';
    currentKeyIndex.pos = pos;
  }

  void remapFileArray() {
    if (!_index.empty()) {
      _file.remap(_path);
    }
  }
public:
  CommittedStorage(std::string_view path) : _path(path) {
    remapFileArray();
    utils::RecordIteration it(std::string_view(_file.begin(), _file.end()));
    while (auto record = it.next()) {
      const auto pos = record->key.data() - _file.data();
      addToIndex(record->key, pos);
    }
  }

  bool get(std::string& output, std::string_view key) {
    if (!_bloomFilter.contains(key)) {
      return false;
    }

    const auto keySlice = key.substr(0, INDEX_KEY_SIZE - 1);
    auto it = std::lower_bound(
      _index.begin(), _index.end(), keySlice,
      [](const IndexEntry& index, std::string_view key) {
      return std::string_view(index.key) < key;
    });
    if (it == _index.end() || std::string_view(it->key) != keySlice) {
      return false;
    }

    std::optional<std::string_view> result;
    utils::RecordIteration records(std::string_view(_file.begin() + it->pos, _file.end()));
    while (auto record = records.next()) {
      if (record->key == key) {
        result = record->value;
        break;
      }
    }
    if (!result || *result == "\0") {
      return false;
    }

    output = result.value();
    return true;
  }

  template<typename ContainerType>
  void add(ContainerType& incoming) {
    _index.clear();
    _bloomFilter.clear();
    std::ofstream tmp(_path + ".tmp", std::ios::out | std::ios::binary);
    const auto write = [&](std::string_view key, std::string_view value) {
      addToIndex(key, tmp.tellp());
      utils::writeRecordToFile(tmp, {key, value});
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
        utils::writeRecordToFile(ouput, {older->key, older->value});
        older = olderIt.next();
        continue;
      }
      if (!older) {
        utils::writeRecordToFile(ouput, {newer->key, newer->value});
        newer = newerIt.next();
        continue;
      }
      if (newer->key < older->key) {
        utils::writeRecordToFile(ouput, {newer->key, newer->value});
        newer = newerIt.next();
      } else if (newer->key > older->key) {
        utils::writeRecordToFile(ouput, {older->key, older->value});
        older = olderIt.next();
      } else {
        utils::writeRecordToFile(ouput, {newer->key, newer->value});
        newer = newerIt.next();
        older = olderIt.next();
      }
    }

    return;
  }
};