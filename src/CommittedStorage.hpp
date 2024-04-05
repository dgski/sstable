#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <fstream>
#include <vector>
#include <map>
#include <cstring>

#include "Utils.hpp"

// A simple hash index that stores the position of the record in the file
// The key is sliced to the first 8 characters
class Index {
  std::string _indexPath;
  static constexpr size_t KEY_SLICE_SIZE = 8;
  struct IndexEntry {
    char key[KEY_SLICE_SIZE];
    size_t position;
    bool operator<(std::string_view key) const {
      return key < view();
    }
    void set(std::string_view key, size_t position) {
      std::strncpy(this->key, key.data(), KEY_SLICE_SIZE - 1);
      this->position = position;
    }
    std::string_view view() const {
      return std::string_view(key, KEY_SLICE_SIZE - 1);
    }
  };
  utils::ReadOnlyFileMappedArray<IndexEntry> _file;

  static auto getKeySlice(std::string_view key) {
    return (key.size() < KEY_SLICE_SIZE) ?
      key :
      key.substr(0, KEY_SLICE_SIZE - 1);
  }
public:
  Index(const std::string_view path) : _indexPath(std::format("{}.index", path)) {
    const bool indexExists = std::filesystem::exists(_indexPath);
    if (!indexExists) {
      std::ofstream indexFile(_indexPath.data(), std::ios::binary);
      std::ifstream dataFile(path.data(), std::ios::binary);
      utils::RecordStreamIteration it(dataFile);
      while (auto record = it.next()) {
        IndexEntry entry;
        entry.set(record->key, record->position);
        indexFile.write(reinterpret_cast<const char*>(&entry), sizeof(entry));
      }
    }
    _file.remap(_indexPath);
  }
  std::optional<size_t> find(std::string_view key) const {
    auto it = std::lower_bound(
      _file.begin(),
      _file.end(),
      getKeySlice(key),
      [](const auto& entry, const auto& key) {
        return entry.view() < key;
      });
    if (it == _file.end() || it->view() != getKeySlice(key)) {
      return std::nullopt;
    }
    return it->position;
  }
  void rename(std::string_view newPath) {
    const auto newIndexPath = std::format("{}.index", newPath);
    std::filesystem::rename(_indexPath, newIndexPath);
    _indexPath = newIndexPath;
    _file.remap(_indexPath);
  }
};

// Allows access to a single segment of the sorted committed storage via
// memory-mapped file i/o.
// On start-up the segment is scanned and the index and bloom filter are populated.
class CommittedStorage {
  std::string _path;
  utils::ReadOnlyFileMappedArray<char> _file;
  Index _index;
  utils::BloomFilter _bloomFilter;

  void remapFileArray() {
    if (std::filesystem::exists(_path) && std::filesystem::file_size(_path) != 0) {
      _file.remap(_path);
    }
  }
public:
  CommittedStorage(std::string_view path) : _path(path), _index(path) {
    remapFileArray();
    utils::RecordIteration it(std::string_view(_file.begin(), _file.end()));
    while (auto record = it.next()) {
      _bloomFilter.add(record->key);
    }
  }

  bool get(std::string& output, std::string_view key) const {
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
        if (record->value == utils::TOMBSTONE) {
          return false;
        }
        output = record->value;
        return true;
      }
    }
    return false;
  }

  void rename(std::string_view newPath) {
    std::filesystem::rename(_path, newPath);
    _path = newPath;
    _index.rename(newPath);
  }

  // Merges two sorted segment files into a new sorted segment file.
  static void merge(
    std::string_view outputPath,
    std::string_view newerPath,
    std::string_view olderPath)
  {
    std::ifstream newerFile(newerPath.data(), std::ios::binary);
    std::ifstream olderFile(olderPath.data(), std::ios::binary);
    utils::RecordStreamIteration newerIt(newerFile);
    utils::RecordStreamIteration olderIt(olderFile);

    std::ofstream ouput(outputPath.data(), std::ios::binary);
    auto write = [&ouput](const auto& record) {
      utils::writeRecordToFile<false /*flush*/>(ouput, {record->key, record->value});
    };

    auto newer = newerIt.next();
    auto older = olderIt.next();
    while (true) {
      if (!newer && !older) {
        break;
      }
      if (!newer) {
        write(older);
        older = olderIt.next();
        continue;
      }
      if (!older) {
        write(newer);
        newer = newerIt.next();
        continue;
      }
      if (newer->key < older->key) {
        write(newer);
        newer = newerIt.next();
      } else if (newer->key > older->key) {
        write(older);
        older = olderIt.next();
      } else {
        write(newer);
        newer = newerIt.next();
        older = olderIt.next();
      }
    }

    return;
  }

  // Converts an unsorted write ahead log file to a sorted segment file.
  static void logToSegment(std::string_view segmentPath, std::string_view logPath)
  {
    thread_local std::map<std::string, std::string> records;
    records.clear();

    std::ifstream input(logPath.data(), std::ios::binary);
    utils::RecordStreamIteration it(input);
    while (auto record = it.next()) {
      records[std::string(record->key)] = std::string(record->value);
    }

    std::ofstream output(segmentPath.data(), std::ios::binary);
    for (const auto& [key, value] : records) {
      utils::writeRecordToFile<false /*flush*/>(output, {key, value});
    }
  }
};