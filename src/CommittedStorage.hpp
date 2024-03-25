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

  struct Record { std::string_view key; std::string_view value; };
  static void writeRecordToFile(std::ofstream& file, const Record& record) {
    file << record.key << '\0' << record.value << std::endl;
  }
  class RecordIteration {
    std::string_view _contents;
  public:
    RecordIteration(std::string_view contents) : _contents(contents) {}
    struct Record {
      std::string_view key;
      std::string_view value;
    };
    std::optional<Record> next() {
      if (_contents.empty()) {
        return std::nullopt;
      }
      const auto lineEnd = std::find(_contents.begin(), _contents.end(), '\n');
      const auto [key, value] = utils::split(std::string_view(_contents.begin(), lineEnd));
      _contents = std::string_view(lineEnd + 1, _contents.end());
      return Record{key, value};
    }
  };

  void addToIndex(std::string_view key, size_t pos) {
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
    RecordIteration it(std::string_view(_file.begin(), _file.end()));
    while (auto record = it.next()) {
      const auto pos = record->key.data() - _file.data();
      addToIndex(record->key, pos);
    }
  }

  bool get(std::string& output, std::string_view key) {
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
    RecordIteration records(std::string_view(_file.begin() + it->pos, _file.end()));
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
    std::ofstream tmp(_path + ".tmp", std::ios::out | std::ios::binary);
    const auto writeToTmp = [&](std::string_view key, std::string_view value) {
      addToIndex(key, tmp.tellp());
      writeRecordToFile(tmp, {key, value});
    };

    RecordIteration it(std::string_view(_file.begin(), _file.end()));
    while (auto record = it.next()) {
      if (auto it = incoming.find(record->key); it != incoming.end()) {
        writeToTmp(it->first, it->second);
        incoming.erase(it);
      } else {
        writeToTmp(record->key, record->value);
      }
    }

    thread_local std::vector<std::pair<std::string_view, std::string_view>> remaining;
    remaining.reserve(incoming.size());
    remaining.assign(incoming.begin(), incoming.end());
    std::sort(remaining.begin(), remaining.end());
    for (const auto& [key, value] : remaining) {
      writeToTmp(key, value);
    }

    std::filesystem::rename(_path + ".tmp", _path);
    remapFileArray();
  }

  static void merge(
    std::string_view path, std::string_view newer, std::string_view older)
  {
    utils::ReadOnlyFileMappedArray<char> newerFile(newer);
    utils::ReadOnlyFileMappedArray<char> olderFile(older);
    RecordIteration newerIt(std::string_view(newerFile.begin(), newerFile.end()));
    RecordIteration olderIt(std::string_view(olderFile.begin(), olderFile.end()));

    std::ofstream ouput(path.data(), std::ios::out | std::ios::binary);
    const auto write = [&](std::string_view key, std::string_view value) {
      writeRecordToFile(ouput, {key, value});
    };

    auto newValue = newerIt.next();
    auto oldValue = olderIt.next();
    while (true) {
      if (!newValue && !oldValue) {
        break;
      }
      if (!newValue) {
        write(oldValue->key, oldValue->value);
        oldValue = olderIt.next();
        continue;
      }
      if (!oldValue) {
        write(newValue->key, newValue->value);
        newValue = newerIt.next();
        continue;
      }
      if (newValue->key < oldValue->key) {
        write(newValue->key, newValue->value);
        newValue = newerIt.next();
      } else if (newValue->key > oldValue->key) {
        write(oldValue->key, oldValue->value);
        oldValue = olderIt.next();
      } else {
        write(newValue->key, newValue->value);
        newValue = newerIt.next();
        oldValue = olderIt.next();
      }
    }

    return;
  }
};