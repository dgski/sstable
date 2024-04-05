#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <optional>
#include <fstream>

#include "Utils.hpp"

// Stores uncommitted key-values in-memory, while also appending them to a write-ahead log file.
// 'get' operations are therefore very performant, while 'set' and 'remove' are slower due to the
// disk write and flush.
class UncommittedStorage {
  const std::string _writeAheadLogPath;
  utils::StringKeyHashTable<std::string> _data;
  std::ofstream _writeAheadLog;
public:
  UncommittedStorage(std::string_view writeAheadLogPath)
  : _writeAheadLogPath(writeAheadLogPath),
    _writeAheadLog(writeAheadLogPath.data(), std::ios::app | std::ios::binary)
  {
    const bool fileIsNull =
      !std::filesystem::exists(writeAheadLogPath) ||
      (std::filesystem::file_size(writeAheadLogPath) == 0);
    if (fileIsNull) {
      return;
    }

    // Read previous uncommitted data from the write-ahead log
    // and populate the in-memory hash table.
    std::ifstream file(writeAheadLogPath.data(), std::ios::binary);
    utils::RecordStreamIteration it(file);
    while (auto record = it.next()) {
      _data[record->key] = record->value;
    }
  }

  void set(std::string_view key, std::string_view value) {
    auto [it, inserted] = _data.try_emplace(key, value);
    if (!inserted) {
      if (it->second == value) {
        return;
      }
      it->second = value;
    }
    utils::writeRecordToFile<true /*flush*/>(_writeAheadLog, {key, value});
  }

  bool get(std::string& output, std::string_view key) const {
    if (auto it = _data.find(key); it != _data.end()) {
      if (it->second == utils::TOMBSTONE) {
        return false;
      }
      output = it->second;
      return true;
    }
    return false;
  }

  void remove(std::string_view key) {
    set(key, utils::TOMBSTONE);
  }

  // Clears the in-memory hash table and deletes the write-ahead log file.
  void clear() {
    _writeAheadLog.close();
    std::filesystem::remove(_writeAheadLogPath);
    _writeAheadLog.open(_writeAheadLogPath, std::ios::app | std::ios::binary);
    _data.clear();
  }
  auto size() const { return _data.size(); }
  bool empty() const { return _data.empty(); }
  auto& data() { return _data; }
};