#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <optional>
#include <fstream>

#include "Utils.hpp"

class UncommittedStorage {
  const std::string _path;
  utils::StringKeyHashTable<std::string> _data;
  std::ofstream _uncommitted;
public:
  UncommittedStorage(std::string_view path)
  : _path(path),
    _uncommitted(path.data(), std::ios::app | std::ios::binary)
  {
    if (!std::filesystem::exists(path) || std::filesystem::file_size(path) == 0) {
      return;
    }

    std::ifstream file(path.data(), std::ios::binary);
    utils::RecordStreamIteration it(file);
    while (auto record = it.next()) {
      _data[std::string(record->key)] = std::string(record->value);
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
    utils::writeRecordToFile<true /*flush*/>(_uncommitted, {key, value});
  }
  bool get(std::string& output, std::string_view key) {
    if (auto it = _data.find(key); it != _data.end()) {
      if (it->second == "\0") {
        return false;
      }
      output = it->second;
      return true;
    }
    return false;
  }
  void remove(std::string_view key) {
    set(key, "\0");
  }
  void clear() {
    _uncommitted.close();
    std::filesystem::remove(_path);
    _uncommitted.open(_path, std::ios::app | std::ios::binary);
    _data.clear();
  }
  auto size() const { return _data.size(); }
  bool empty() const { return _data.empty(); }
  auto& data() { return _data; }
};