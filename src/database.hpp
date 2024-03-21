#pragma once

#include <string>
#include <string_view>
#include <map>
#include <unordered_map>
#include <optional>
#include <fstream>
#include <filesystem>

#include "UnCommittedStorage.hpp"
#include "CommittedStorage.hpp"

class Database {
  const std::string _path;
  UncommittedStorage _uncommitted;
  ComittedStorage _committed;
public:
  Database(std::string_view path)
    : _path(path),
    _uncommitted(std::string(path) + "/uncommitted.log"),
    _committed(std::string(path) + "/committed.log") {
  }
  void set(std::string_view key, std::string_view value) {
    _uncommitted.set(key, value);
  }
  void remove(std::string_view key) {
    _uncommitted.remove(key);
  }
  std::optional<std::string> get(std::string_view key) {
    auto it =  _uncommitted.get(key);
    if (it) {
      return it;
    }
    return _committed.get(key);
  }

  void commit() {
    std::filesystem::rename(_path + "/uncommitted.log", _path + "/committed.log");
    _uncommitted.clear();
  }
};