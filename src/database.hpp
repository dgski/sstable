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
  UncommittedStorage _committing;
  ComittedStorage _committed;
public:
  Database(std::string_view path)
    : _path(path),
    _uncommitted(std::string(path) + "/uncommitted.log"),
    _committing(std::string(path) + "/committing.log"),
    _committed(std::string(path) + "/committed.log") {
  }
  void set(std::string_view key, std::string_view value) {
    _uncommitted.set(key, value);
  }
  void remove(std::string_view key) {
    _uncommitted.remove(key);
  }
  std::optional<std::string> get(std::string_view key) {
    if (auto result = _uncommitted.get(key); result) {
      return result;
    }
    if (auto result = _committing.get(key); result) {
      return result;
    }
    return _committed.get(key);
  }

  void commit() {
    if (_uncommitted.empty() ||  !_committing.empty()) {
      return;
    }

    std::filesystem::rename(_path + "/uncommitted.log", _path + "/committing.log");
    _uncommitted.clear();
    _committing.clear();
  }
};