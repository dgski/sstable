#pragma once

#include <string>
#include <string_view>
#include <map>
#include <unordered_map>
#include <optional>
#include <fstream>
#include <filesystem>

class UncommitedStorage {
  const std::string _path;
  std::unordered_map<std::string, std::string> _data;
  std::ofstream _uncommitted;
public:
  UncommitedStorage(std::string_view path) : _path(path) {
    std::ifstream scanUncommitted(_path, std::ios::binary);
    std::string line;
    while (std::getline(scanUncommitted, line)) {
      const auto isSet = line.find('\0');
      if (isSet != std::string::npos) {
        _data[line.substr(0, isSet)] = line.substr(isSet + 1);
      } else {
        _data.erase(line);
      }
    }
    _uncommitted.open(path.data(), std::ios::app | std::ios::binary);
  }
  void set(std::string_view key, std::string_view value) {
    _uncommitted << key << '\0' << value << std::endl;
    _data[std::string(key)] = std::string(value);
  }
  std::optional<std::string> get(std::string_view key) {
    if (auto it = _data.find(std::string(key)); it != _data.end()) {
      return it->second;
    }
    return std::nullopt;
  }
  void remove(std::string_view key) {
    _uncommitted << key << std::endl;
    _data.erase(std::string(key));
  }
  void clear() {
    _uncommitted.close();
    std::filesystem::remove(_path);
    _uncommitted.open(_path, std::ios::app | std::ios::binary);
    _data.clear();
  }
};

class ComittedStorage {
  const std::string _path;
public:
  ComittedStorage(std::string_view path) : _path(path) {}

  std::optional<std::string> get(std::string_view key) {
    std::ifstream scanCommitted(_path, std::ios::binary);
    std::string line;
    std::optional<std::string> result;
    while (std::getline(scanCommitted, line)) {
      const auto isSet = line.find('\0');
      if (isSet != std::string::npos) {
        if (line.substr(0, isSet) == key) {
          result = line.substr(isSet + 1);
        }
      } else {
        result = std::nullopt;
      }
    }

    return result;
  }
};

class Database {
  const std::string _path;
  UncommitedStorage _uncommitted;
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