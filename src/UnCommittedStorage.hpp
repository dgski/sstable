#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <optional>
#include <fstream>
#include <boost/unordered/unordered_flat_map.hpp>

struct Hash {
  using is_transparent = void;
  size_t operator()(const std::string& key) const {
    return std::hash<std::string>()(key);
  }
  size_t operator()(const std::string_view& key) const {
    return std::hash<std::string_view>()(key);
  }
};

struct Eq {
  using is_transparent = void;
  bool operator()(const std::string& lhs, const std::string& rhs) const {
    return lhs == rhs;
  }
  bool operator()(const std::string_view& lhs, const std::string_view& rhs) const {
    return lhs == rhs;
  }
  bool operator()(const std::string& lhs, const std::string_view& rhs) const {
    return lhs == rhs;
  }
  bool operator()(const std::string_view& lhs, const std::string& rhs) const {
    return lhs == rhs;
  }
};

using HashTable = boost::unordered_flat_map<std::string, std::string, Hash, Eq>;

class UncommittedStorage {
  const std::string _path;
  HashTable _data;
  std::ofstream _uncommitted;
public:
  UncommittedStorage(std::string_view path) : _path(path) {
    std::ifstream scanUncommitted(_path, std::ios::binary);
    std::string line;
    while (std::getline(scanUncommitted, line)) {
      const auto isSet = line.find('\0');
      if (isSet != std::string::npos) {
        _data[line.substr(0, isSet)] = line.substr(isSet + 1);
      } else {
        _data[line] = "\0";
      }
    }
    _uncommitted.open(path.data(), std::ios::app | std::ios::binary);
  }
  void set(std::string_view key, std::string_view value) {
    auto it = _data.find(key);
    if (it != _data.end() && it->second == value) {
      return;
    }
    _uncommitted << key << '\0' << value << std::endl;
    _data[key] = std::string(value);
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
    auto it = _data.find(key);
    if (it != _data.end() && it->second == "\0") {
      return;
    }
    _uncommitted << key << std::endl;
    _data[key] = "\0";
  }
  void clear() {
    _uncommitted.close();
    std::filesystem::remove(_path);
    _uncommitted.open(_path, std::ios::app | std::ios::binary);
    _data.clear();
  }
  auto size() const {
    return _data.size();
  }
  bool empty() const {
    return _data.empty();
  }
  auto& data() {
    return _data;
  }
};