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
      _data[line.substr(0, line.find('\0'))] = line.substr(line.find('\0') + 1);
    }
    _uncommitted.open(path.data(), std::ios::app | std::ios::binary);
  }
  void set(std::string_view key, std::string_view value) {
    _uncommitted << key << '\0' << value << std::endl;
    _data[std::string(key)] = std::string(value);
  }
  std::optional<std::string> get(std::string_view key) {
    std::ifstream scanUncommitted(_path, std::ios::binary);
    std::string line;
    while (std::getline(scanUncommitted, line)) {
      if (line.substr(0, line.find('\0')) == key) {
        return line.substr(line.find('\0') + 1);
      }
    }
    return std::nullopt;
  }
  void remove(std::string_view key) {
    std::ofstream temp(_path + ".tmp", std::ios::binary);
    std::ifstream scanUncommitted(_path, std::ios::binary);
    std::string line;
    while (std::getline(scanUncommitted, line)) {
      if (line.substr(0, line.find('\0')) != key) {
        temp << line << std::endl;
      }
    }
    temp.close();
    scanUncommitted.close();
    std::filesystem::remove(_path);
    std::filesystem::rename(_path + ".tmp", _path);
    _data.erase(std::string(key));
  }
};


class Database {
  const std::string _path;
  UncommitedStorage _uncommitted;
public:
  Database(std::string_view path)
    : _path(path),
    _uncommitted(std::string(path) + "/uncommitted.log") {
  }
  void set(std::string_view key, std::string_view value) {
    _uncommitted.set(key, value);
  }
  std::optional<std::string> get(std::string_view key) {
    return _uncommitted.get(key);
  }
  void remove(std::string_view key) {
    _uncommitted.remove(key);
  }
};