#pragma once

#include <string>
#include <string_view>
#include <map>
#include <unordered_map>
#include <optional>
#include <fstream>
#include <filesystem>

class Database {
  std::unordered_map<std::string, std::string> _data;
  const std::string _path;
  std::ofstream _uncommitted;
public:
  Database(std::string_view path) : _path(path) {
    if (!std::filesystem::exists(_path)) {
      std::filesystem::create_directories(_path);
    }
    _uncommitted.open(std::string(_path) + "/uncommitted.log", std::ios::app | std::ios::binary);
  }
  void set(std::string_view key, std::string_view value) {
    _uncommitted << key << '\0' << value << std::endl;
    //_data[std::string(key)] = std::string(value);
  }
  std::optional<std::string> get(std::string_view key) {
    std::ifstream scanUncommitted(_path + "/uncommitted.log", std::ios::binary);
    std::string line;
    while (std::getline(scanUncommitted, line)) {
      if (line.substr(0, line.find('\0')) == key) {
        return line.substr(line.find('\0') + 1);
      }
    }
    return std::nullopt;
  }
  void remove(std::string_view key) {
    //_data.erase(std::string(key));
  }
};