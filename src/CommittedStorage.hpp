#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <fstream>

class ComittedStorage {
  const std::string _path;
public:
  ComittedStorage(std::string_view path) : _path(path) {}

  std::optional<std::string> get(std::string_view key) {
    std::ifstream scanCommitted(_path, std::ios::binary);
    std::string line;
    while (std::getline(scanCommitted, line)) {
      const auto pos = line.find('\0');
      if (pos != std::string::npos) {
        if (line.substr(0, pos) == key) {
          return line.substr(pos + 1);
        }
      }
    }
    return std::nullopt;
  }

  template<typename It>
  void add(It begin, It end) {
    // Load current into std::unordered_map
    std::unordered_map<std::string, std::string> data;
    std::ifstream scanCommitted(_path, std::ios::binary);
    std::string line;
    while (std::getline(scanCommitted, line)) {
      const auto pos = line.find('\0');
      if (pos != std::string::npos) {
        data[line.substr(0, pos)] = line.substr(pos + 1);
      }
    }
    // Add new entries
    for (auto it = begin; it != end; ++it) {
      if (it->second == "\0") {
        data.erase(it->first);
      } else {
        data[it->first] = it->second;
      }
    }

    // Write back to file
    std::ofstream committed(_path, std::ios::binary);
    for (const auto& [key, value] : data) {
      committed << key << '\0' << value << std::endl;
    }
  }
};