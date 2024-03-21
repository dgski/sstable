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