#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <fstream>
#include <vector>
#include <map>
#include <cstring>


class ComittedStorage {
  const std::string _path;
  struct IndexEntry { char key[30]; size_t pos; };
  std::vector<IndexEntry> _index;
public:
  ComittedStorage(std::string_view path) : _path(path) {}

  std::optional<std::string> get(std::string_view key) {
    auto keySlice = key.substr(0, 29);
    auto it = std::lower_bound(
      _index.begin(), _index.end(), keySlice,
      [](const IndexEntry& index, std::string_view key) {
      return std::string_view(index.key) < key;
    });
    if (it == _index.end()) {
      return std::nullopt;
    }
    std::ifstream committed(_path, std::ios::binary);
    committed.seekg(it->pos);
    std::string line;

    while (std::getline(committed, line)) {
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
    // Load current data
    std::map<std::string, std::string> data;
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

    _index.clear();

    // Write back to file
    std::ofstream committed(_path, std::ios::binary);
    for (const auto& [key, value] : data) {
      const auto pos = committed.tellp();
      auto& currentKeyIndex = _index.emplace_back();
      std::memcpy(currentKeyIndex.key, key.data(), 29);
      currentKeyIndex.key[29] = '\0';
      currentKeyIndex.pos = pos;
      committed << key << '\0' << value << std::endl;
    }
  }
};