#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <fstream>
#include <vector>
#include <map>
#include <cstring>

#include "Utils.hpp"

class CommittedStorage {
  const std::string _path;
  utils::ReadOnlyFileMappedArray<char> _file;
  static constexpr size_t INDEX_KEY_SIZE = 30;
  struct IndexEntry { char key[INDEX_KEY_SIZE]; size_t pos; };
  std::vector<IndexEntry> _index;

  void addToIndex(std::string_view key, size_t pos) {
    auto& currentKeyIndex = _index.emplace_back();
    std::memcpy(currentKeyIndex.key, key.data(), INDEX_KEY_SIZE - 1);
    currentKeyIndex.key[INDEX_KEY_SIZE - 1] = '\0';
    currentKeyIndex.pos = pos;
  }

  void remapFileArray() {
    if (!_index.empty()) {
      _file.remap(_path);
    }
  }
public:
  CommittedStorage(std::string_view path) : _path(path) {
    remapFileArray();
    utils::forEachLine(
      std::string_view(_file.begin(), _file.end()),
      [&](std::string_view line)
      {
        const auto keyEndPos = line.find('\0');
        if (keyEndPos != std::string_view::npos) {
          addToIndex(line.substr(0, keyEndPos), 0);
        }
        return true;
      });
  }

  std::optional<std::string> get(std::string_view key) {
    auto keySlice = key.substr(0, INDEX_KEY_SIZE - 1);
    auto it = std::lower_bound(
      _index.begin(), _index.end(), keySlice,
      [](const IndexEntry& index, std::string_view key) {
      return std::string_view(index.key) < key;
    });
    if (it == _index.end()) {
      return std::nullopt;
    }

    std::optional<std::string> result;
    utils::forEachLine(
      std::string_view(_file.begin() + it->pos, _file.end()),
      [&](std::string_view line)
      {
        const auto keyEndPos = line.find('\0');
        if (keyEndPos != std::string_view::npos) {
          if (line.substr(0, keyEndPos) == key) {
            result = std::string(line.substr(keyEndPos + 1));
            return false;
          }
        }
        return true;
      });

    return result;
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
    std::ofstream committed(_path, std::ios::out | std::ios::binary);
    for (const auto& [key, value] : data) {
      addToIndex(key, committed.tellp());
      committed << key << '\0' << value << std::endl;
    }

    remapFileArray();
  }
};