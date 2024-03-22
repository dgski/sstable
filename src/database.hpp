#pragma once

#include <string>
#include <string_view>
#include <map>
#include <unordered_map>
#include <optional>
#include <fstream>
#include <filesystem>
#include <set>
#include <format>
#include <ranges>

#include "UnCommittedStorage.hpp"
#include "CommittedStorage.hpp"
#include "Utils.hpp"

class Database {
  static constexpr auto MAX_UNCOMMITTED_ACTIONS = 10000;

  const std::string _path;
  UncommittedStorage _uncommitted;
  utils::ProtectedResource<UncommittedStorage> _committing;
  utils::ProtectedResource<std::map<size_t, CommittedStorage, std::greater<>>> _committed;
  
  // Accessed by background thread only after init
  size_t _nextCommitId = 0;
  std::set<size_t> _knownSegments;
public:
  Database(std::string_view path)
    : _path(path),
    _uncommitted(std::string(path) + "/uncommitted.log"),
    _committing(std::string(path) + "/committing.log")
  {
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      if (entry.path().extension() == ".data") {
        const size_t segmentId = std::stoull(entry.path().stem().string());
        _nextCommitId = std::max(_nextCommitId, segmentId + 1);
        _committed.access()->emplace(segmentId, CommittedStorage(entry.path().string()));
        _knownSegments.insert(segmentId);
      }
    }
  }
  ~Database() {
    commit();
    prepareCommit();
    commit();
  }

  void set(std::string_view key, std::string_view value) {
    _uncommitted.set(key, value);
    if (_uncommitted.size() > MAX_UNCOMMITTED_ACTIONS) {
      prepareCommit();
    }
  }
  void remove(std::string_view key) {
    _uncommitted.remove(key);
    if (_uncommitted.size() > MAX_UNCOMMITTED_ACTIONS) {
      prepareCommit();
    }
  }
  std::optional<std::string> get(std::string_view key) {
    if (auto result = _uncommitted.get(key); result) {
      return result;
    }
    {
      auto committingHandle = _committing.access();
      if (auto result = committingHandle->get(key); result) {
        return result;
      }
    }

    for (auto& [_, committed] : *_committed.access()) {
      if (auto result = committed.get(key); result) {
        return result;
      }
    }

    return std::nullopt;
  }

  void prepareCommit() {
    {
      auto committingHandle = _committing.access();
      if (!committingHandle->empty()) {
        return;
      }
      std::filesystem::rename(_path + "/uncommitted.log", _path + "/committing.log");
      committingHandle->data().swap(_uncommitted.data());
      _uncommitted.clear();
    }
  }

  void commit() {
    if (_committing.access()->empty()) {
      return;
    }

    UncommittedStorage tmp(_path + "/committing.log");
    const size_t commitSegmentId = _nextCommitId++;
    CommittedStorage newCommitted(std::format("{}/{}.data", _path, commitSegmentId));
    newCommitted.add(tmp.data());
    _committed.access()->emplace(commitSegmentId, std::move(newCommitted));
    _knownSegments.insert(commitSegmentId);
    _committing.access()->clear();
  }

  void merge() {
    constexpr auto MAX_SEGMENT_SIZE = 1024 * 1024 * 10; // 10MB
    // Merge adjacent segments if they are small enough
    auto first = _knownSegments.begin();
    while ((first != _knownSegments.end()) && (std::next(first) != _knownSegments.end())) {
      auto second = std::next(first);
      const auto firstSize = std::filesystem::file_size(std::format("{}/{}.data", _path, *first));
      const auto secondSize = std::filesystem::file_size(std::format("{}/{}.data", _path, *second));
      const auto combinedSize = firstSize + secondSize;
      if (combinedSize > MAX_SEGMENT_SIZE) {
        first = second;
        continue;
      }

      const auto newSegmentId = _nextCommitId++;
      auto merged = CommittedStorage::merge(
        std::format("{}/{}.data", _path, newSegmentId),
        std::format("{}/{}.data", _path, *first),
        std::format("{}/{}.data", _path, *second));

      auto committedHandle = _committed.access();
      committedHandle->erase(*first);
      committedHandle->erase(*second);
      committedHandle->emplace(newSegmentId, std::move(merged));
    }
  }
};