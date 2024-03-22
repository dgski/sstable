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
#include "Utils.hpp"

class Database {
  static constexpr auto MAX_UNCOMMITTED_ACTIONS = 10000;

  const std::string _path;
  UncommittedStorage _uncommitted;
  utils::ProtectedResource<UncommittedStorage> _committing;

  size_t _nextCommitId = 0;
  utils::ProtectedResource<std::map<size_t, CommittedStorage, std::greater<>>> _committed;
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
    _committing.access()->clear();
  }
};