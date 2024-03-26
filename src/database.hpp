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
  std::atomic<bool> _running = true;
  size_t _nextCommitId = 0;
  std::set<size_t> _knownSegments;
  std::unique_ptr<std::thread> _backgroundThread;

  void background() {
    while (_running.load(std::memory_order_relaxed)) {
      commit();
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }
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
    _backgroundThread = std::make_unique<std::thread>(&Database::background, this);
  }
  ~Database() {
    _running.store(false, std::memory_order_relaxed);
    _backgroundThread->join();
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
  std::string* get(std::string_view key) {
    thread_local std::string output;
    if (auto result = _uncommitted.get(output, key); result) {
      return &output;
    }
    {
      if (auto result = _committing.access()->get(output, key); result) {
        return &output;
      }
    }

    for (auto& [_, committed] : *_committed.access()) {
      if (auto result = committed.get(output, key); result) {
        return &output;
      }
    }

    return nullptr;
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
    merge();

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
    constexpr auto MAX_SEGMENT_SIZE = 1024 * 1024 * 50; // MB;
    thread_local std::vector<size_t> removed;
    removed.clear();
    struct IdAndStorage { size_t segmentId; CommittedStorage storage; };
    thread_local std::vector<IdAndStorage> added;
    added.clear();
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
      CommittedStorage::merge(
        std::format("{}/{}.data", _path, newSegmentId),
        std::format("{}/{}.data", _path, *first),
        std::format("{}/{}.data", _path, *second));
      added.push_back(IdAndStorage{
        newSegmentId,
        CommittedStorage(std::format("{}/{}.data", _path, newSegmentId))});
      removed.push_back(*first);
      removed.push_back(*second);
      first = std::next(second);
    }

    auto committedHandle = _committed.access();
    for (const auto segmentId : removed) {
      _knownSegments.erase(segmentId);
      committedHandle->erase(segmentId);
      std::filesystem::remove(std::format("{}/{}.data", _path, segmentId));
    }

    for (auto& committed : added) {
      _knownSegments.insert(committed.segmentId);
      committedHandle->emplace(committed.segmentId, std::move(committed.storage));
    }
  }

  void blockUntilAllCommitsAreDone() {
    while (!_uncommitted.empty() || !_committing.access()->empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      prepareCommit();
    }
  }
};