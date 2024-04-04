#pragma once

#include <string>
#include <string_view>
#include <map>
#include <unordered_map>
#include <optional>
#include <fstream>
#include <filesystem>
#include <format>
#include <ranges>
#include <thread>
#include <atomic>
#include <unordered_set>

#include "UnCommittedStorage.hpp"
#include "CommittedStorage.hpp"
#include "Utils.hpp"

class Database {
  static constexpr auto MAX_UNCOMMITTED_ACTIONS = 10000;
  static constexpr auto MAX_SEGMENT_SIZE = 1024 * 1024 * 50; // 10MB

  const std::string _path;
  UncommittedStorage _uncommitted;
  utils::ProtectedResource<UncommittedStorage> _committing;
  utils::ProtectedResource<std::map<size_t, CommittedStorage, std::greater<>>> _committed;

  // Accessed by background thread only after init
  std::atomic<bool> _running = true;
  size_t _nextCommitId = 0;
  std::unique_ptr<std::thread> _backgroundThread;

  void background() {
    while (_running.load(std::memory_order_relaxed)) {
      commit();
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  size_t getSegmentSize(size_t segmentId) {
    return std::filesystem::file_size(std::format("{}/{}.data", _path, segmentId));
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
    if (bool found = _uncommitted.get(output, key); found) {
      return &output;
    }
    if (bool found = _committing.access()->get(output, key); found) {
      return &output;
    }
    for (auto& [_, committed] : *_committed.access()) {
      if (bool found = committed.get(output, key); found) {
        return &output;
      }
    }
    return nullptr;
  }

  void prepareCommit() {
    auto committing = _committing.access();
    if (!committing->empty()) {
      return;
    }
    std::filesystem::rename(_path + "/uncommitted.log", _path + "/committing.log");
    committing->data().swap(_uncommitted.data());
    _uncommitted.clear();
  }

  void commit() {
    mergeAdjacentSegments();

    if (_committing.access()->empty()) {
      return;
    }

    const size_t commitSegmentId = _nextCommitId++;
    const auto newSegmentPath = std::format("{}/{}.data", _path, commitSegmentId);
    CommittedStorage::logToSegment(newSegmentPath, std::format("{}/committing.log", _path));
    CommittedStorage newCommitted(newSegmentPath);
    _committed.access()->emplace(commitSegmentId, std::move(newCommitted));
    _committing.access()->clear();
  }

  void mergeAdjacentSegments() {
    struct MergedAction {
      size_t firstSegmentId;
      size_t secondSegmentId;
      size_t segmentId;
      CommittedStorage storage;
    };
    std::vector<MergedAction> merged;

    const auto& segments = _committed.unprotectedAccess();
    auto first = segments.begin();
    while ((first != segments.end()) && (std::next(first) != segments.end())) {
      auto second = std::next(first);
      const size_t firstSegmentId = first->first;
      const size_t secondSegmentId = second->first;
      const size_t combinedSize = getSegmentSize(firstSegmentId) + getSegmentSize(secondSegmentId);
      if (combinedSize > MAX_SEGMENT_SIZE) {
        first = second;
        continue;
      }

      const auto newSegmentId = _nextCommitId++;
      CommittedStorage::merge(
        std::format("{}/{}.data", _path, newSegmentId),
        std::format("{}/{}.data", _path, firstSegmentId),
        std::format("{}/{}.data", _path, secondSegmentId));
      merged.emplace_back(
        firstSegmentId,
        secondSegmentId,
        newSegmentId,
        CommittedStorage(std::format("{}/{}.data", _path, newSegmentId)));
      first = std::next(second);
    }

    auto committed = _committed.access();
    for (auto& action : merged) {
      committed->erase(action.firstSegmentId);
      std::filesystem::remove(std::format("{}/{}.data", _path, action.firstSegmentId));
      committed->erase(action.secondSegmentId);
      std::filesystem::remove(std::format("{}/{}.data", _path, action.secondSegmentId));
      committed->emplace(action.segmentId, std::move(action.storage));
    }
  }

  void blockUntilAllCommitsAreDone() {
    while (!_uncommitted.empty() || !_committing.access()->empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      prepareCommit();
    }
  }
};