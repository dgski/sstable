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
  ComittedStorage _committed;
public:
  Database(std::string_view path)
    : _path(path),
    _uncommitted(std::string(path) + "/uncommitted.log"),
    _committing(std::string(path) + "/committing.log"),
    _committed(std::string(path) + "/committed.log") {
  }
  ~Database() {
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
    return _committed.get(key);
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
    _committed.add(tmp.data().begin(), tmp.data().end());
    _committing.access()->clear();
  }
};