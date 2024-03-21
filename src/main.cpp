#include <iostream>

#include "utils.hpp"
#include "database.hpp"

int main() {
  Database db("db");
  
  constexpr auto ENTRIES_COUNT = 1000;
  const auto entries = utils::createRandomEntries(ENTRIES_COUNT, 100, 100);

  const auto writesPerf = utils::benchmark([&db, &entries] {
    for (const auto& [key, value] : entries) {
      db.set(key, value);
    }
    return true;
  }, 10);
  const auto timeForASingleWrite = writesPerf.second / ENTRIES_COUNT;
  std::cout
    << "timeForASingleWrite=" << timeForASingleWrite
    << " result=" << writesPerf.first << std::endl;

  const auto readsPerf = utils::benchmark([&db, &entries] {
    std::optional<std::string> result;
    for (const auto& [key, value] : entries) {
      result = db.get(key);
    }
    return result;
  }, 10);
  const auto timeForASingleRead = readsPerf.second / ENTRIES_COUNT;
  std::cout
    << "timeForASingleRead=" << timeForASingleRead
    << " result=" << readsPerf.first.value_or("NULL") << std::endl;

  const auto removesPerf = utils::benchmark([&db, &entries] {
    for (const auto& [key, value] : entries) {
      db.remove(key);
    }
    return true;
  }, 10);
  const auto timeForASingleRemove = removesPerf.second / ENTRIES_COUNT;
  std::cout
    << "timeForASingleRemove=" << timeForASingleRemove
      << " result=" << removesPerf.first << std::endl;

  // Re-add
  for (const auto& [key, value] : entries) {
    db.set(key, value);
  }
  db.commit();

  const auto readsPerfAfterCommit = utils::benchmark([&db, &entries] {
    std::optional<std::string> result;
    for (const auto& [key, value] : entries) {
      result = db.get(key);
    }
    return result;
  }, 10);
  const auto timeForASingleReadAfterCommit = readsPerfAfterCommit.second / ENTRIES_COUNT;
  std::cout
    << "timeForASingleReadAfterCommit=" << timeForASingleReadAfterCommit
    << " result=" << readsPerfAfterCommit.first.value_or("NULL") << std::endl;

  // Erase all
  for (const auto& [key, value] : entries) {
    db.remove(key);
  }

  return 0;
}