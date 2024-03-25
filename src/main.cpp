#include <iostream>

#include "Utils.hpp"
#include "Database.hpp"

int main(int argc, char* argv[]) {
  assert(argc == 2);
  const auto ENTRIES_COUNT = std::stoi(argv[1]);
  const auto entries = utils::createRandomEntries(ENTRIES_COUNT, 30, 100);

  std::filesystem::create_directories("db");
  Database db("db");

  const auto writesPerf = utils::benchmark([&db, &entries] {
    for (const auto& [key, value] : entries) {
      db.set(key, value);
    }
    return true;
  }, 1);
  const auto timeForASingleWrite = writesPerf.second / ENTRIES_COUNT;
  std::cout
    << "timeForASingleWrite=" << timeForASingleWrite
    << " result=" << writesPerf.first << std::endl;

  const auto readsPerf = utils::benchmark([&db, &entries] {
    std::string* result = nullptr;
    for (const auto& [key, value] : entries) {
      result = db.get(key);
    }
    return result;
  }, 1);
  const auto timeForASingleRead = readsPerf.second / ENTRIES_COUNT;
  std::cout
    << "timeForASingleRead=" << timeForASingleRead
    << " result=" << (readsPerf.first ? readsPerf.first->data() : "NULL") << std::endl;

  const auto removesPerf = utils::benchmark([&db, &entries] {
    for (const auto& [key, value] : entries) {
      db.remove(key);
    }
    return true;
  }, 1);
  const auto timeForASingleRemove = removesPerf.second / ENTRIES_COUNT;
  std::cout
    << "timeForASingleRemove=" << timeForASingleRemove
    << " result=" << removesPerf.first << std::endl;

  // Re-add
  for (const auto& [key, value] : entries) {
    db.set(key, value);
  }

  db.blockUntilAllCommitsAreDone();

  const auto readsPerfAfterCommit = utils::benchmark([&db, &entries] {
    std::string* result = nullptr;
    for (const auto& [key, value] : entries) {
      result = db.get(key);
    }
    return result;
  }, 10);
  const auto timeForASingleReadAfterCommit = readsPerfAfterCommit.second / ENTRIES_COUNT;
  std::cout
    << "timeForASingleReadAfterCommit=" << timeForASingleReadAfterCommit
    << " result=" << (readsPerfAfterCommit.first ? readsPerfAfterCommit.first->data() : "NULL") << std::endl;

  return 0;
}