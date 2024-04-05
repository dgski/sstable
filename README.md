# Simple SSTable Storage Implementation

Simple, clear implementation of SSTable-based Key-Value Data Storage Engine in C++. Design information:

- Two threads:
    - New reads/writes
    - Committing new segments to disk and merging adjacent segments
- Stores new writes into an in-memory hash-table as well as a write-ahead log file.
- After a threshold is met, moves the 'uncommitted' writes into the read-only 'committing' section.
- When the background thread is ready it will take all the 'committing' writes, sort them by the keys, and write them out into a new file segment with a unique ID.
- The committed records file segments are memory mapped to fast repeated access.
- To make reads from committed file segments as fast as possible there are two added data structures:
    - A Bloom Filter to probabilistically check that the key *may* be within the segment.
    - An memory-mapped index file compatible with binary search.
- While commits are happening on the background thread, all records can be read via the 'committing' section.
- The background thread will periodically 'merge' adjacent segments into one segment. While this is happening all records can be read via the old segments.
- Commits/Merges are finalized atomically via a mutex which controls access to the committing and the committed storage.