# Simple SSTable Storage Implementation

## TODO
- Make index memory-mapped:
    - On add and merge, write out a {NUM}.index file with an array of (char key(8), size_t pos)
- Un-map 'old segments':
    1. Convert 'CommitedStorage' to Parent Class.
    2. Make get, add, remove into virtual methods
    3. Create 'MemoryMappedStorage' and 'FileStreamStorage' deriving from 'CommittedStorage'
- Remove use of '\0' as tombstone:
    - Options:
        - Reserve use of size_t::max for tombstone
        - Add a byte to mark deletion (space effective)

## TODO AMBITIOUS
- Add networking support
- Add caching support
- Add multi-node