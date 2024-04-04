# Simple SSTable Storage Implementation

## TODO
- Make index memory-mapped:
    - On add and merge, write out a {NUM}.index file with an array of (char key(8), size_t pos)
- Remove use of '\0' as tombstone:
    - Options:
        - Reserve use of size_t::max for tombstone
        - Add a byte to mark deletion (space effective)

## TODO AMBITIOUS
- Add networking support
- Add caching support
- Add multi-node