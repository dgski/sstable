# Simple SSTable Storage Implementation

## TODO
- Multiple committed data segments:
    - Have a list of data segments... start at highest id, continue down
    - Merge the segments in a safe way
- Add simple repl / runtime
- BloomFilters

## TODO AMBITIOUS
- Add networking support
- Add caching support
- Add multi-node