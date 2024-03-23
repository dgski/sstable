# Simple SSTable Storage Implementation

## TODO
- Write strings as length+chars rather than null seperated
- Write records as length+record rather than newline separated 
- Add simple repl / runtime
- BloomFilters
- Un-map 'old segments'

## Support Many Data Segments TODO
- [x] Support removes
- [x] Create collection of MultiStroage + load on start-up
- [x] Create merging apparatus

## TODO AMBITIOUS
- Add networking support
- Add caching support
- Add multi-node