# Streaming-Tool
To build:
```
npm i
npm run build
```
### remove-duplicated-message
To start:
```
node dist/remove-duplicated-message.js
```
Parameters:
```
node dist/remove-duplicated-message.js --help

  Usage: remove-duplicated-message [options]

  Options:

    -V, --version                    output the version number
    --redis-url [url]                Redis connect URL (default: redis://127.0.0.1:6379)
    --redis-input-channel [input]    Redis subscribe channel (default: p_tploader)
    --redis-output-channel [output]  Redis publish channel (default: clear_tploader)
    --field-list [list]              Field list to be compared with (default: startTs,endTs)
    -h, --help                       output usage information
```
