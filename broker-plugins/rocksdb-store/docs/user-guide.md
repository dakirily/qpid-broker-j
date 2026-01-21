# RocksDB Store User Guide

## Overview
The RocksDB store plugin provides configuration, preference, message, and AMQP 1.0 link storage for Qpid Broker-J.
It uses a single RocksDB database with dedicated column families for each data domain.

## Modules
- `qpid-broker-plugins-rocksdb-store` provides configuration and message persistence.
- `qpid-broker-plugins-amqp-1-0-protocol-rocksdb-link-store` provides AMQP 1.0 link persistence.

## Requirements
- RocksDB JNI and native libraries must be available for the target platform.
- The configured `storePath` must be writable by the broker.

## Installation and Enablement
- Add the RocksDB store plugin jar(s) to the broker runtime classpath.
- Ensure the RocksDB JNI library loads successfully before starting the broker.

## Configuration
- Configure `VirtualHostNode` and `VirtualHost` objects with type `ROCKSDB`.
- For broker configuration, `SystemConfig` can also use type `ROCKSDB`.
- Set `storePath` and size monitoring attributes (`storeUnderfullSize`, `storeOverfullSize`) as required.
- Override managed attributes to tune RocksDB options.
- Configure message content chunking using `messageChunkSize` and `messageInlineThreshold` when needed.
- Preference storage is provided by RocksDB by default via `preferenceStoreAttributes` type `RocksDB`.
- If `storePath` does not already point at an existing RocksDB directory, the message store uses a `store` subdirectory (i.e. `storePath/store`).

Example snippet:
```json
{
  "type": "ROCKSDB",
  "storePath": "C:\\qpid\\data\\vh",
  "createIfMissing": true,
  "preferenceStoreAttributes": { "type": "RocksDB" },
  "writeBufferSize": 134217728
}
```

## Managed Attributes
The following attributes are supported on RocksDB-backed configured objects:
- `createIfMissing`, `createMissingColumnFamilies`
- `maxOpenFiles`, `maxBackgroundJobs`, `maxSubcompactions`
- `walDir`, `bytesPerSync`, `walBytesPerSync`, `writeSync`, `disableWAL`
- `enableStatistics`, `statsDumpPeriodSec`
- `committerNotifyThreshold`, `committerWaitTimeoutMs`
- `messageChunkSize`, `messageInlineThreshold`
- `queueSegmentShift`
- `defaultLockTimeout`, `transactionLockTimeout`, `maxNumLocks`, `numStripes`, `txnWritePolicy`
- `txnRetryAttempts`, `txnRetryBaseSleepMs`
- `maxTotalWalSize`, `walTtlSeconds`, `walSizeLimitMb`
- `writeBufferSize`, `maxWriteBufferNumber`, `minWriteBufferNumberToMerge`
- `targetFileSizeBase`, `levelCompactionDynamicLevelBytes`
- `compactionStyle`, `compressionType`
- `blockCacheSize`, `blockSize`
- `bloomFilterBitsPerKey`, `cacheIndexAndFilterBlocks`, `pinL0FilterAndIndexBlocksInCache`

Most options are immutable after startup. Use broker restart when changing immutable settings.

## Message Content Chunking
- `messageChunkSize` controls the chunk size in bytes for stored message bodies (default 65536).
- `messageInlineThreshold` controls the size in bytes above which content is chunked (default equals `messageChunkSize`).
- When the content size exceeds `messageInlineThreshold`, chunks are stored in a dedicated column family.

## Queue Segments
- `queueSegmentShift` controls the segment range using `segmentNo = messageId >>> queueSegmentShift` (default 16).
- Larger values produce fewer, larger segments; smaller values produce more, smaller segments.
- Segment state is tracked per segment to speed recovery and ack scanning.
- Large segments increase O(n) insert cost (array copy) and ack bit shifts; consider reducing `queueSegmentShift` for very large queues.
- The message store logs a warning when a single segment grows beyond ~100k entries to highlight potential hotspots.

## Write Durability
- `writeSync` controls whether each write is synchronized to disk (default false).
- `disableWAL` disables the write-ahead log (default false).
- `bytesPerSync` and `walBytesPerSync` control background sync intervals for data and WAL.

## Async Commit (Coalescing)
- `committerNotifyThreshold` controls how many async commits trigger a flush batch (default 8).
- `committerWaitTimeoutMs` controls max wait time before flushing pending async commits (default 500ms).
- Async commits are coalesced to reduce flush overhead when `writeSync` is enabled.

## Transaction Settings
- `defaultLockTimeout` and `transactionLockTimeout` are in milliseconds (default 0, which uses RocksDB defaults).
- `maxNumLocks` and `numStripes` control TransactionDB lock table sizing (default 0 uses RocksDB defaults).
- `txnWritePolicy` controls TransactionDB write policy (default empty uses RocksDB defaults).
- `txnRetryAttempts` defaults to 5 (0 uses the default).
- `txnRetryBaseSleepMs` defaults to 25ms (0 uses the default); the backoff is exponential per attempt.

## Operations
RocksDB-managed objects expose maintenance operations:
- `updateMutableConfig` applies mutable settings to the live database.
- `flush(columnFamily, wait)` flushes memtables to disk.
- `compactRange(columnFamily)` compacts data files.
- `getDbProperties(propertyPrefix)` and `getDbProperty(property)` return RocksDB properties.
- `getRocksDBStatistics(reset)` returns collected statistics when enabled.

## Native Resource Lifecycle
- When enabled, RocksDB uses native resources for cache, bloom filter, and statistics.
- These resources are explicitly released on broker shutdown.

## AMQP 1.0 Link Store
- The link store uses the same RocksDB database and a dedicated column family.
- The link store plugin must be present on the classpath for AMQP 1.0 persistence.
- Migration from BDB or JDBC link stores is not implemented yet.

## Troubleshooting
- Verify store path permissions and available disk space.
- Confirm JNI/native library compatibility for the target OS and JVM.
- Enable `enableStatistics` and inspect `getRocksDBStatistics` output for performance analysis.
