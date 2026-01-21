# RocksDB Store User Guide

## Overview
The RocksDB store plugin provides configuration, message, and AMQP 1.0 link storage for Qpid Broker-J.
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

Example snippet:
```json
{
  "type": "ROCKSDB",
  "storePath": "C:\\qpid\\data\\vh",
  "createIfMissing": true,
  "writeBufferSize": 134217728
}
```

## Managed Attributes
The following attributes are supported on RocksDB-backed configured objects:
- `createIfMissing`, `createMissingColumnFamilies`
- `maxOpenFiles`, `maxBackgroundJobs`, `maxSubcompactions`
- `walDir`, `bytesPerSync`, `walBytesPerSync`
- `enableStatistics`, `statsDumpPeriodSec`
- `maxTotalWalSize`, `walTtlSeconds`, `walSizeLimitMb`
- `writeBufferSize`, `maxWriteBufferNumber`, `minWriteBufferNumberToMerge`
- `targetFileSizeBase`, `levelCompactionDynamicLevelBytes`
- `compactionStyle`, `compressionType`
- `blockCacheSize`, `blockSize`
- `bloomFilterBitsPerKey`, `cacheIndexAndFilterBlocks`, `pinL0FilterAndIndexBlocksInCache`

Most options are immutable after startup. Use broker restart when changing immutable settings.

## Operations
RocksDB-managed objects expose maintenance operations:
- `updateMutableConfig` applies mutable settings to the live database.
- `flush(columnFamily, wait)` flushes memtables to disk.
- `compactRange(columnFamily)` compacts data files.
- `getDbProperties(propertyPrefix)` and `getDbProperty(property)` return RocksDB properties.
- `getRocksDBStatistics(reset)` returns collected statistics when enabled.

## AMQP 1.0 Link Store
- The link store uses the same RocksDB database and a dedicated column family.
- The link store plugin must be present on the classpath for AMQP 1.0 persistence.
- Migration from BDB or JDBC link stores is not implemented yet.

## Troubleshooting
- Verify store path permissions and available disk space.
- Confirm JNI/native library compatibility for the target OS and JVM.
- Enable `enableStatistics` and inspect `getRocksDBStatistics` output for performance analysis.
