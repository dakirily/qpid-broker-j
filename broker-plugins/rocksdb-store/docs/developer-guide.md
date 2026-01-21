# RocksDB Store Developer Guide

## Architecture
- The RocksDB store provides configuration and message persistence in one RocksDB database.
- The AMQP 1.0 link store shares the same RocksDB environment via `RocksDBContainer`.
- Column families isolate configuration records, message data, queue entries, and link definitions.
- Preference storage is created via `PreferenceStoreFactoryService` with type `RocksDB`.
- Message store data is stored under `storePath/store` unless `storePath` already points at an existing RocksDB directory.

## Column Families
- `default` used by RocksDB internals.
- `configured_objects` stores serialized configuration records.
- `configured_object_hierarchy` stores parent links for configured objects.
- `preferences` stores preference records.
- `message_metadata` stores message metadata payloads.
- `message_content` stores inline message content.
- `message_chunks` stores chunked message content.
- `queue_segments` stores queue segment data (message ids and ack state).
- `queue_state` stores queue segment state metadata.
- `queue_entries` stores queue-id/message-id pairs.
- `xids` stores distributed transaction records.
- `version` stores store metadata such as `nextMessageId` and link store version.
- `amqp_1_0_links` stores AMQP 1.0 link definitions.

## Configuration Store Layout
- Keys are UUID strings encoded as UTF-8.
- Values contain JSON with `type` and `attributes` fields.
- Parent links are stored as `{childId}|{parentType}` -> `{parentId}`.

## Message Store Layout
- Message metadata keys are 8-byte big-endian message ids.
- Metadata values include a header with a chunked flag and per-message chunk size.
- Inline message content keys are 8-byte message ids.
- Chunked message content keys are `{messageId}{chunkIndex}` pairs.
- Queue entries are `{queueId}{messageId}` for ordered traversal by queue.
- Queue segments are stored per `{queueId, segmentNo}` and carry entry ids/acks.
- Queue segment state stores per-segment metadata used during recovery.
- XIDs store the format and byte arrays for global and branch ids with encoded actions.

Chunking is selected at write time based on `messageInlineThreshold`, and the chosen `messageChunkSize`
is stored in metadata so readers can reconstruct content correctly.

Queue segments group message ids by `segmentNo = messageId >>> queueSegmentShift`, with the shift configured
via the managed attribute `queueSegmentShift` (default 16).
Large segments trade fewer keys for O(n) insertion cost (array copy) and ack bit shifts; smaller segments reduce per-insert cost but increase the number of segment entries.
The message store logs a warning when a segment exceeds ~100k entries to help identify hotspots.

## Write Durability
- Write behavior is controlled by `writeSync` and `disableWAL` via `WriteOptions`.
- `bytesPerSync` and `walBytesPerSync` control background data/WAL sync intervals.

## Transaction Settings
- Message store operations use TransactionDB and configure `TransactionDBOptions` from managed attributes.
- `defaultLockTimeout`, `transactionLockTimeout`, `maxNumLocks`, `numStripes`, and `txnWritePolicy` map directly
  to TransactionDB options; 0 or empty values use RocksDB defaults.
- Transaction retries are controlled by `txnRetryAttempts` (default 5) and `txnRetryBaseSleepMs` (default 25ms),
  using exponential backoff between attempts.

## Async Commit (Coalescing)
- Async commit is handled by a dedicated commit thread that batches transactions.
- `committerNotifyThreshold` controls batch size; `committerWaitTimeoutMs` controls max wait before flush.
- When `writeSync` is enabled, async commits defer WAL sync and perform a batch `flushWal(true)` after commit.

## AMQP 1.0 Link Store Layout
- Link keys are length-prefixed UTF-8 values for `remoteContainerId` and `linkName`,
  followed by a single boolean for the role.
- Link values contain a version byte followed by AMQP-encoded `Source` and `Target` payloads.
- The link store version is stored under key `amqp_1_0_links_version` in the `version` column family.

## Versioning and Upgrades
- Link store version uses `BrokerModel` model version strings.
- On open, the link store rejects downgrades and rewrites entries for upgrades.
- Configuration and message stores record format versions in the `version` column family.
- Store version keys: `rocksdb_config_store_version`, `rocksdb_message_store_version` (current value 1).
- If a newer version is detected, startup fails fast; upgrade scaffolding is required for future versions.

## Operations and Management
- `RocksDBManagementSupport` provides update, flush, compaction, and diagnostics.
- Statistics are available only when `enableStatistics` is true in settings.
- `updateMutableConfig` applies mutable options to live DB/column families.

## Native Resource Lifecycle
- `LRUCache`, `BloomFilter`, `Statistics`, and `BlockBasedTableConfig` are created when enabled.
- These are explicitly closed in `RocksDBEnvironmentImpl.close()` to avoid JNI memory leaks.

## Testing
- Configuration store tests exercise create/update/remove/reload operations.
- Message store tests validate enqueue, dequeue, and transaction semantics.
- Link store tests validate persistence and delete semantics using `LinkStoreTestCase`.
