# RocksDB Store Developer Guide

## Architecture
- The RocksDB store provides configuration and message persistence in one RocksDB database.
- The AMQP 1.0 link store shares the same RocksDB environment via `RocksDBContainer`.
- Column families isolate configuration records, message data, queue entries, and link definitions.

## Column Families
- `default` used by RocksDB internals.
- `configured_objects` stores serialized configuration records.
- `configured_object_hierarchy` stores parent links for configured objects.
- `preferences` stores preference records.
- `message_metadata` stores message metadata payloads.
- `message_content` stores message content segments.
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
- Message content keys are `{messageId}{chunkIndex}` pairs.
- Queue entries are `{queueId}{messageId}` for ordered traversal by queue.
- XIDs store the format and byte arrays for global and branch ids with encoded actions.

## AMQP 1.0 Link Store Layout
- Link keys are length-prefixed UTF-8 values for `remoteContainerId` and `linkName`,
  followed by a single boolean for the role.
- Link values contain a version byte followed by AMQP-encoded `Source` and `Target` payloads.
- The link store version is stored under key `amqp_1_0_links_version` in the `version` column family.

## Versioning and Upgrades
- Link store version uses `BrokerModel` model version strings.
- On open, the link store rejects downgrades and rewrites entries for upgrades.
- Configuration and message stores currently perform no schema upgrades.

## Operations and Management
- `RocksDBManagementSupport` provides update, flush, compaction, and diagnostics.
- Statistics are available only when `enableStatistics` is true in settings.
- `updateMutableConfig` applies mutable options to live DB/column families.

## Testing
- Configuration store tests exercise create/update/remove/reload operations.
- Message store tests validate enqueue, dequeue, and transaction semantics.
- Link store tests validate persistence and delete semantics using `LinkStoreTestCase`.
