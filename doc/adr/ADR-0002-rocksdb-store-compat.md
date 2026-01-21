# ADR-0002: RocksDB store compatibility contract

Status: Draft  
Date: 2026-01-26  
Owner: TBD

## Context
The RocksDB store currently lacks a formal versioning and upgrade contract. We need
explicit guarantees for store format compatibility and migration support.

## Decision
Introduce a store format version key and define a compatibility policy:
- Backward-compatible reads for N-1 (previous store version).
- No downgrade support (opening with older binary fails fast).
- Explicit migration tooling for BDB -> RocksDB (offline).

## Store format versioning (proposal)
- Store format version is stored in the `version` column family with key
  `rocksdb_store_version` (uint32, big-endian).
- Initial value: `1`.
- Any incompatible change requires incrementing the version and adding an upgrader.

## Compatibility policy
- **Backward-compat**: RocksDB store code must read N-1 stores without data loss.
- **Forward-compat**: not supported; opening newer stores fails with a clear error.
- **Downgrade**: not supported.

## Migration support
- **BDB -> RocksDB**: offline migration tool (initially required for parity).
- **RocksDB -> RocksDB**: in-place upgrade on startup when an upgrader is present.
- If no upgrader exists for a detected version, startup must fail with a clear
  remediation message.

## Consequences
- A version key must be added and enforced in open/upgrade paths.
- Upgrade tests are required for every format bump.
- Documentation must describe supported migration paths and constraints.

## Follow-up work
- Implement store version key for message + configuration stores.
- Add upgrade scaffolding and first upgrader (v1 -> v2 placeholder).
- Define and implement BDB -> RocksDB migration tool.
