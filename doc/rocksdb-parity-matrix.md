# RocksDB vs BDB parity matrix (single-node, no HA)

Status: Draft  
Date: 2026-01-26  
Owner: TBD

## Legend
- Status: Done / Partial / Gap
- Priority: MUST / SHOULD / NICE

## Matrix

| Feature area | BDB capability | RocksDB status | Priority | Evidence / notes |
|--------------|----------------|----------------|----------|------------------|
| Message store basic CRUD | Durable message metadata/content storage | Done | MUST | Implemented in `RocksDBMessageStore` |
| Chunked content | Chunked message content | Done | MUST | Implemented + tests |
| Queue enqueue/dequeue | Atomic enqueue/dequeue with transactions | Done | MUST | TransactionDB usage |
| XID support | Prepared/committed XID records | Done | MUST | XID encoding in message store |
| Message id allocation | Sequence-based ID allocation | Done | MUST | TransactionDB + getForUpdate |
| Async commit | Coalescing/async commit | Done | SHOULD | `commitTranAsync` uses commit thread + batch `flushWal(true)` |
| Durability policies | Explicit sync/WAL policies | Done | MUST | WriteOptions `sync/disableWAL` wiring |
| Store size events | Overfull/underfull events | Done | SHOULD | Size tracking + event manager |
| Preferences store | Durable preferences in store | Gap | SHOULD | Uses JSON file currently |
| Configuration store | Durable config store | Done | MUST | RocksDB config store |
| Upgrade framework | Store versioning + upgrader | Done | MUST | Store version keys + upgrade checks |
| Migration BDB -> RocksDB | Supported migration path | Gap | SHOULD | Not implemented |
| Ops: flush/compact/stats | Maintenance operations | Done | SHOULD | RocksDB management support |
| Diagnostics/metrics | Stats and internal metrics | Partial | SHOULD | Stats exists, gaps in txn retry/lock metrics |
| Concurrency | Concurrent transactions | Partial | MUST | TransactionDB enabled; needs full config + perf tests |
| Tests (unit) | Broad test coverage | Partial | MUST | Fewer tests than BDB |
| Tests (systest) | System-level store tests | Gap | SHOULD | No dedicated rocksdb systests |

## Exit criteria for parity (single-node)
- All MUST items marked Done.
- No open crash-loss issues for the selected durability profile.
- Basic perf targets met for selected profile (p99 latency/throughput).
- Upgrade/versioning in place with at least one upgrade test.
- Key BDB tests ported (quota events, crash/restart, txn atomicity, stress).

## Immediate next steps
1) Add crash/restart and power-loss simulation tests for durability profiles.
2) Implement preference store parity and validate config store migration paths.
3) Expand systest coverage for RocksDB (txn atomicity, perf/latency).
