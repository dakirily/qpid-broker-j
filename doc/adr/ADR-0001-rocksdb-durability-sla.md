# ADR-0001: RocksDB durability/SLA profiles (single-node)

Status: Draft  
Date: 2026-01-26  
Owner: TBD

## Context
We need explicit durability guarantees and performance targets for the RocksDB store
to reach parity with the existing BDB store in a single-node deployment.

## Decision
Define three operational profiles with explicit `WriteOptions` and WAL settings, and
attach initial latency/throughput targets and crash-loss expectations. These are
initial targets and will be validated by benchmarks and crash tests.

### Profiles and parameters (initial proposal)

| Profile  | WriteOptions.sync | WriteOptions.disableWAL | DBOptions.bytesPerSync | DBOptions.walBytesPerSync |
|----------|-------------------|-------------------------|------------------------|---------------------------|
| Strict   | true              | false                   | 1048576 (1 MiB)        | 1048576 (1 MiB)           |
| Balanced | false             | false                   | 1048576 (1 MiB)        | 1048576 (1 MiB)           |
| Fast     | false             | true                    | 0 (RocksDB default)    | 0 (RocksDB default)       |

### Crash-loss expectations
- Strict: no acknowledged message loss after crash (WAL + fsync per commit).
- Balanced: possible loss of recent commits since last OS flush; bounded by WAL
  fsync behavior and OS cache (expected small window).
- Fast: possible loss of last seconds of data; WAL is disabled.

### Async commit behavior
When `commitTranAsync` is used with `writeSync=true`, commits are coalesced and WAL
sync is deferred until the batch flush (`flushWal(true)`), reducing per-transaction
fsync overhead while maintaining sync guarantees at the batch boundary.

### Initial SLA targets (to validate)
| Profile  | enqueue+commit p99 | throughput target (ops/s) |
|----------|--------------------|---------------------------|
| Strict   | <= 20 ms           | >= 3,000                  |
| Balanced | <= 10 ms           | >= 8,000                  |
| Fast     | <= 5 ms            | >= 15,000                 |

## Rationale
These profiles map directly to RocksDB durability tradeoffs and provide a clear,
documented contract for production usage. They also establish measurable targets
for performance testing.

## Consequences
- Configuration must expose `WriteOptions` and WAL settings explicitly.
- Tests must validate crash-loss expectations per profile.
- Documentation must surface the profile selection and guarantees.
- Async commit changes the durability boundary to the batch flush when `writeSync=true`.

## Follow-up work
- Add crash/restart tests to validate each profile.
- Tune targets based on benchmark results.
- Benchmark async-commit coalescing impact on latency/throughput.
