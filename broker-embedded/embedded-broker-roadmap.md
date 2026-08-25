<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Embedded Broker Runtime Isolation Roadmap

## Status

This document is a design and delivery roadmap. It does not claim that the current embedded broker API is safe for
multiple concurrent brokers in one JVM.

Commit 1, the initial embedded-path characterization, was completed on 2026-08-25. Its executable baseline and measured
results are documented in [Embedded Broker Isolation Baseline Results](embedded-broker-baseline-results.md). The next
step is commit 2, which completes the full standalone and cross-JDK baseline before runtime construction changes begin.

The roadmap addresses the embedding problems described by
[QPID-8670](https://issues.apache.org/jira/browse/QPID-8670) and the broader runtime isolation issues found in the
Broker-J code base. Its end state is:

> Multiple embedded brokers can run concurrently in one JVM without sharing mutable broker-owned state or changing
> host-JVM global state. A standalone broker retains its existing behaviour and production defaults.

The important qualification is “broker-owned”. Brokers in one JVM necessarily share the JVM heap, garbage collector,
JIT compiler, installed security providers, native libraries, and the logging implementation selected by the host
application. Complete fault containment, protection from hostile plug-ins, and survival of an out-of-memory error still
require process isolation.

## Executive conclusion

Supporting multiple embedded brokers is feasible, but it is a runtime-wide isolation project rather than a small change
to <code>SystemLauncher</code>. The current implementation contains mutable process-global state in configuration
loading, service discovery, provider registries, byte-buffer allocation, logging, failure handling, resource loading,
and some plug-ins.

Saving JVM properties before broker startup and restoring them on shutdown is not a solution. It is racy as soon as two
brokers start concurrently, cannot safely restore values changed by the host in the meantime, and does not cover static
registries, loggers, URL handlers, shutdown hooks, default exception handlers, or allocator state.

The central architectural change is to introduce one explicit <code>BrokerRuntime</code> for every broker and pass its
services through construction boundaries. The runtime owns all mutable broker facilities and has a deterministic
lifecycle. No “current runtime” thread-local or mutable global default should be introduced.

## Isolation contract

### Broker-private mutable state

Each <code>BrokerRuntime</code> must exclusively own:

- its effective configuration and property overlay;
- configured-object model and mutable type registrations;
- instantiated service providers and provider visibility;
- protocol, conversion, metadata, and management provider registries;
- byte-buffer pools, per-thread buffer caches, counters, and flow-control metrics;
- executors, schedulers, housekeeping tasks, thread factories, and exception handlers;
- caches, ID factories, helper workers, and lifecycle state;
- operational event sinks, failure policy, and process-lifecycle policy;
- plug-in instances and any mutable state created by those instances.

Closing one runtime must not change another runtime or the host application.

### State that may be shared

The following may be shared when it is immutable and does not retain a runtime:

- bytecode and class metadata;
- parsed, immutable configured-object schema metadata;
- immutable protocol constants and lookup tables;
- host-provided clocks, metrics facades, or logging facades whose ownership remains with the host.

Any shared metadata cache must contain only immutable values, be keyed safely by class loader, and never retain broker,
configured-object, provider, executor, allocator, or application instances.

### Host-JVM state

Strict embedded startup and shutdown must not mutate:

- <code>System.getProperties()</code>;
- <code>java.protocol.handler.pkgs</code> or JVM URL handler registration;
- the default uncaught-exception handler;
- the root logger, logging context, appenders, or global logging filters;
- JVM shutdown hooks;
- the thread context class loader of a caller;
- JVM-wide security providers;
- process termination state through <code>System.exit</code>, <code>Runtime.exit</code>, or
  <code>Runtime.halt</code>;
- process-global settings belonging to databases or other third-party libraries.

Tests must compare these values before startup, while multiple brokers are live, and after shutdown. Snapshot-and-restore
logic is not sufficient: the values must remain unchanged throughout the test.

## Current-state audit

The audit identified the following isolation boundaries. File and class names are listed so that each migration can be
reviewed against concrete call sites.

| Area | Current behaviour | Isolation risk | Required direction |
|---|---|---|---|
| Startup properties | <code>SystemLauncher</code> and <code>CommonProperties</code> populate JVM properties | Brokers overwrite host and one another | Build an immutable per-runtime environment |
| Resource URLs | Classpath and data URL handlers extend <code>java.protocol.handler.pkgs</code> | Process-global handler registration | Resolve supported resources through an explicit resolver |
| Service discovery | <code>QpidServiceLoader</code> reads the thread context class loader and system properties | Ambient, caller-dependent provider set | Use an explicit per-runtime service registry and class loader |
| Object model | <code>BrokerModel</code> and bootstrap model use singleton access patterns | Mutable registrations and discovery can cross runtimes | Construct a model for each runtime |
| Provider registries | Message conversion, metadata, MIME, mutator, and HTTP authentication registries are static | Providers and caches are shared by all brokers | Make registries runtime-owned and freeze after startup |
| Buffer allocation | <code>QpidByteBufferFactory</code> owns static pools, thread locals, settings, and statistics | Pool ownership, accounting, and configuration leak between brokers | Introduce an instance allocator owned by the runtime |
| Failure handling | Housekeeping and BDB HA paths can halt the JVM | A library component can kill its host application | Report to a runtime failure policy; terminate only in standalone main |
| Lifecycle | Broker startup installs shutdown hooks | Each embedded broker modifies process lifecycle | Hooks belong to the standalone entry point, never the runtime |
| Logging | Logback plug-ins modify root logger state and global filters | One broker can capture host logs or uninstall another broker's filter | Use broker-local event sinks; classify root integration as process-global |
| Database plug-ins | Derby configuration writes <code>derby.stream.error.method</code> | A broker changes process-wide database behaviour | Prefer instance APIs or reject the plug-in in strict mode |
| Executors | Executors are created across model, transport, store, and plug-in layers | Thread count and shutdown are hard to bound | Centralize creation in a runtime resource manager |
| Thread defaults | One virtual host can provision at least 64 workers and multiple selectors | An embedded test broker is unnecessarily heavy | Add explicit production and lightweight resource profiles |
| Test harness | The system-test embedded holder restores only selected properties and changes a default exception handler | It demonstrates single-broker cleanup, not concurrent isolation | Replace it with the public embedded runtime and parallel tests |

Particularly important source areas include:

- [SystemLauncher](../broker-core/src/main/java/org/apache/qpid/server/SystemLauncher.java), whose startup path currently
  loads defaults into JVM properties and installs broker process behaviour;
- <code>CommonProperties</code>, whose static initialization supplies global Qpid defaults;
- <code>QpidServiceLoader</code> and <code>PluggableFactoryLoader</code>, which form the discovery boundary;
- <code>BrokerModel</code>, <code>SystemConfigBootstrapModel</code>, and
  <code>ConfiguredObjectTypeRegistry</code>, which form the model boundary;
- <code>QpidByteBufferFactory</code> and <code>PooledByteBufferRef</code>, which form the allocator boundary;
- <code>BrokerImpl</code>, virtual-host implementations, and <code>NetworkConnectionScheduler</code>, which establish
  lifecycle and thread defaults;
- configured broker loggers, the Logback integration, Derby integration, housekeeping, and BDB HA failure paths.

The current [broker-embedded module](./) is a useful API prototype, but it starts a conventional
<code>SystemLauncher</code>, uses a temporary JSON configuration, and inherits production thread defaults and the global
state listed above. It must not be advertised as concurrently isolated until the runtime migrations are complete.

The commit 1 fresh-JVM baseline made the current boundary measurable. On the measured 16-processor Java 17 host, one
ready broker created 81 threads; 72 names contained a broker or virtual-host identity and 9 did not. Sequential close
removed all measured broker threads and the broker shutdown hook, but <code>java.protocol.handler.pkgs</code> and
<code>qpid.version</code> remained set. Two concurrently started minimal brokers bound distinct ports and survived
independent close, while still registering two process shutdown hooks and sharing the global state described above.
This proves functional coexistence for one minimal configuration, not runtime isolation.

## Progress gates

Roadmap progress is reported through four independent gates. Passing one gate must not be presented as passing another.

| Gate | Definition | Commit 1 result |
|---|---|---|
| Functional coexistence | Multiple brokers start, bind distinct ports, remain usable, and close independently | Passes for two minimal AMQP 1.0 memory-store brokers |
| Host-JVM isolation | Startup, operation, failure, and close do not mutate host-global state | Fails on properties, URL handler registration, and live shutdown hooks |
| Lightweight resource budget | A minimal broker stays within the agreed thread and memory budgets | Fails the thread target with 81 live threads on the measured host |
| Provider/model/allocator isolation | Runtimes can use conflicting providers and settings without visibility or ownership crossing | Not yet testable through the public API |

Milestones and release claims must name the gates they satisfy. In particular, functional coexistence is useful
compatibility evidence but is not a substitute for host-JVM or provider isolation.

## Target architecture

Every public embedded broker owns exactly one runtime:

    EmbeddedQpidBroker
        |
        +-- BrokerRuntime
              |
              +-- BrokerEnvironment
              +-- BrokerServiceRegistry
              +-- BrokerModel and bootstrap model
              +-- ResourceResolver
              +-- ByteBufferAllocator
              +-- BrokerResourceManager
              |     +-- transport executors and selectors
              |     +-- configuration executors
              |     +-- housekeeping scheduler
              |     +-- plug-in executors
              +-- LifecyclePolicy
              +-- FailurePolicy
              +-- OperationalEventSink

<code>BrokerRuntime</code> is created before <code>SystemConfig</code> and is closed after the configured-object tree has
closed. Runtime services are propagated through constructors and model creation APIs. Protocol engines, stores, virtual
hosts, and plug-ins obtain services from their explicit owning context, not from static accessors.

<code>SystemLauncher</code> becomes a compatibility facade over the runtime. The standalone <code>Main</code> remains the
only process boundary: it may read system properties, install a shutdown hook, configure process logging, and select a
fail-fast process policy. The reusable runtime may not perform those operations.

### BrokerEnvironment

<code>BrokerEnvironment</code> is an immutable snapshot with:

- a runtime identifier;
- an explicit class loader;
- immutable configuration defaults and overrides;
- an explicit environment-variable view where interpolation requires it;
- an explicit resource resolver;
- locale, clock, and other deterministic inputs needed by broker services;
- security limits for resource schemes, sizes, and locations.

Standalone startup creates it from the CLI, environment, and a snapshot of JVM properties using the current precedence
rules. Strict embedding starts with a minimal environment and explicitly supplied overrides. It does not silently expose
all host properties to broker configuration.

### BrokerServiceRegistry

The service registry performs discovery once for a specified class loader, validates duplicate keys and isolation
capabilities, creates runtime-local provider instances, and becomes immutable before broker objects open. This gives
deterministic startup and avoids repeated <code>ServiceLoader</code> scans on hot paths.

Generated registrations should accept a runtime or registry rather than finding one globally. Compatibility overloads
may remain temporarily, but strict embedding must use only the explicit path.

### ResourceResolver

Broker configuration should resolve <code>classpath:</code>, <code>data:</code>, and other supported resources without
installing JVM URL handlers. The resolver returns a bounded resource abstraction or stream and applies:

- a scheme allow-list;
- maximum decoded size;
- class-loader and base-directory boundaries;
- redirect and network-access policy;
- secret-safe error reporting.

Standalone compatibility can enable the same legacy schemes through this resolver. Strict embedded defaults should not
permit network resource loading and should restrict file access unless the caller opts in.

### LifecyclePolicy and FailurePolicy

Lifecycle policy decides whether process integration is permitted. The embedded policy installs no shutdown hooks and
never changes host handlers. The standalone policy is selected only by <code>Main</code>.

Failure policy receives structured failures and decides whether to:

- record and continue;
- make a configured object enter an error state;
- close the affected runtime;
- complete a caller-visible failure stage exceptionally;
- request process termination from the standalone adapter.

Core code and plug-ins must never directly halt or exit the JVM.

### ByteBufferAllocator

The allocator owns its pool, thread caches, reference counters, leak diagnostics, and close state. Every pooled reference
records its allocator owner and can only return memory to that owner. Allocator statistics used by flow-to-disk decisions
must be read from the owning virtual host's runtime.

An embedded profile may disable pooling or use a small bounded pool. Production keeps today's tuned defaults unless a
benchmark supports changing them. Instance-level thread caches must be removable during runtime shutdown and must not
retain a runtime through arbitrary application threads.

### BrokerResourceManager

All broker-created executors, schedulers, selector groups, and thread factories are obtained from one runtime-owned
resource manager. It provides:

- typed executor roles and configuration;
- thread names containing the runtime identifier and role;
- per-thread uncaught-exception handlers without changing the JVM default;
- lazy start and optional worker prestart;
- ownership tracking and deterministic close order;
- shutdown deadlines and diagnostics listing threads that failed to stop;
- metrics for configured capacity, active threads, queue depth, and rejected work.

The manager is an ownership mechanism, not one shared executor for all work. Workloads that require isolation or ordering
retain separate executors, but their creation and lifecycle become visible and configurable.

## Design rules

The implementation should follow these rules throughout the migration:

1. Dependency propagation is explicit. Do not introduce a thread-local “current broker”, static runtime map, or mutable
   default runtime.
2. Compatibility adapters point into the new architecture; the new architecture must never call back into legacy global
   facades.
3. Runtime configuration is validated and frozen before configured objects open.
4. Provider discovery happens at startup, not on message-processing hot paths.
5. Every mutable resource has one owner and one close path.
6. Closing is idempotent and proceeds from callers toward dependencies: listeners, connections, protocol work, model
   objects, stores, executors, then allocator and registry state.
7. Strict embedded mode fails closed when a plug-in cannot demonstrate runtime isolation.
8. Standalone defaults remain unchanged until tests and benchmarks justify an intentional improvement.
9. Each commit is independently buildable and includes focused regression tests.
10. Deprecations should include a migration replacement and a removal target, not merely hide global access.

## Commit roadmap

The commits below form an ordered chain. A commit may be split mechanically if review size requires it, but its stated
invariant should be established before work starts on the next dependent step.

### Phase 0 - Define and measure the boundary

#### Commit 1 - test/docs: define broker runtime isolation contract and freeze standalone behaviour (completed)

Add executable tests and baselines before changing construction:

- snapshot all relevant host-global state;
- start and stop two current broker instances sequentially to expose known mutations;
- capture standalone configuration precedence, default provider set, default transport sizing, and exit behaviour;
- introduce thread ownership diagnostics based on runtime-specific name prefixes;
- record startup time, post-readiness thread count, and retained memory baselines;
- document the allowed immutable JVM sharing and the process-isolation limitations.

The isolation tests are opt-in expected failures with issue references. The enabled characterization tests protect
current property precedence, minimal provider visibility, production resource defaults, exit-code propagation, and
sequential lifecycle behaviour.

Result: completed for the minimal embedded critical path. The baseline found persistent mutation of
<code>java.protocol.handler.pkgs</code> and <code>qpid.version</code>, one shutdown hook per live broker, 81 live threads
per ready broker on the measured 16-processor host, and 9 thread names without runtime attribution. Concurrent
functional coexistence passed for two minimal brokers. See
[Embedded Broker Isolation Baseline Results](embedded-broker-baseline-results.md).

Exit criterion: met for the embedded path. Full standalone distribution and cross-JDK coverage is completed by commit 2.

#### Commit 2 - test/docs: extend standalone and cross-JDK compatibility baselines

Close the remaining characterization gaps before changing runtime construction:

- move concurrent functional lifecycle assertions into the enabled compatibility baseline while keeping host-isolation
  assertions opt-in;
- launch the real standalone <code>Main</code> in a subprocess and freeze CLI/environment/property precedence, process
  exit codes, shutdown signals, startup logging, and failure behaviour;
- record the provider set of the assembled standalone distribution, separately from the minimal embedded provider pack;
- run the baseline on Java 17 and Java 21 and on the primary Linux and Windows CI environments;
- distinguish machine-independent compatibility assertions from diagnostic timing, thread, and heap measurements;
- retain the four progress gates as separate CI and reporting outcomes.

Exit criterion: both the minimal embedded path and the full standalone assembly have passing compatibility baselines on
supported JDKs, while the desired host-isolation contract remains explicitly red.

### Phase 1 - Introduce the runtime seam

#### Commit 3 - core: add BrokerRuntime identity and immutable BrokerEnvironment

Introduce runtime identity and environment values without changing default startup:

- a unique, stable runtime identifier available before any broker-owned resource is created;
- immutable builders and validated environment snapshots;
- explicit class-loader selection;
- immutable configuration defaults, overrides, locale, clock, and environment views;
- compatibility factories that reproduce current system-property and TCCL input behaviour for standalone startup
  without writing host state.

Do not migrate all consumers in this commit. It establishes explicit ownership and tests immutability, concurrent
construction, snapshot semantics, and runtime identifier uniqueness.

Exit criterion: two environments can coexist with conflicting values and class loaders, and every later runtime-owned
resource can receive a stable owner identity.

#### Commit 4 - core: add immutable BrokerServiceRegistry

Introduce a registry owned by one runtime:

- explicit discovery through the environment class loader;
- a populate, validate, and freeze lifecycle;
- runtime-local provider instances by default;
- deterministic indexes and duplicate-provider diagnostics;
- immutable shared-provider declarations only for reviewed stateless implementations;
- compatibility construction that preserves the current standalone provider view.

Do not migrate all service consumers in this commit. Test concurrent registries with different provider class loaders,
disabled providers, duplicate logical names, and independent close.

Exit criterion: two registries can coexist with different provider sets and neither retains or exposes the other
runtime.

#### Commit 5 - core/codegen: add runtime-aware SystemConfigFactory API

Extend <code>SystemConfigFactory</code> and generated factory code to accept the environment and service registry.
Preserve the old signature as a deprecated standalone-compatible adapter.

Update annotation-processor tests and generated-source tests. Generated code should store no static runtime reference.

Exit criterion: a system configuration can be built entirely from explicit runtime inputs.

#### Commit 6 - core: construct bootstrap and broker models per runtime

Replace singleton model acquisition in the runtime-aware path:

- create <code>SystemConfigBootstrapModel</code>, <code>BrokerModel</code>, and mutable type registries per runtime;
- distinguish immutable schema metadata from runtime registrations;
- ensure parent-child model navigation remains local to the runtime;
- keep legacy singleton access only behind deprecated compatibility entry points.

Add a test that gives two runtimes different plug-in registrations and verifies that neither model sees the other's
types.

Exit criterion: configured-object factories and registrations are scoped to their owning runtime.

#### Commit 7 - core: make property loading and interpolation runtime-scoped

Move default loading and interpolation away from JVM mutation:

- make Qpid defaults regular immutable data;
- replace direct system-property reads with <code>BrokerEnvironment</code> lookups;
- preserve standalone precedence by importing a JVM-property snapshot at the process boundary;
- provide explicit missing-value and secret-redaction rules;
- remove startup writes performed by <code>populateSystemPropertiesFromDefaults</code> from the runtime-aware path.

Audit static initializers so merely loading a broker class cannot change system properties.

Exit criterion: starting a runtime with custom defaults changes no JVM property, including <code>qpid.version</code> and
<code>java.protocol.handler.pkgs</code>.

#### Commit 8 - core/security: replace global URL handlers with ResourceResolver

Introduce the bounded resource abstraction and migrate configuration, key-store, trust-store, and classpath/data resource
consumers. Remove URL handler package registration from reusable code.

Keep compatibility parsing for existing broker configuration strings. Reject disallowed schemes before opening a
resource and test maximum sizes, traversal attempts, redirects, malformed data URIs, and credential redaction.

Exit criterion: all resources needed by the normal memory-store AMQP startup path resolve without JVM URL handler
changes.

#### Commit 9 - core: introduce runtime LifecyclePolicy and FailurePolicy

Move process control out of reusable broker code:

- remove unconditional shutdown-hook registration from runtime startup;
- remove changes to the JVM default exception handler;
- route housekeeping and BDB HA fatal paths through <code>FailurePolicy</code>;
- make asynchronous startup failure observable to the embedding caller;
- implement process hook and termination adapters in standalone <code>Main</code>.

Unit tests must use a fake process adapter; tests must never call exit or halt.

Exit criterion: embedded startup, failure, and shutdown cannot install hooks or terminate the JVM, while standalone
behaviour remains compatible.

### Phase 2 - Establish an early embedded resource budget

#### Commit 10 - embedded/core: add typed production and lightweight resource plans

Introduce the resource-setting abstraction early enough to make continued multi-broker testing practical:

- define immutable production, lightweight, and custom plans associated with one runtime identity;
- preserve the measured standalone defaults exactly in the production plan;
- map lightweight port, virtual-host, selector, housekeeping, and preference settings onto existing managed attributes
  and context values rather than JVM properties;
- use two workers and one selector as the initial minimum where the transport requires workers to exceed selectors;
- make currently configurable core thread names include the runtime identity;
- expose configured capacity and observed live-thread diagnostics through the embedded test facade.

This is an early critical-path slice, not the final resource architecture. It must not introduce a second executor
framework, silently merge workloads with different ordering requirements, or claim ownership of plug-in-created
threads. Commit 18 later centralizes creation and deterministic shutdown through <code>BrokerResourceManager</code>.

Exit criterion: the production plan preserves the commit 1 compatibility baseline, while one ready lightweight
AMQP 1.0 memory-store broker uses no more than 10 live broker-owned threads and passes the existing JMS lifecycle test.

### Phase 3 - Isolate discovery and providers

#### Commit 11 - core: route configured-object and protocol discovery through BrokerServiceRegistry

Migrate <code>QpidServiceLoader</code>, <code>PluggableFactoryLoader</code>, configured-object registrations, protocol
engine creators, transport providers, system-config providers, and authentication factories.

The registry should produce immutable indexed views for lookup performance. Duplicate logical names must be rejected
with diagnostics that identify both provider classes and their source locations.

Exit criterion: a strict runtime does not use TCCL or direct <code>ServiceLoader</code> calls after construction.

#### Commit 12 - plugins: replace static message and management provider registries

Convert at least the following families to runtime services:

- message converter registry;
- message format and metadata type registries;
- MIME content converter registry;
- server-message mutator factories;
- HTTP authentication-method collections;
- management converter and serializer registries found by the audit.

Provider objects should be instantiated per runtime unless explicitly declared immutable and stateless. Registry
contents are frozen before network listeners open.

Exit criterion: two brokers can expose different message and management providers without visibility across runtimes.

#### Commit 13 - core/plugins: eliminate ambient broker configuration reads

Audit production code for:

- <code>System.getProperty</code>, <code>System.setProperty</code>, <code>Boolean.getBoolean</code>, and
  <code>Integer.getInteger</code>;
- environment-variable reads;
- TCCL access;
- direct service loading;
- static mutable provider caches.

Move broker-specific reads to environment or typed runtime settings. Keep only genuinely JVM-descriptive reads in
well-named host capability code, and document why each remaining use is safe.

Add a build-time forbidden-API rule for new uses in runtime packages, with a narrow reviewed allow-list for standalone
entry points.

Exit criterion: the embedded critical path contains no ambient configuration reads.

### Phase 4 - Isolate memory and hot-path state

#### Commit 14 - buffer: extract instance ByteBufferAllocator and owner-aware pooled references

Create the allocator interface and implementation:

- instance pool configuration and accounting;
- allocator-owned per-thread cache policy;
- owner identity on pooled references;
- deterministic drain and close;
- use-after-close and wrong-owner checks in diagnostic mode;
- small-buffer, direct-buffer, and pooling controls.

Keep <code>QpidByteBufferFactory</code> temporarily as a deprecated compatibility facade, but prevent new code from using
it.

Exit criterion: allocator unit tests can run two differently configured pools concurrently without shared counters or
returned buffers crossing ownership.

#### Commit 15 - transport/amqp1/memory: migrate the embedded critical path to runtime allocator

Pass the allocator through transport connections, protocol engines, AMQP 1.0 message handling, the memory store, and
flow-to-disk calculations required by the default embedded profile.

Avoid per-operation registry lookup. Resolve the allocator once into long-lived connection or virtual-host objects.
Benchmark allocation, encode/decode throughput, and small-message latency against the static implementation.

Exit criterion: the default AMQP 1.0 plus memory-store embedded broker does not call the static allocator facade.

#### Commit 16 - protocols/stores: migrate remaining buffer consumers

Migrate AMQP 0-x protocols, message conversion, persistent stores, WebSocket, management, and optional plug-ins. Add a
temporary test that fails if a strict runtime reaches <code>QpidByteBufferFactory</code>.

Remove or reduce the facade after all supported runtime paths have explicit ownership.

Exit criterion: all broker distributions use owner-aware runtime allocation and global pool deinitialization is no
longer required.

#### Commit 17 - core/plugins: scope workload caches, IDs, and helper workers

Audit other mutable statics and implicit singletons, including:

- UUID and ID caches;
- compiled selector and conversion caches;
- MIME and metadata helper maps;
- compression or codec helpers with mutable buffers;
- store and plug-in background workers;
- metrics registries that retain broker objects.

Make broker-dependent caches runtime-owned and bounded. Share only immutable values. Document cache limits and clear
runtime-owned caches during close.

Exit criterion: a heap-retention test can close one runtime while another remains live, and the closed runtime becomes
unreachable after caller references are released.

### Phase 5 - Complete deterministic resource ownership

#### Commit 18 - core: centralize executor creation and deterministic shutdown

Introduce <code>BrokerResourceManager</code> and migrate transport schedulers, configuration executors, preference
executors, housekeeping, and core helper pools. Then migrate plug-in executors incrementally.

Do not silently merge executors with different ordering or blocking requirements. Instead, give each role an explicit
configuration and owner. Remove eager prestart where it is not required for latency.

Add shutdown tests with pending work, blocked tasks, failed tasks, interrupted close, and repeated close.

Exit criterion: every core broker thread has a runtime owner, role, configurable bound, and deterministic shutdown path.

#### Commit 19 - core: complete production and lightweight resource profiles

Complete the typed plans introduced by commit 10 and make the resource manager their sole execution mechanism:

- migrate remaining core and compatible plug-in executor roles into the effective plan;
- keep <strong>production</strong> identical to the frozen standalone sizing and prestart behaviour;
- keep <strong>lightweight</strong> within its thread budget while retaining protocol correctness and useful test
  performance;
- report configured capacity, lazy versus started roles, active threads, and shutdown status by runtime identity;
- remove temporary attribute translation that is no longer needed after resource-manager adoption.

Every numeric value remains overridable. Validate relationships such as worker count being greater than selector count.
Benchmark startup, first-connection latency, throughput, and tail latency against both the commit 1 baseline and the
early commit 10 profile.

Exit criterion: every supported broker thread is represented by the effective plan, an idle lightweight broker remains
within the agreed budget, and production performance stays within the accepted benchmark tolerance.

### Phase 6 - Contain integrations and publish the API

#### Commit 20 - logging/plugins: enforce isolation capabilities and leave host logging untouched

Add an isolation-capability declaration for plug-ins:

| Capability | Meaning in strict embedding |
|---|---|
| <code>RUNTIME_ISOLATED</code> | May be loaded; mutable state and resources belong to the runtime |
| <code>IMMUTABLE_SHARED</code> | May be shared; implementation is stateless and immutable |
| <code>PROCESS_GLOBAL</code> | Rejected; requires ownership of host-JVM state |
| <code>UNKNOWN</code> | Rejected until reviewed |

Convert normal broker logging to a runtime-owned operational event sink. A caller can bridge events to SLF4J without the
broker changing root configuration. Keep Logback root configuration only as an explicit standalone process feature.

Classify the Derby global stream setting and similar integrations as process-global until redesigned. Strict mode should
report the exact incompatible plug-in and remediation instead of silently accepting it.

Exit criterion: starting and closing embedded brokers leaves the host logging context unchanged and incompatible plug-ins
fail before listeners open.

#### Commit 21 - embedded: switch EmbeddedQpidBroker to strict BrokerRuntime

Make the module a thin, public facade over the isolated runtime. The builder should offer:

- <code>production</code>, <code>lightweight</code>, and custom resource profiles;
- typed selector, worker, housekeeping, buffer-pool, and shutdown settings;
- loopback binding and dynamic ports by default;
- explicit provider packs, with AMQP 1.0 plus memory store as the minimal default;
- in-memory initial configuration, avoiding a mandatory temporary JSON file;
- a small DSL for virtual hosts, exchanges, queues, and bindings;
- caller-visible startup, failure, readiness, and close stages;
- failure and process-termination policies that are safe for libraries;
- secret values whose string forms are redacted.

The JUnit extension should:

- create one broker per test or per test class as requested;
- support parallel test execution;
- inject connection information without setting system properties;
- close deterministically and report leaked threads or resources;
- use dynamic ports and unique runtime identifiers;
- never replace the JVM default exception handler.

Exit criterion: the public API starts multiple strict runtimes without using legacy global facades.

### Phase 7 - Prove the end state

#### Commit 22 - test/docs: prove multi-broker isolation and standalone parity

Enable the isolation suite and make it a release gate. Run at least:

- two and four brokers starting concurrently;
- brokers with conflicting property overlays, provider sets, locales, credentials, buffer settings, and thread profiles;
- simultaneous client traffic, topology creation, management operations, and shutdown;
- closing one broker while others continue traffic;
- repeated start/close cycles and failure during partial startup;
- class-loader release tests;
- standalone smoke, upgrade, protocol, store, and performance profiles on supported JDKs.

Publish the supported plug-in matrix, resource-setting reference, embedding limitations, migration guide, and examples.

Exit criterion: all final acceptance criteria below pass in CI and the module can be documented as multi-broker safe.

## Resource profiles and thread budget

The commit 1 fresh-JVM baseline observed 81 newly live threads for each ready minimal broker on a 16-processor host.
Seventy-two names included the broker or virtual-host identity; nine port or configuration names did not. All measured
threads stopped after close, so the immediate finding is excessive footprint and ambiguous ownership rather than a
sequential thread leak. The measurement is diagnostic, while the configured defaults and profile budgets below are the
portable compatibility contract.

The current defaults are appropriate for a standalone production process but excessive for test infrastructure. A
virtual host can default to at least 64 network workers and approximately one selector per eight workers; a port commonly
adds eight workers and one selector. Several configuration, preference, and housekeeping executors add further capacity.
The network scheduler eagerly prestarts workers, so the cost is visible even before meaningful traffic.

The first lightweight profile should be conservative:

| Role | Current production-style default | Initial lightweight target |
|---|---:|---:|
| Port/network workers | 8 | 2 |
| Port selectors | 1 | 1 |
| Virtual-host workers | at least 64 | 2 |
| Virtual-host selectors | at least 8 | 1 |
| Configuration execution | 2 dedicated roles | preserve ordering with no more than 2 live threads |
| Preference execution | 2 dedicated roles | lazy or caller/configuration-thread execution where safe |
| Broker housekeeping capacity | 2 | shared runtime scheduler with 1 thread |
| Virtual-host housekeeping capacity | 4 per host | use the runtime housekeeping scheduler by default |

Commit 10 establishes an early target of no more than ten live broker-owned threads for one ready, idle, minimal broker
using existing managed configuration points. Commit 19 completes ownership and targets approximately seven where the
resource manager can safely keep roles lazy. Exact accounting depends on which roles can remain lazy and whether
selectors are shared by transport scope. The effective resource plan must report both configured capacity and actually
started threads, so tests do not confuse a pool's maximum size with its idle footprint.

Workers must remain greater than selectors wherever the transport algorithm requires it. A two-worker/one-selector
configuration is therefore a safe initial minimum. A later, independently benchmarked change could share a network
reactor across roles inside one runtime and reduce the footprint further, but cross-runtime executors should not be
shared merely to obtain a smaller headline count.

Required resource tests:

- lightweight idle thread count after readiness has stabilized;
- first-connection latency with lazy workers;
- throughput and tail latency with one and several test clients;
- fairness when two runtimes have different worker limits;
- housekeeping progress under transport load;
- no live runtime-owned threads after close;
- production profile parity and performance regression thresholds.

## Embedded API direction

The public facade should stay small and typed. An illustrative shape is:

    EmbeddedQpidBroker broker =
            EmbeddedQpidBroker.builder()
                    .resourceProfile(ResourceProfile.lightweight())
                    .listenOnLoopback(0)
                    .virtualHost("test", virtualHost -> virtualHost
                            .directExchange("events")
                            .queue("orders")
                            .bind("events", "orders", "orders.created"))
                    .build();

    broker.start();
    URI endpoint = broker.getAmqpUri();
    broker.close();

This is illustrative rather than a frozen API. The DSL should translate into the same configured-object operations used
by management, preserving validation, access control, lifecycle ordering, and future extensibility. It should not create
an independent topology model that bypasses Broker-J semantics.

Advanced callers should be able to supply an initial configuration document or use configured-object operations after
startup. The convenience DSL should cover common test topology, not every broker attribute.

## Security model

Embedding changes the trust boundary: broker code and application code have the same process permissions. The strict
profile should therefore:

- bind only to loopback unless an external interface is explicitly requested;
- prefer a dynamic port and report the selected endpoint;
- disable HTTP management and optional protocols unless selected;
- require explicit authentication choices and redact credentials in logs, exceptions, and object string forms;
- restrict resource schemes, paths, decoded sizes, and network access;
- avoid importing the host's complete property and environment maps by default;
- reject unknown or process-global plug-ins before any listener opens;
- close partially constructed resources if validation or startup fails;
- expose no process termination capability to plug-ins.

Plug-in capability declarations are not a security sandbox. Java's in-process boundary cannot safely contain malicious
or arbitrary third-party bytecode. Untrusted plug-ins, native libraries, incompatible database engines, and workloads
requiring independent memory or crash containment must use separate processes or containers.

## Standalone compatibility contract

The standalone broker remains the primary production assembly. Its compatibility path should:

1. parse the current CLI and environment inputs;
2. snapshot JVM properties into a production <code>BrokerEnvironment</code>;
3. preserve current precedence, substitution, defaults, provider visibility, ports, and resource sizing;
4. build the same <code>BrokerRuntime</code> used by embedding;
5. install process logging, shutdown hooks, and process failure behaviour only in <code>Main</code>;
6. close the runtime through the same deterministic lifecycle.

Compatibility tests should cover:

- configuration files and startup scripts from supported upgrade sources;
- CLI option and property precedence;
- protocol and store discovery;
- logging output and process exit codes;
- shutdown signals;
- production thread sizing;
- supported Java 17 and Java 21 builds;
- all normal system-test profiles.

Where current standalone behaviour depends on an unsafe global side effect, moving that effect to the process boundary
is considered a compatible implementation change. If duplicate providers are currently resolved by incidental
classpath order, standalone can initially warn and preserve the selected provider while strict embedding rejects the
ambiguity. A later release may tighten standalone validation through the normal deprecation process.

## Final acceptance criteria

The roadmap is complete only when all of the following are demonstrated.

### Concurrent functional isolation

- At least four brokers can start concurrently in one JVM.
- Each can use a different property overlay, provider pack, credentials, locale, allocator configuration, and resource
  profile.
- Queues, exchanges, bindings, messages, connections, statistics, management objects, and failures remain local to the
  owning broker.
- Closing or failing one broker does not interrupt traffic through another.

### Host-global invariants

During startup, normal operation, failure, and shutdown:

- JVM properties are unchanged;
- URL handler configuration is unchanged;
- the default uncaught-exception handler is unchanged;
- root logging configuration and filters are unchanged;
- no embedded shutdown hook is installed;
- caller thread context class loaders are unchanged;
- no process exit or halt method is invoked.

### Provider and model isolation

- Different runtimes may expose different protocols and plug-ins.
- A provider added to one runtime is invisible to every other runtime.
- Duplicate and incompatible providers fail deterministically before listener startup.
- Closing a runtime releases its class loader when the caller releases all references.

### Allocator isolation

- Buffers are returned only to their originating allocator.
- Pool size, direct-memory accounting, leak statistics, and flow-to-disk decisions are runtime-local.
- Closing one allocator has no effect on live connections in another runtime.
- No runtime-owned thread-local buffer survives close.

### Resource and lifecycle guarantees

- Selector, worker, configuration, preference, and housekeeping counts are typed and configurable.
- The minimal lightweight profile stays within the agreed idle thread budget.
- Every broker-created thread is named and attributed to one runtime.
- Close is idempotent and leaves no runtime-owned thread or scheduled task alive.
- Partial startup failure releases ports, files, stores, executors, and buffers.

### Standalone parity

- Production configuration and provider defaults remain compatible.
- Protocol, store, system, upgrade, and management tests pass.
- Startup time, throughput, latency, and memory stay within agreed regression limits.
- Shutdown signals, exit status, and production logging remain correct.

### Preventing regressions

CI should reject unreviewed uses in reusable runtime code of:

- JVM property mutation;
- default exception-handler mutation;
- URL handler registration;
- shutdown-hook registration;
- process exit or halt;
- direct <code>ServiceLoader</code> or ambient TCCL discovery;
- unmanaged executor or raw thread creation;
- mutable static provider registries;
- the legacy static buffer allocator.

A small allow-list may exist for the standalone process adapter and documented JVM capability probes. Every exception
must name its owner and reason.

## Delivery milestones

The chain has four externally meaningful milestones:

1. <strong>Characterized boundary, commits 1-2.</strong> Minimal embedded and full standalone behaviour are frozen on
   supported JDKs. Functional coexistence is measured separately from the still-failing isolation gates.
2. <strong>Runtime foundation and practical test footprint, commits 3-10.</strong> Configuration, resources, lifecycle,
   and failure handling have explicit runtime ownership; the minimal path no longer requires the measured host-global
   mutations and an early lightweight plan permits broader parallel validation.
3. <strong>Isolated minimal broker, commits 11-19.</strong> The AMQP 1.0 memory-store path owns providers, buffers,
   caches, and threads and meets the complete lightweight resource budget. It is suitable for internal parallel
   validation, but plug-in support remains capability-dependent.
4. <strong>Supported public embedding, commits 20-22.</strong> Plug-in isolation is enforced, the public builder and
   JUnit extension use strict runtime construction, and the full acceptance matrix passes.

The module must not claim multi-broker isolation at milestone 1 merely because functional coexistence passes. Milestone
2 may be described as an internal runtime foundation, not a supported embedding guarantee. A limited experimental label
is reasonable at milestone 3 if the supported provider pack is stated precisely. General support begins only at
milestone 4.

## Review and maintenance guidance

Each commit review should answer:

- What owns every new mutable object?
- How is that owner passed to its consumer?
- What closes it, and in what order?
- Can it retain a class loader or runtime after close?
- Does it read or write ambient JVM state?
- Is it accessed on a message-processing hot path?
- Can a plug-in bypass the boundary?
- Does standalone behaviour remain covered?

Architecture documentation should be updated when a new executor role, provider family, process-global integration, or
resource scheme is added. The isolation test suite and forbidden-API checks should make these questions routine rather
than relying on periodic static-state audits.
