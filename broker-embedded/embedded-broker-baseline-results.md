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

# Embedded Broker Isolation Baseline Results

## Scope

This document records the implementation and observed results of commit 1 from the
[embedded broker runtime isolation roadmap](embedded-broker-roadmap.md):

> Define the broker runtime isolation contract and freeze standalone behaviour before refactoring.

The observations were collected on 2026-08-25 from branch <code>embedded-broker</code>, based on source revision
<code>aab6003f6bb352792bea9f6b9ed44cf72ad2f1ab</code>. The roadmap work described here is uncommitted at the time of
measurement.

This is a characterization baseline, not evidence that the current implementation is isolated. The enabled tests
deliberately preserve existing standalone-compatible behaviour and identify known side effects. The desired isolation
contract is executable but opt-in because it is expected to fail until later roadmap commits remove the global state.

## What was added

The baseline consists of four test-side components:

| Component | Purpose |
|---|---|
| <code>EmbeddedBrokerIsolationProbe</code> | Starts brokers in a fresh JVM and captures lifecycle, global state, threads, providers, resource defaults, timing, and heap measurements |
| <code>FreshJvmBrokerProbe</code> | Launches the probe with a clean class-initialization boundary and reads its JSON result |
| <code>EmbeddedBrokerBaselineTest</code> | Enabled characterization and standalone-compatibility assertions |
| <code>EmbeddedBrokerIsolationContractTest</code> | Opt-in end-state assertions that remain skipped in the normal build |

The fresh process is important. Running the checks in the ordinary Maven test JVM could miss the first mutation because
another test may already have initialized <code>SystemLauncher</code>, <code>CommonProperties</code>, or a provider
registry.

Before taking the initial snapshot, the probe initializes generic JDK locale/time-zone and logging facilities. This
keeps host-owned lazy initialization, such as the JDK <code>Logging-Cleaner</code> hook and <code>user.timezone</code>,
out of the Broker-J difference.

## Environment

| Item | Observed value |
|---|---|
| Operating system | Windows 11, version 10.0 |
| Java runtime | Eclipse Adoptium 17.0.17 |
| Available processors | 16 |
| Maximum JVM heap | 17,112,760,320 bytes |
| Probe time | 2026-08-25T04:09:49.791587300Z |
| Embedded provider pack | AMQP 1.0 protocol, memory system configuration |

Timing, thread scheduling, and heap figures are machine-specific. Behavioural results and configured defaults are the
portable part of the baseline.

## Commands and outcomes

### Compile

    mvn -pl broker-embedded -DskipTests test-compile

Result: success.

### Enabled module tests

    mvn -pl broker-embedded test

Result: success.

- 13 tests discovered;
- 10 tests executed and passed;
- 3 isolation-contract tests skipped by their system-property condition;
- no failures or errors.

### Regenerate the detailed JSON baseline

PowerShell:

    mvn -pl broker-embedded -Dtest=EmbeddedBrokerBaselineTest "-Dqpid.embedded.baseline.output=target/embedded-broker-baseline.json" test

Result: success, with six enabled baseline tests passing. The JSON file is a generated build artifact under
<code>broker-embedded/target</code> and is not committed.

### Run the desired isolation contract

PowerShell:

    mvn -pl broker-embedded -Dtest=EmbeddedBrokerIsolationContractTest "-Dqpid.embedded.runIsolationContract=true" test

Result: expected failure. All three contract tests ran and failed for the known gaps documented below. Without the
system property, these tests are skipped and do not make the normal build red.

## Global-state observations

### System properties

The first broker added these JVM properties:

- <code>java.protocol.handler.pkgs</code>;
- <code>qpid.version</code>.

Both remained set after the first broker closed and after a second broker completed its lifecycle. No properties were
removed or changed.

The final URL handler package value was:

    org.apache.qpid.server.util.urlstreamhandler|org.apache.qpid.server.util.urlstreamhandler

The duplicate entry is consistent with the classpath and data URL handlers independently registering the same handler
package. The current save-and-restore approach would not make this safe under concurrent startup.

### Shutdown hooks

While the first broker was live, the probe observed one added hook:

    QpidBrokerShutdownHook

With two concurrently live brokers, it observed two hooks with this name. Each hook was removed when its broker closed;
no Broker-J shutdown hook remained after the sequential or concurrent lifecycle completed.

This is correct cleanup for the current standalone lifecycle, but it violates the embedded contract because a reusable
library installs process-lifecycle state while active.

### Other host state

The probe observed no change to:

- the JVM default uncaught-exception handler;
- the probe thread's context class loader;
- root logging level, appenders, or turbo filters.

These passing observations are now protected by the enabled baseline. Logging coverage intentionally describes the root
configuration rather than every logger object cached by the host logging implementation.

## Broker lifecycle and resource baseline

Two brokers were started and stopped sequentially in the fresh process.

| Measurement | First broker | Second broker |
|---|---:|---:|
| Startup time | 1,121 ms | 52 ms |
| New live threads at readiness | 81 | 81 |
| Runtime-attributed thread names | 72 | 72 |
| Unattributed thread names | 9 | 9 |
| Retained heap delta after close and explicit GC | 11,971,584 bytes | 27,496 bytes |
| Runtime-attributed threads after close | 0 | 0 |
| All newly observed threads after close | 0 | 0 |
| Temporary work directory removed | yes | yes |

The first retained-heap figure is primarily class loading, immutable metadata, service discovery, logging, and other
one-time JVM warming. The warm second lifecycle retained approximately 27 KiB in this run. The total retained delta from
the initial primed JVM to the end of both lifecycles was 12,032,864 bytes.

Heap results use an explicit <code>System.gc()</code> request and a short settling interval. They are diagnostic values,
not leak proofs or suitable pass/fail thresholds. A later heap-reachability test must verify that a closed
<code>BrokerRuntime</code> and its class loader become unreachable.

### Thread attribution

The 72 names attributable to the broker or virtual host include these families:

- <code>broker-&lt;broker-name&gt;-pool-*</code>;
- <code>broker-&lt;broker-name&gt;-preferences</code>;
- <code>VirtualHostNode-&lt;virtual-host-name&gt;-Config</code>;
- <code>virtualhost-&lt;virtual-host-name&gt;-iopool-*</code>;
- virtual-host selector, preference, and housekeeping names.

The nine names that cannot be assigned to a specific runtime are:

- one <code>Broker-Config</code> thread;
- the <code>IO-pool-Port-AMQP-*</code> worker threads;
- the <code>Selector-Port-AMQP</code> thread.

This makes thread dumps ambiguous when multiple brokers use the default port name. The opt-in thread contract therefore
fails until every broker-created thread contains a runtime identity.

### Configured production defaults

The baseline freezes the current defaults independently of the live-thread measurement:

| Role | Current default |
|---|---:|
| AMQP port workers | 8 |
| AMQP port selectors | 1 |
| Broker housekeeping capacity | 2 |
| Virtual-host workers | 64 on this 16-processor host |
| Virtual-host selectors | 8 |
| Virtual-host housekeeping capacity | 4 |

The virtual-host worker formula is <code>max(availableProcessors * 2, 64)</code>; selectors use
<code>max(workerCount / 8, 1)</code>. Some executor capacity is lazy, which is why configured maxima should not be
confused with the 81 threads observed at readiness.

## Standalone compatibility baseline

The probe captured the following behaviours:

| Behaviour | Baseline |
|---|---|
| Protocol providers on the embedded test classpath | exactly <code>AMQP_1_0</code> |
| System configuration providers | exactly <code>JSON</code> and <code>Memory</code> |
| Existing JVM property versus initial-properties file | existing JVM value wins |
| Missing JVM property versus initial-properties file | file value is installed |
| Requested launcher shutdown code | listener receives the requested code; probe used 17 |
| Startup failure | listener receives the failure and shutdown code 1 |

The provider assertion freezes the intentionally minimal classpath of <code>broker-embedded</code>, not every plug-in in
the full Broker-J distribution.

## Concurrent-broker characterization

The opt-in concurrent probe started two brokers at the same time in one fresh JVM. The functional checks completed
before the isolation assertions and all passed:

- both brokers bound distinct dynamic ports;
- both reached the running state;
- the second remained running after the first closed;
- both temporary work directories were removed.

The global-state contract then failed because:

- startup added <code>java.protocol.handler.pkgs</code> and <code>qpid.version</code>;
- the URL handler package value changed;
- two <code>QpidBrokerShutdownHook</code> instances were registered while both brokers were live.

This result demonstrates that multiple current brokers can coexist for this minimal scenario, but it does not establish
isolation. They still share the static provider/model/allocator state identified by the roadmap, and the probe does not
yet exercise conflicting per-broker provider or allocator settings because those settings do not exist.

## Expected contract failures

The opt-in test run reported three failing tests:

1. <code>testSequentialBrokersDoNotChangeHostJvmState</code>
   - added properties: <code>java.protocol.handler.pkgs</code> and <code>qpid.version</code>;
   - changed URL handler package setting;
   - one live <code>QpidBrokerShutdownHook</code>.
2. <code>testConcurrentBrokersRemainIndependent</code>
   - functional independence checks passed;
   - the same two properties and URL setting changed;
   - two live <code>QpidBrokerShutdownHook</code> instances.
3. <code>testBrokerThreadsAreRuntimeAttributedAndReleased</code>
   - shutdown released all measured broker threads;
   - nine active threads lacked a broker/runtime identity.

These failures are the initial red contract. Later roadmap commits should make them green rather than weakening their
assertions.

## Interpretation and next priorities

The results refine the roadmap ordering:

1. Complete the full standalone distribution, Java 21, Linux, and process-level <code>Main</code> baselines.
2. Promote concurrent functional coexistence into the enabled compatibility suite while keeping isolation gates red.
3. Introduce runtime identity and an immutable per-runtime <code>BrokerEnvironment</code>.
4. Introduce <code>BrokerServiceRegistry</code> as a separate reviewable ownership boundary.
5. Stop runtime code from registering URL handlers or populating JVM properties.
6. Move shutdown-hook ownership to standalone <code>Main</code>.
7. Establish an early typed lightweight resource plan so broader multi-broker tests do not require 81 threads per
   broker, then centralize all executor ownership later.
8. Preserve provider, property-precedence, exit-code, and production-default assertions while construction changes.

The next roadmap commit is therefore a focused baseline-extension commit. Runtime construction changes begin with
<code>BrokerRuntime</code> identity and <code>BrokerEnvironment</code> in commit 3, followed by
<code>BrokerServiceRegistry</code> in commit 4.

## Probe limitations

- Shutdown-hook inspection uses the JDK-internal <code>java.lang.ApplicationShutdownHooks</code> map. The child process
  is launched with the narrow <code>--add-opens=java.base/java.lang=ALL-UNNAMED</code> option. If a supported JDK changes
  this implementation, the probe reports hook inspection as unavailable rather than silently omitting it.
- The root logging signature covers the logger factory identity, root logger identity and level, appenders, and turbo
  filters. It is not a complete snapshot of logging-library caches.
- Thread attribution is name-based. It deliberately exposes the current ambiguous port/configuration names, but later
  runtime ownership should be reported directly by <code>BrokerResourceManager</code>.
- Retained heap numbers are noisy and include JVM warming. They are recorded for comparison, not enforced.
- The provider baseline is the minimal embedded module classpath. Additional plug-in packs require separate baselines.
- The concurrent probe verifies lifecycle independence, ports, and cleanup. Message, topology, provider, allocator, and
  failure isolation remain acceptance work for later commits.
