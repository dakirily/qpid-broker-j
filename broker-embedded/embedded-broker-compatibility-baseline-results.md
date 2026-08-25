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

# Embedded Broker Compatibility Baseline Results

## Scope

This document records commit 2 from the
[embedded broker runtime isolation roadmap](embedded-broker-roadmap.md). It closes the characterization gaps left by
commit 1 before runtime construction changes begin:

- concurrent functional coexistence is part of the enabled build;
- the real standalone <code>Main</code> is exercised in child JVMs;
- the standalone assembly provider set is frozen separately from the minimal embedded pack;
- Linux and Windows CI cover Java 17 and Java 21;
- portable compatibility assertions are separated from machine-specific diagnostics.

This is still not a claim that current brokers are isolated. The host-JVM contract remains opt-in and expected to fail.

## Enabled concurrent compatibility baseline

<code>EmbeddedBrokerBaselineTest</code> now launches the concurrent fresh-JVM probe in its normal
<code>@BeforeAll</code> setup. The enabled test verifies:

- both minimal brokers start and obtain positive, distinct dynamic ports;
- the second broker remains running after the first closes;
- a new TCP connection can be established to the second broker after the first closes;
- both temporary work directories are removed;
- no runtime-attributed broker threads remain after both brokers close.

Establishing a new connection is stronger than checking only the model state: it proves that closing the first broker
did not close the surviving broker's listener. Message flow and conflicting runtime settings remain later acceptance
work.

The host-isolation assertions remain in <code>EmbeddedBrokerIsolationContractTest</code>, guarded by
<code>qpid.embedded.runIsolationContract=true</code>. No failing isolation assertion was weakened or converted into a
compatibility assertion.

## Real standalone process baseline

<code>StandaloneProcessBaselineTest</code> invokes <code>org.apache.qpid.server.Main</code> in a child JVM. Test output
and test utility classes are removed from the child class path so that Broker-J production logging and service resources
win discovery. Each process receives a clean Qpid environment plus only the values required by its scenario.

| Behaviour | Frozen baseline |
|---|---|
| Version command | Reports <code>0-8, 0-9, 0-9-1, 0-10, 1.0</code> and exits with code 0 |
| Invalid store type | Reports the startup exception and exits with code 1 |
| Normal startup | Emits <code>BRK-1001</code> and reaches <code>BRK-1004</code> |
| External termination on Windows | <code>Process.destroy()</code> produces exit code 1 |
| POSIX termination | SIGTERM exits 143; the configured broker log records <code>BRK-1005</code> |
| Work-directory precedence | CLI config property, JVM property, initial property file, then process environment |

The precedence test uses four independent process starts and observes the selected value through the location of the
created JSON configuration store. Its exact ordering is:

    --config-property qpid.work_dir=...
        > -DQPID_WORK=...
        > --system-properties-file containing QPID_WORK=...
        > QPID_WORK in the child process environment

The lifecycle case supplies a minimal file broker logger. Console output during bootstrap is provided by a temporary
startup appender which is detached when startup completes, so the shutdown assertion reads the persistent broker log
rather than depending on that bootstrap-only appender.

The OS-specific termination assertions deliberately record current standalone behaviour. They do not prescribe the
future in-process embedded close API, which must never terminate the host JVM.

## Standalone provider baseline

The <code>broker</code> module is the runtime dependency root copied into the binary assembly. Its provider baseline is:

| Provider family | Types |
|---|---|
| Protocol engines | <code>AMQP_0_8</code>, <code>AMQP_0_9</code>, <code>AMQP_0_9_1</code>, <code>AMQP_0_10</code>, <code>AMQP_1_0</code> |
| System configurations | <code>BDB</code>, <code>DERBY</code>, <code>JDBC</code>, <code>JSON</code>, <code>Memory</code> |
| Transports | <code>TCPandSSL</code>, <code>Websocket</code> |

This is intentionally separate from the minimal <code>broker-embedded</code> baseline, which contains only the AMQP 1.0
protocol and the JSON and memory system-configuration providers. Later per-runtime registry work must preserve each
pack independently rather than accidentally exposing the full standalone set to minimal embedding.

## Portable assertions and diagnostics

The normal pass/fail contract contains machine-independent observations:

- lifecycle state, listener reachability, distinct ports, and cleanup;
- process exit categories and required log-message identifiers;
- configuration-source precedence;
- provider type sets;
- configured production resource formulas.

Startup duration, retained heap, live-thread counts, and thread-name inventories remain diagnostic output. Tests check
only that these measurements were captured consistently; they do not apply machine-dependent timing or heap thresholds.
The configured lightweight profile introduced later will have an explicit portable resource budget.

## Independent progress gates

| Gate | Commit 2 outcome |
|---|---|
| Functional coexistence | Pass: enabled concurrent lifecycle and survivor-listener test |
| Host-JVM isolation | Expected fail: JVM properties, URL handler packages, and live shutdown hooks still change |
| Lightweight resource budget | Fail against the roadmap target: commit 1 measured 81 live threads |
| Provider/model/allocator isolation | Not yet testable: provider packs are characterized, but registries and allocators are still global |

A green compatibility suite means only that current minimal and standalone behaviour is frozen. It does not turn the
other three outcomes green.

## CI and local verification

The repository CI matrix now supplies the required compatibility environments:

| Environment | Java versions | Command path |
|---|---|---|
| Ubuntu 24.04 GitHub Actions | 17, 21 (and 25) | Root reactor <code>mvn test</code> |
| Windows Visual Studio 2022 AppVeyor image | 17, 21 | Root reactor <code>mvn test -B -Dskip.systests=true --fail-at-end</code> |

The following focused verification was run locally on Windows 11 with Eclipse Adoptium 17.0.17:

    mvn -pl broker -Dtest=StandaloneProcessBaselineTest,StandaloneProviderBaselineTest test

Result: success; seven tests executed with no failures, errors, or skips.

    mvn -pl broker-embedded -Dtest=EmbeddedBrokerBaselineTest,EmbeddedBrokerIsolationContractTest test

Result: success; seven enabled tests passed and the three opt-in isolation-contract tests were skipped.

    mvn -pl broker,broker-embedded test

Result: success; 42 tests passed across the two affected modules and the three opt-in isolation tests were skipped.

The desired contract was also run explicitly:

    mvn -pl broker-embedded -Dtest=EmbeddedBrokerIsolationContractTest \
        -Dqpid.embedded.runIsolationContract=true test

Result: expected failure; all three isolation tests failed at the documented global-state and thread-attribution gates.
The concurrent lifecycle and survivor-listener assertions completed successfully before the global-state assertion
failed.

Cross-JDK and cross-OS results are supplied by the CI jobs when this commit is submitted. Until all four Java 17/21
Linux/Windows combinations pass, the roadmap's cross-platform exit criterion must not be reported as closed.

## Next step

The characterization milestone is now implemented. Commit 3 introduces a unique <code>BrokerRuntime</code> identity and
an immutable <code>BrokerEnvironment</code> while preserving every compatibility assertion above.
