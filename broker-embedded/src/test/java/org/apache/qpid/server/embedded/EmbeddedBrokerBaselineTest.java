/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.server.embedded;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Freezes the current standalone-compatible behaviour and records known embedded broker side effects.
 */
public class EmbeddedBrokerBaselineTest extends UnitTestBase
{
    private static final String BASELINE_OUTPUT_PROPERTY = "qpid.embedded.baseline.output";

    private static Map<String, Object> _baseline;

    @BeforeAll
    public static void createFreshJvmBaseline() throws Exception
    {
        _baseline = FreshJvmBrokerProbe.run("sequential");
        final Map<String, Object> first = map(_baseline.get("firstBroker"));
        final Map<String, Object> second = map(_baseline.get("secondBroker"));
        System.out.printf("Embedded broker baseline: first startup=%d ms, threads=%d; " +
                          "second startup=%d ms, threads=%d; retained heap=%d bytes%n",
                          number(first.get("startupMillis")),
                          strings(map(first.get("activeGlobalDifference")).get("threadsAdded")).size(),
                          number(second.get("startupMillis")),
                          strings(map(second.get("activeGlobalDifference")).get("threadsAdded")).size(),
                          number(_baseline.get("totalRetainedHeapDeltaBytes")));

        final String output = System.getProperty(BASELINE_OUTPUT_PROPERTY);
        if (output != null && !output.isBlank())
        {
            final Path outputPath = Path.of(output);
            final Path parent = outputPath.toAbsolutePath().getParent();
            if (parent != null)
            {
                Files.createDirectories(parent);
            }
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(outputPath.toFile(), _baseline);
        }
    }

    @Test
    public void testSequentialBrokerLifecycleBaseline()
    {
        final Map<String, Object> first = map(_baseline.get("firstBroker"));
        final Map<String, Object> second = map(_baseline.get("secondBroker"));
        assertBrokerLifecycle(first);
        assertBrokerLifecycle(second);
    }

    @Test
    public void testKnownJvmGlobalMutationBaseline()
    {
        final Map<String, Object> first = map(_baseline.get("firstBroker"));
        final Map<String, Object> activeDifference = map(first.get("activeGlobalDifference"));
        final Map<String, Object> activePropertyChanges = map(activeDifference.get("propertyChanges"));
        assertTrue(strings(activePropertyChanges.get("added")).contains("java.protocol.handler.pkgs"),
                   "SystemLauncher should currently register its URL handler globally");
        assertTrue(strings(activePropertyChanges.get("added")).contains("qpid.version"),
                   "CommonProperties should currently add qpid.version globally");
        assertTrue(strings(activeDifference.get("shutdownHooksAdded")).contains("QpidBrokerShutdownHook"),
                   "A live broker should currently install its process shutdown hook");
        assertEquals(Boolean.TRUE, activeDifference.get("protocolHandlerPackagesChanged"));

        final Map<String, Object> afterFirst = map(_baseline.get("afterFirstBrokerGlobalDifference"));
        final Map<String, Object> finalDifference = map(_baseline.get("finalGlobalDifference"));
        assertTrue(strings(map(afterFirst.get("propertyChanges")).get("added"))
                           .contains("java.protocol.handler.pkgs"));
        assertTrue(strings(map(finalDifference.get("propertyChanges")).get("added"))
                           .contains("java.protocol.handler.pkgs"));
        assertTrue(strings(afterFirst.get("shutdownHooksAdded")).isEmpty(),
                   "Broker shutdown should remove the broker shutdown hook");
        assertTrue(strings(finalDifference.get("shutdownHooksAdded")).isEmpty(),
                   "Sequential broker shutdown should remove all broker shutdown hooks");
    }

    @Test
    public void testUnaffectedJvmGlobalStateBaseline()
    {
        final Map<String, Object> first = map(_baseline.get("firstBroker"));
        final Map<String, Object> activeDifference = map(first.get("activeGlobalDifference"));
        final Map<String, Object> finalDifference = map(_baseline.get("finalGlobalDifference"));
        assertFalse(booleanValue(activeDifference.get("defaultUncaughtExceptionHandlerChanged")));
        assertFalse(booleanValue(activeDifference.get("contextClassLoaderChanged")));
        assertFalse(booleanValue(activeDifference.get("rootLoggingConfigurationChanged")));
        assertFalse(booleanValue(finalDifference.get("defaultUncaughtExceptionHandlerChanged")));
        assertFalse(booleanValue(finalDifference.get("contextClassLoaderChanged")));
        assertFalse(booleanValue(finalDifference.get("rootLoggingConfigurationChanged")));
    }

    @Test
    public void testThreadAndMemoryDiagnosticsAreCaptured()
    {
        for (final String brokerKey : List.of("firstBroker", "secondBroker"))
        {
            final Map<String, Object> broker = map(_baseline.get(brokerKey));
            final List<String> allThreads =
                    strings(map(broker.get("activeGlobalDifference")).get("threadsAdded"));
            final List<String> attributedThreads = strings(broker.get("activeRuntimeAttributedThreads"));
            final List<String> unattributedThreads = strings(broker.get("activeUnattributedThreads"));
            assertFalse(allThreads.isEmpty());
            assertEquals(allThreads.size(), attributedThreads.size() + unattributedThreads.size());
            assertTrue(number(broker.get("heapBeforeStartBytes")) > 0L);
            assertTrue(number(broker.get("activeHeapUsedBytes")) > 0L);
            assertTrue(number(broker.get("heapAfterCloseBytes")) > 0L);
        }
    }

    @Test
    public void testStandaloneCompatibilityBaseline()
    {
        final Map<String, Object> compatibility = map(_baseline.get("standaloneCompatibility"));
        assertEquals(Set.of("AMQP_1_0"), new TreeSet<>(strings(compatibility.get("protocolVersions"))));
        assertEquals(Set.of("JSON", "Memory"),
                     new TreeSet<>(strings(compatibility.get("systemConfigTypes"))));

        final Map<String, Object> propertyPrecedence = map(compatibility.get("propertyPrecedence"));
        assertEquals("host-value", propertyPrecedence.get("existingPropertyValue"));
        assertEquals("file-value", propertyPrecedence.get("missingPropertyValue"));
        assertEquals(17L, number(compatibility.get("requestedShutdownExitCode")));
        assertEquals(Boolean.TRUE, compatibility.get("failedStartupReported"));
        assertEquals(1L, number(compatibility.get("failedStartupExitCode")));
    }

    @Test
    public void testProductionResourceDefaultsBaseline()
    {
        final Map<String, Object> environment = map(_baseline.get("environment"));
        final Map<String, Object> compatibility = map(_baseline.get("standaloneCompatibility"));
        final Map<String, Object> defaults = map(compatibility.get("resourceDefaults"));
        final long processors = number(environment.get("availableProcessors"));
        final long virtualHostWorkers = Math.max(processors * 2L, 64L);

        assertEquals(8L, number(defaults.get("portWorkerThreads")));
        assertEquals(1L, number(defaults.get("portSelectorThreads")));
        assertEquals(2L, number(defaults.get("brokerHousekeepingThreads")));
        assertEquals(virtualHostWorkers, number(defaults.get("virtualHostWorkerThreads")));
        assertEquals(Math.max(virtualHostWorkers / 8L, 1L),
                     number(defaults.get("virtualHostSelectorThreads")));
        assertEquals(4L, number(defaults.get("virtualHostHousekeepingThreads")));
    }

    private static void assertBrokerLifecycle(final Map<String, Object> broker)
    {
        assertTrue(number(broker.get("startupMillis")) >= 0L);
        assertTrue(number(broker.get("port")) > 0L);
        assertEquals(Boolean.TRUE, broker.get("workDirectoryExistsWhileActive"));
        assertEquals(Boolean.TRUE, broker.get("workDirectoryRemovedAfterClose"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(final Object value)
    {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<String> strings(final Object value)
    {
        return (List<String>) value;
    }

    private static long number(final Object value)
    {
        return ((Number) value).longValue();
    }

    private static boolean booleanValue(final Object value)
    {
        return (Boolean) value;
    }
}
