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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Executable end-state contract. These tests are opt-in until the runtime-isolation roadmap is implemented.
 */
@EnabledIfSystemProperty(named = "qpid.embedded.runIsolationContract", matches = "true")
public class EmbeddedBrokerIsolationContractTest extends UnitTestBase
{
    @Test
    public void testSequentialBrokersDoNotChangeHostJvmState() throws Exception
    {
        final Map<String, Object> result = FreshJvmBrokerProbe.run("sequential");
        final Map<String, Object> first = map(result.get("firstBroker"));
        assertNoGlobalMutation(map(first.get("activeGlobalDifference")));
        assertNoGlobalMutation(map(first.get("closedGlobalDifference")));
        assertNoGlobalMutation(map(result.get("finalGlobalDifference")));
    }

    @Test
    public void testConcurrentBrokersRemainIndependent() throws Exception
    {
        final Map<String, Object> result = FreshJvmBrokerProbe.run("concurrent");
        assertAll("concurrent broker function",
                  () -> assertEquals(Boolean.TRUE, result.get("portsAreDistinct")),
                  () -> assertEquals(Boolean.TRUE, result.get("secondBrokerRunningAfterFirstClose")),
                  () -> assertEquals(Boolean.TRUE, result.get("firstWorkDirectoryRemoved")),
                  () -> assertEquals(Boolean.TRUE, result.get("secondWorkDirectoryRemoved")));
        assertNoGlobalMutation(map(result.get("activeGlobalDifference")));
        assertNoGlobalMutation(map(result.get("afterFirstCloseGlobalDifference")));
        assertNoGlobalMutation(map(result.get("finalGlobalDifference")));
    }

    @Test
    public void testBrokerThreadsAreRuntimeAttributedAndReleased() throws Exception
    {
        final Map<String, Object> result = FreshJvmBrokerProbe.run("sequential");
        for (final String brokerKey : List.of("firstBroker", "secondBroker"))
        {
            final Map<String, Object> broker = map(result.get(brokerKey));
            assertAll(brokerKey,
                      () -> assertTrue(strings(broker.get("activeUnattributedThreads")).isEmpty(),
                                       () -> "Unattributed threads: " +
                                             strings(broker.get("activeUnattributedThreads"))),
                      () -> assertTrue(strings(broker.get("closedRuntimeAttributedThreads")).isEmpty(),
                                       () -> "Threads remaining after close: " +
                                             strings(broker.get("closedRuntimeAttributedThreads"))));
        }
    }

    private static void assertNoGlobalMutation(final Map<String, Object> difference)
    {
        final Map<String, Object> properties = map(difference.get("propertyChanges"));
        assertAll("host JVM state",
                  () -> assertTrue(strings(properties.get("added")).isEmpty(),
                                   () -> "Added properties: " + strings(properties.get("added"))),
                  () -> assertTrue(strings(properties.get("removed")).isEmpty(),
                                   () -> "Removed properties: " + strings(properties.get("removed"))),
                  () -> assertTrue(strings(properties.get("changed")).isEmpty(),
                                   () -> "Changed properties: " + strings(properties.get("changed"))),
                  () -> assertEquals(Boolean.FALSE, difference.get("protocolHandlerPackagesChanged")),
                  () -> assertEquals(Boolean.FALSE,
                                     difference.get("defaultUncaughtExceptionHandlerChanged")),
                  () -> assertEquals(Boolean.FALSE, difference.get("contextClassLoaderChanged")),
                  () -> assertEquals(Boolean.FALSE, difference.get("rootLoggingConfigurationChanged")),
                  () -> assertTrue(strings(difference.get("shutdownHooksAdded")).isEmpty(),
                                   () -> "Added shutdown hooks: " +
                                         strings(difference.get("shutdownHooksAdded"))),
                  () -> assertTrue(strings(difference.get("shutdownHooksRemoved")).isEmpty(),
                                   () -> "Removed shutdown hooks: " +
                                         strings(difference.get("shutdownHooksRemoved"))));
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
}
