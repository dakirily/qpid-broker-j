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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class EmbeddedQpidBrokerTest extends UnitTestBase
{
    @Test
    public void testStartAndClose()
    {
        final EmbeddedQpidBroker broker = EmbeddedQpidBroker.builder()
                .brokerName(getTestName())
                .virtualHost("application-test")
                .credentials("test-user", "test-password")
                .exchange("events", ExchangeType.TOPIC)
                .queue("orders")
                .binding("events", "orders", "order.*")
                .build();

        assertFalse(broker.isRunning());
        assertThrows(IllegalStateException.class, broker::getAmqpAddress);
        assertThrows(IllegalStateException.class, broker::getWorkDirectory);

        final Path workDirectory;
        try
        {
            assertSame(broker, broker.start());
            assertTrue(broker.isRunning());
            workDirectory = broker.getWorkDirectory();
            assertTrue(Files.isDirectory(workDirectory));

            final InetSocketAddress address = broker.getAmqpAddress();
            assertEquals("127.0.0.1", address.getHostString());
            assertTrue(address.getPort() > 0);
            assertEquals("amqp", broker.getAmqpUri().getScheme());
            assertEquals(address.getPort(), broker.getAmqpUri().getPort());
            assertEquals("application-test", broker.getVirtualHostName());
            assertEquals("test-user", broker.getUsername());
            assertEquals("test-password", broker.getPassword());
        }
        finally
        {
            broker.close();
        }

        assertFalse(broker.isRunning());
        assertFalse(Files.exists(workDirectory));
        broker.close();
        assertThrows(IllegalStateException.class, broker::start);
    }

    @Test
    public void testFailedTopologyCreationCleansUp()
    {
        final EmbeddedQpidBroker broker = EmbeddedQpidBroker.builder()
                .brokerName(getTestName())
                .binding("missing-exchange", "missing-queue", "key")
                .build();

        assertThrows(EmbeddedBrokerException.class, broker::start);
        assertFalse(broker.isRunning());
        assertFalse(Files.exists(broker.getWorkDirectory()));
        broker.close();
    }

    @Test
    public void testBuilderRejectsInvalidConfiguration()
    {
        assertThrows(IllegalArgumentException.class, () -> EmbeddedQpidBroker.builder().port(-1));
        assertThrows(IllegalArgumentException.class, () -> EmbeddedQpidBroker.builder().protocols());
        assertThrows(IllegalArgumentException.class,
                     () -> EmbeddedQpidBroker.builder().queue("orders").queue("orders"));
        assertThrows(IllegalArgumentException.class,
                     () -> EmbeddedQpidBroker.builder()
                             .exchange("events", ExchangeType.DIRECT)
                             .exchange("events", ExchangeType.TOPIC));
    }
}
