/*
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

package org.apache.qpid.systests.jms_3_1.connection;

import static org.apache.qpid.systests.EntityTypes.CONNECTION;
import static org.apache.qpid.systests.EntityTypes.VIRTUALHOST;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import org.apache.qpid.systests.support.AsyncSupport;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.apache.qpid.tests.utils.VirtualhostContextVariable;

@JmsSystemTest
@Tag("connection")
class SpontaneousConnectionCloseTest
{
    private CompletableFuture<JMSException> _connectionCloseFuture;

    @BeforeEach
    void setUp()
    {
        _connectionCloseFuture = new CompletableFuture<>();
    }

    @Test
    @VirtualhostContextVariable(name = "virtualhost.connectionThreadPool.size", value = "4")
    @VirtualhostContextVariable(name = "virtualhost.connectionThreadPool.numberOfSelectors", value = "2")
    @VirtualhostContextVariable(name = "virtualhost.housekeepingThreadCount", value = "2")
    void explicitManagementConnectionClose(final JmsSupport jms) throws Exception
    {
        assumeTrue(jms.brokerAdmin().isManagementSupported());

        try (final var connection = jms.builder().connection().create())
        {
            connection.setExceptionListener(_connectionCloseFuture::complete);
            connection.start();
            final Map<String, String> result = jms.management()
                    .perform(connection, "", "getConnectionMetaData", VIRTUALHOST, Map.of());
            final String connectionId = result.get("connectionId");
            jms.management().perform(connectionId, "DELETE", CONNECTION, Map.of("identity", connectionId));

            assertClientConnectionClosed(connection);
        }
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void brokerRestartConnectionClose(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        assumeTrue(jms.brokerAdmin().supportsRestart());
        try (final var con = jms.builder().connection().failover(false).create())
        {
            con.setExceptionListener(_connectionCloseFuture::complete);

            async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

            assertClientConnectionClosed(con);
        }
    }

    private void assertClientConnectionClosed(final Connection con) throws Exception
    {
        _connectionCloseFuture.get(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS);

        assertThrows(JMSException.class,
                () -> con.createSession(false, Session.AUTO_ACKNOWLEDGE),
                "Connection close ought to invalidate further use");
    }
}
