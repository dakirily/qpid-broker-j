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
 */

package org.apache.qpid.systests.jms_3_1.extensions.queue;

import static org.apache.qpid.systests.EntityTypes.VIRTUALHOST;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import jakarta.jms.Session;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("policy")
@Tag("queue")
class TemporaryQueuePrefixTest
{
    @Test
    void noPrefixSet(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session.createTemporaryQueue();

            assertTrue(queue.getQueueName().startsWith("TempQueue"),
                    queue.getQueueName() + " does not start with \"TempQueue\".");
        }
    }

    @Test
    void emptyPrefix(final JmsSupport jms) throws Exception
    {
        updateGlobalAddressDomains(jms, "[]");

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session.createTemporaryQueue();

            assertTrue(queue.getQueueName().startsWith("TempQueue"),
                    queue.getQueueName() + " does not start with \"TempQueue\".");
        }
    }

    @Test
    void twoDomains(final JmsSupport jms) throws Exception
    {
        final String primaryPrefix = "/testPrefix";
        updateGlobalAddressDomains(jms, "[\"" + primaryPrefix + "\", \"/foo\" ]");

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session.createTemporaryQueue();

            assertFalse(queue.getQueueName().startsWith(("[\"" + primaryPrefix + "\", \"/foo\" ]") + "/"),
                    queue.getQueueName() + " has superfluous slash in prefix.");
            assertTrue(queue.getQueueName().startsWith(primaryPrefix),
                    queue.getQueueName() + " does not start with expected prefix \"" + primaryPrefix + "\".");
        }
    }

    @Test
    void prefix(final JmsSupport jms) throws Exception
    {
        String prefix = "/testPrefix";
        updateGlobalAddressDomains(jms, "[ \"" + prefix + "\" ]");

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session.createTemporaryQueue();

            assertTrue(queue.getQueueName().startsWith(prefix + "/"),
                    queue.getQueueName() + " does not start with expected prefix \"" + prefix + "/\".");
        }
    }

    private void updateGlobalAddressDomains(final JmsSupport jms, String globalDomains) throws Exception
    {
        final Map<String, Object> args = Map.of(QueueManagingVirtualHost.GLOBAL_ADDRESS_DOMAINS, globalDomains);
        jms.management().update(jms.virtualHostName(), VIRTUALHOST, args);
    }
}
