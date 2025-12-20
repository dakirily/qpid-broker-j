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

package org.apache.qpid.systests.jms_3_1.queueconnection;

import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.QueueConnection;
import jakarta.jms.Session;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("legacy")
@Tag("queue")
class QueueSessionTest
{
    @Test
    void queueSessionCannotCreateTemporaryTopics(final JmsSupport jms) throws Exception
    {
        try (final var queueConnection = jms.builder().connection().create(QueueConnection.class);
             final var queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            assertThrows(jakarta.jms.IllegalStateException.class,
                    queueSession::createTemporaryTopic,
                    "expected exception did not occur");
        }
    }

    @Test
    void queueSessionCannotCreateTopics(final JmsSupport jms) throws Exception
    {
        try (final var queueConnection = jms.builder().connection().create(QueueConnection.class);
             final var queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            assertThrows(jakarta.jms.IllegalStateException.class,
                    () -> queueSession.createTopic("abc"),
                    "expected exception did not occur");
        }
    }

    @Test
    void queueSessionCannotCreateDurableSubscriber(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();
        try (final var queueConnection = jms.builder().connection().create(QueueConnection.class);
             final var queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            assertThrows(jakarta.jms.IllegalStateException.class,
                    () -> queueSession.createDurableSubscriber(topic, "abc"),
                    "expected exception did not occur");
        }
    }

    @Test
    void queueSessionCannotUnsubscribe(final JmsSupport jms) throws Exception
    {
        try (final var queueConnection = jms.builder().connection().create(QueueConnection.class);
             final var queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            assertThrows(jakarta.jms.IllegalStateException.class,
                    () -> queueSession.unsubscribe("abc"),
                    "expected exception did not occur");
        }
    }
}
