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

package org.apache.qpid.systests.jms_3_1.connection;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on Connection.stop() semantics for synchronous receive.
 * <br>
 * The tests in this class are aligned with:
 * - 6.1.5 "Pausing delivery of incoming messages"
 */
@JmsSystemTest
@Tag("connection")
@Tag("queue")
class ConnectionStopTest
{

    /**
     * Verifies that Connection.stop():
     * - suppresses synchronous receives until start() is called;
     * - is idempotent, as is start(), without affecting the ability to send messages.
     */
    @Test
    void stopSuppressesSynchronousReceiveUntilStart(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            final MessageConsumer consumer = session.createConsumer(queue);

            producer.send(session.createTextMessage("A"));

            connection.start();
            connection.stop();

            assertNull(consumer.receive(Timeouts.noMessagesMillis()),
                    "No message should be delivered while the connection is stopped");

            producer.send(session.createTextMessage("B"));

            connection.stop();
            connection.start();
            connection.start();

            assertNotNull(consumer.receive(Timeouts.receiveMillis()),
                    "Message should be delivered after connection start");
            assertNotNull(consumer.receive(Timeouts.receiveMillis()),
                    "Message sent while stopped should be delivered after connection start");
        }
    }
}
