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

package org.apache.qpid.systests.jms_3_1.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Message;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on JMSTimestamp semantics for the classic API.
 *
 * The tests in this class are aligned with:
 * - 3.4.4 "JMSTimestamp"
 * - 7.6   "Setting message headers"
 */
@JmsSystemTest
@Tag("queue")
@Tag("message")
class JMSTimestampTest
{
    /**
     * Verifies that JMSTimestamp is set by the provider on send
     * and preserved on the received message.
     */
    @Test
    void timestampIsSetOnSendAndPreservedOnReceive(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            final var message = session.createMessage();
            producer.send(message);

            final long timestamp = message.getJMSTimestamp();
            assertTrue(timestamp > 0, "JMSTimestamp should be set by the provider");

            final var consumer = session.createConsumer(queue);
            connection.start();
            final Message received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");
            assertEquals(timestamp, received.getJMSTimestamp(),
                    "Received JMSTimestamp should match the value set on send");
        }
    }

    /**
     * Verifies that disabling timestamps results in JMSTimestamp being zero
     * on both the sent and received messages.
     */
    @Test
    void disableMessageTimestampSuppressesTimestamp(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            producer.setDisableMessageTimestamp(true);

            final var message = session.createMessage();
            producer.send(message);

            assertEquals(0L, message.getJMSTimestamp(), "JMSTimestamp should be zero when disabled");

            final var consumer = session.createConsumer(queue);
            connection.start();
            final Message received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");
            assertEquals(0L, received.getJMSTimestamp(),
                    "Received JMSTimestamp should be zero when disabled");
        }
    }
}
