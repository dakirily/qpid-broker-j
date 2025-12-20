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

package org.apache.qpid.systests.jms_3_1.topic;

import static org.apache.qpid.systests.JmsAwait.jms;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

import java.time.Duration;
import java.util.Map;

@JmsSystemTest
@Tag("topic")
class TemporaryTopicTest
{
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void messageDeliveryUsingTemporaryTopic(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var topic = session.createTemporaryTopic();
            assertNotNull(topic, "Temporary topic is null");
            final var producer = session.createProducer(topic);
            final var consumer1 = session.createConsumer(topic);
            final var consumer2 = session.createConsumer(topic);
            connection.start();
            producer.send(session.createTextMessage("hello"));

            final var tm1 = (TextMessage) consumer1.receive(Timeouts.receiveMillis());
            final var tm2 = (TextMessage) consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(tm1, "Message not received by subscriber1");
            assertEquals("hello", tm1.getText());
            assertNotNull(tm2, "Message not received by subscriber2");
            assertEquals("hello", tm2.getText());
        }
    }

    @Test
    void explicitTemporaryTopicDeletion(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var topic = session.createTemporaryTopic();
            assertNotNull(topic, "Temporary topic is null");

            try (final var ignored = session.createConsumer(topic))
            {
                connection.start();
                assertThrows(JMSException.class, topic::delete,
                        "Expected JMSException : should not be able to delete while there are active consumers");
            }

            // Now deletion should succeed.
            topic.delete();

            assertThrows(JMSException.class, () -> session.createConsumer(topic), "Exception not thrown");
        }
    }

    @Test
    void useFromAnotherConnectionProhibited(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var connection2 = jms.builder().connection().create();
             final var session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var topic = session1.createTemporaryTopic();

            assertThrows(JMSException.class, () -> session2.createConsumer(topic),
                    "Expected a JMSException when subscribing to a temporary topic created on a different connection");
        }
    }

    @Test
    void temporaryTopicReused(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var topic = session.createTemporaryTopic();
            assertNotNull(topic, "Temporary topic is null");

            final var producer = session.createProducer(topic);
            try (final var consumer1 = session.createConsumer(topic))
            {
                connection.start();
                producer.send(session.createTextMessage("message1"));
                final var tm = (TextMessage) consumer1.receive(Timeouts.receiveMillis());
                assertNotNull(tm, "Message not received by first consumer");
                assertEquals("message1", tm.getText());
            }

            try (final var consumer2 = session.createConsumer(topic))
            {
                connection.start();
                producer.send(session.createTextMessage("message2"));
                final var tm = (TextMessage) consumer2.receive(Timeouts.receiveMillis());
                assertNotNull(tm, "Message not received by second consumer");
                assertEquals("message2", tm.getText());
            }
        }
    }

    /**
     * Verifies that a temporary topic created on a connection is automatically deleted when the
     * connection is closed.
     *
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#creating-temporary-destinations">6.2.2 "Creating temporary destinations"</a>
     * states that temporary destinations have the lifetime of their connection and that they will be
     * automatically deleted when their connection is closed.
     *
     * This test uses the broker management statistics (queueCount/exchangeCount) as an observable
     * proxy for the provider resources allocated on behalf of the temporary destination.
     */
    @Test
    void temporaryTopicIsAutoDeletedWhenConnectionIsClosed(final JmsSupport jms) throws Exception
    {
        final int numberOfQueuesBeforeTest = jms.virtualhost().queueCount();
        final int numberOfExchangesBeforeTest = jms.virtualhost().exchangeCount();
        final String temporaryTopicName;

        // Create and use a TemporaryTopic to ensure broker-side resources are allocated.
        // We do not rely on a specific delta (+1, +2, ...) because a provider may allocate different
        // internal resources to implement a temporary topic.
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();

            final var temporaryTopic = session.createTemporaryTopic();
            temporaryTopicName = temporaryTopic.getTopicName();

            try (final var consumer = session.createConsumer(temporaryTopic);
                 final var producer = session.createProducer(temporaryTopic))
            {
                final TextMessage message = session.createTextMessage("probe");
                producer.send(message);

                final TextMessage received = (TextMessage) consumer.receive(Timeouts.receiveMillis());
                assertNotNull(received, "Message was not received from temporary topic '" + temporaryTopicName + "'");
                assertEquals("probe", received.getText(), "Unexpected message body received from temporary topic");
            }

            final int numberOfQueuesAfterCreate = jms.virtualhost().queueCount();
            final int numberOfExchangesAfterCreate = jms.virtualhost().exchangeCount();
            assertTrue(numberOfQueuesAfterCreate > numberOfQueuesBeforeTest
                            || numberOfExchangesAfterCreate > numberOfExchangesBeforeTest,
                    "Creating/using a TemporaryTopic did not result in any observable broker-side resource allocation");
        }

        // The spec requires that temporary destinations are automatically deleted when their
        // connection is closed.
        jms(TIMEOUT).pollInterval(Duration.ofMillis(100)).untilAsserted(() ->
        {
            assertEquals(numberOfQueuesBeforeTest,
                    jms.virtualhost().queueCount(),
                    "Temporary topic '" + temporaryTopicName + "' was not fully cleaned up on connection close (queueCount)");
            assertEquals(numberOfExchangesBeforeTest,
                    jms.virtualhost().exchangeCount(),
                    "Temporary topic '" + temporaryTopicName + "' was not fully cleaned up on connection close (exchangeCount)");
        });
    }
}
