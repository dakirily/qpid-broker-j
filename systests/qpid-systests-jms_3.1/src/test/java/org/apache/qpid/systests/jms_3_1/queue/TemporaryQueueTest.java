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

package org.apache.qpid.systests.jms_3_1.queue;

import static org.apache.qpid.systests.JmsAwait.jms;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

import java.time.Duration;

@JmsSystemTest
@Tag("queue")
class TemporaryQueueTest
{
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    void messageDeliveryUsingTemporaryQueue(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session.createTemporaryQueue();
            assertNotNull(queue, "Temporary queue cannot be null");
            final var producer = session.createProducer(queue);
            final var consumer = session.createConsumer(queue);
            connection.start();
            producer.send(session.createTextMessage("hello"));
            final var message = consumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "TextMessage should be received");
            assertEquals("hello", textMessage.getText());
        }
    }

    @Test
    void consumeFromAnotherConnectionProhibited(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create(); final var connection2 = jms.builder().connection().create();
             final var session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session1.createTemporaryQueue();

            assertNotNull(queue, "Temporary queue cannot be null");

            assertThrows(JMSException.class,
                    () -> session2.createConsumer(queue),
                    "Expected a JMSException when subscribing to a temporary queue created on a different session");
        }
    }

    @Test
    void consumeFromAnotherConnectionUsingTemporaryQueueName(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create(); final var connection2 = jms.builder().connection().create();
             final var session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session1.createTemporaryQueue();

            assertNotNull(queue, "Temporary queue cannot be null");

            assertThrows(JMSException.class,
                    () -> session2.createConsumer(session2.createQueue(queue.getQueueName())),
                    "Expected a JMSException when subscribing to a temporary queue created on a different session");
        }
    }

    @Test
    void publishFromAnotherConnectionAllowed(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create(); final var connection2 = jms.builder().connection().create();
             final var session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session1.createTemporaryQueue();
            assertNotNull(queue, "Temporary queue cannot be null");

            final var producer = session2.createProducer(queue);
            producer.send(session2.createMessage());

            connection.start();
            final var consumer = session1.createConsumer(queue);
            final var message = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Message not received");
        }
    }

    @Test
    void closingConsumerDoesNotDeleteQueue(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session.createTemporaryQueue();
            assertNotNull(queue, "Temporary queue cannot be null");

            final var producer = session.createProducer(queue);
            final var messageText = "Hello World!";
            producer.send(session.createTextMessage(messageText));

            connection.start();
            session.createConsumer(queue).close();

            final var consumer = session.createConsumer(queue);
            final var message = consumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Received message not a text message");
            assertEquals(messageText, textMessage.getText(), "Incorrect message text");
        }
    }

    @Test
    void closingSessionDoesNotDeleteQueue(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session1.createTemporaryQueue();
            assertNotNull(queue, "Temporary queue cannot be null");

            final var producer = session1.createProducer(queue);
            final var messageText = "Hello World!";
            producer.send(session1.createTextMessage(messageText));

            session1.close();

            connection.start();
            final var consumer = session2.createConsumer(queue);
            final var message = consumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Received message not a text message");
            assertEquals(messageText, textMessage.getText(), "Incorrect message text");
        }
    }

    @Test
    void explicitTemporaryQueueDeletion(final JmsSupport jms) throws Exception
    {
        final int numberOfQueuesBeforeTest = jms.virtualhost().queueCount();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session.createTemporaryQueue();
            assertNotNull(queue, "Temporary queue cannot be null");
            final var consumer = session.createConsumer(queue);
            connection.start();

            assertThrows(JMSException.class, queue::delete,
                    "Expected JMSException : should not be able to delete while there are active consumers");

            int numberOfQueuesAfterQueueDelete = jms.virtualhost().queueCount();
            assertEquals(1, numberOfQueuesAfterQueueDelete - numberOfQueuesBeforeTest,
                    "Unexpected number of queue");

            consumer.close();

            // Now deletion should succeed.
            queue.delete();

            assertThrows(JMSException.class, () -> session.createConsumer(queue), "Exception not thrown");
        }
    }

    @Test
    void delete(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().syncPublish(true).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session.createTemporaryQueue();
            final var producer = session.createProducer(queue);

            assertDoesNotThrow(() -> producer.send(session.createTextMessage("hello")),
                    "Send to temporary queue should succeed");
            assertDoesNotThrow(queue::delete, "temporary queue should be deletable");

            assertThrows(JMSException.class,
                    () -> producer.send(session.createTextMessage("hello")),
                    "Send to deleted temporary queue should not succeed");
        }
    }

    /**
     * Verifies that a temporary queue created on a connection is automatically deleted when the
     * connection is closed.
     *
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#creating-temporary-destinations">6.2.2 "Creating temporary destinations"</a>
     * states that temporary destinations have the lifetime of their connection and that they will be
     * automatically deleted when their connection is closed.
     *
     * This test uses the broker management statistics (queueCount) as an observable proxy for the
     * provider resources allocated on behalf of the temporary destination.
     */
    @Test
    void temporaryQueueIsAutoDeletedWhenConnectionIsClosed(final JmsSupport jms) throws Exception
    {
        final int numberOfQueuesBeforeTest = jms.virtualhost().queueCount();
        final String temporaryQueueName;

        // Create a TemporaryQueue and verify that it results in a broker-side queue being created.
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();

            final var temporaryQueue = session.createTemporaryQueue();
            temporaryQueueName = temporaryQueue.getQueueName();

            final int numberOfQueuesAfterCreate = jms.virtualhost().queueCount();
            assertEquals(numberOfQueuesBeforeTest + 1, numberOfQueuesAfterCreate,
                    "Temporary queue should be created on the broker");
        }

        // Closing the connection should release provider resources, including deletion of the temporary queue.
        jms(TIMEOUT).pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertEquals(numberOfQueuesBeforeTest, jms.virtualhost().queueCount(),
                        () -> "Temporary queue should be deleted when connection is closed. Queue name: "
                                + temporaryQueueName));
    }
}
