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

package org.apache.qpid.systests.jms_3_1.browser;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.apache.qpid.tests.utils.VirtualhostContextVariable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;

import java.time.Duration;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.qpid.systests.JmsAwait.jms;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System tests focusing on QueueBrowser semantics as defined by Jakarta Messaging.
 * <br>
 * The tests in this class are aligned with:
 * - 3.5    "Message headers" (JMSDeliveryTime and its effect on delivery);
 * - 3.8    "Message selectors" (including InvalidSelectorException for invalid syntax);
 * - 4.1.6  "Queue browsing" (non-destructive browsing of messages on a queue);
 * - 6.1.4  "Starting and stopping a connection";
 * - 6.1.8  "Closing a connection";
 * - 6.2.9  "Message consumption".
 * <br>
 * In particular, they verify that:
 * - QueueBrowser enumerates messages without removing them from the queue;
 * - selectors apply to browsers in the same way they apply to consumers;
 * - messages are not visible to a browser before their delivery time;
 * - invalid selectors cause InvalidSelectorException;
 * - usage of a browser after its session is closed results in IllegalStateException;
 * - providers behave reasonably when browsing on a stopped connection.
 */
@JmsSystemTest
@Tag("queue")
class BrowserTest
{
    /**
     * Integer property used to tag messages with an index to verify ordering
     * and selection behavior in the tests.
     */
    private static final String INDEX = "index";

    /**
     * Verifies that browsing an empty queue produces an empty enumeration of messages.
     * <br>
     * Section 4.1.6 "Queue browsing" describes QueueBrowser as a mechanism for
     * looking at messages on a queue without removing them. When the queue is
     * empty, the browser must simply provide an empty enumeration and must not
     * fail.
     * <br>
     * This test:
     * - creates an empty queue;
     * - creates a QueueBrowser on it;
     * - asserts that getEnumeration() returns an Enumeration with no elements.
     */
    @Test
    void emptyQueue(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 final var browser = session.createBrowser(queue))
            {
                final var enumeration = browser.getEnumeration();
                assertFalse(enumeration.hasMoreElements(),
                        "Empty queue must result in an empty browser enumeration");
            }
        }
    }

    /**
     * Verifies basic QueueBrowser behaviour for a queue with multiple messages:
     * - all messages present on the queue are visible via the browser;
     * - the number of messages returned by the enumeration matches the number
     *   of messages produced.
     * <br>
     * Section 4.1.6 "Queue browsing" defines that a QueueBrowser provides a way
     * to inspect the messages currently on a queue without removing them. This
     * test sends a sequence of messages and then checks that the browser can
     * see all of them.
     */
    @Test
    void browser(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final int lastIndex = 10;
            final List<Integer> indices = IntStream.rangeClosed(1, lastIndex)
                    .boxed()
                    .collect(Collectors.toList());
            populateQueue(queue, session, indices);

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> enumeration = browser.getEnumeration();

            Message browsedMessage = null;
            long browsed = 0;
            while (enumeration.hasMoreElements())
            {
                browsed++;
                browsedMessage = (Message) enumeration.nextElement();
            }

            assertEquals(indices.size(), browsed,
                    "Unexpected number of messages in browser enumeration");
            assertNotNull(browsedMessage, "Last browsed message must not be null");
            assertEquals(lastIndex, browsedMessage.getIntProperty(INDEX),
                    "Last message has unexpected index");

            browser.close();
        }
    }

    /**
     * Verifies that message selectors are honored by QueueBrowser in the same
     * way as for MessageConsumer.
     * <br>
     * Section 3.8 "Message selectors" defines a SQL92-based selector syntax that
     * can be applied to both consumers and browsers. Section 4.1.6 "Queue browsing"
     * states that a QueueBrowser may be created with a message selector.
     * <br>
     * This test:
     * - produces messages with indexes 1..10;
     * - creates a QueueBrowser with selector "index % 2 = 0" (even indices;
     *   note that the modulus operator is a Qpid extension of the selector
     *   syntax, not mandated by the Jakarta Messaging specification);
     * - verifies that only five messages are returned and that the last one has
     *   index 10.
     */
    @Test
    void browserWithSelector(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final int lastIndex = 10;
            final List<Integer> indices = IntStream.rangeClosed(1, lastIndex)
                    .boxed()
                    .collect(Collectors.toList());
            populateQueue(queue, session, indices);

            QueueBrowser browser = session.createBrowser(queue, "index % 2 = 0");
            Enumeration enumeration = browser.getEnumeration();

            Message browsedMessage = null;
            long browsed = 0;
            while (enumeration.hasMoreElements())
            {
                browsed++;
                browsedMessage = (Message) enumeration.nextElement();
            }

            assertEquals(5, browsed, "Unexpected number of messages in enumeration");
            assertNotNull(browsedMessage, "Last browsed message must not be null");
            assertEquals(lastIndex, browsedMessage.getIntProperty(INDEX),
                    "Last message has unexpected index");

            browser.close();
        }
    }

    /**
     * Verifies that browsing is non-destructive: QueueBrowser must not remove
     * messages from the queue.
     * <br>
     * Section 4.1.6 "Queue browsing" states that a QueueBrowser allows messages
     * to be seen without removing them. Messages should still be available for
     * normal consumption after they have been browsed.
     * <br>
     * This test:
     * - sends a single message to a queue;
     * - browses the queue once, verifying that the message is visible and matches
     *   the sent JMSMessageID;
     * - then consumes the message via a MessageConsumer and verifies the same
     *   JMSMessageID;
     * - finally browses again and verifies that no messages remain on the queue.
     */
    @Test
    void browserIsNonDestructive(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            final var message = session.createMessage();
            producer.send(message);
            producer.close();

            QueueBrowser browser = session.createBrowser(queue);
            Enumeration enumeration = browser.getEnumeration();
            assertTrue(enumeration.hasMoreElements(), "Browser must see the produced message");

            Message browsedMessage = (Message) enumeration.nextElement();
            assertNotNull(browsedMessage, "No message returned by browser");
            assertEquals(message.getJMSMessageID(), browsedMessage.getJMSMessageID(),
                    "Unexpected JMSMessageID on browsed message");

            browser.close();

            MessageConsumer consumer = session.createConsumer(queue);
            Message consumedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(consumedMessage, "No message returned by consumer");
            assertEquals(message.getJMSMessageID(), consumedMessage.getJMSMessageID(),
                    "Unexpected JMSMessageID on consumed message");

            QueueBrowser browser2 = session.createBrowser(queue);
            Enumeration enumeration2 = browser2.getEnumeration();
            assertFalse(enumeration2.hasMoreElements(),
                    "Queue must be empty after message is consumed");
            browser2.close();
        }
    }

    /**
     * Verifies provider behavior when browsing on a stopped connection.
     * <br>
     * Section 6.1.4 "Starting and stopping a connection" defines that starting
     * and stopping a connection controls the flow of messages to consumers, but
     * does not explicitly dictate the behavior of QueueBrowser.getEnumeration().
     * <br>
     * Different providers may:
     * - allow browsing on a stopped connection; or
     * - require the connection to be started and throw IllegalStateException
     *   otherwise.
     * <br>
     * This test:
     * - creates a QueueBrowser on a stopped connection;
     * - calls getEnumeration();
     * - treats both successful calls and IllegalStateException as acceptable
     *   outcomes, as both are consistent with the specification.
     */
    @Test
    void stoppedConnection(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try (QueueBrowser browser = session.createBrowser(queue))
            {
                browser.getEnumeration();
                // PASS: provider allows browsing on a stopped connection.
            } catch (IllegalStateException e) {
                // PASS: provider requires connection.start() before browsing.
            }
        }
    }

    /**
     * Verifies that creating a QueueBrowser with an invalid message selector
     * results in InvalidSelectorException.
     * <br>
     * Section 3.8 "Message selectors" defines a SQL92-based syntax for selectors
     * and states that InvalidSelectorException must be thrown if the selector
     * string has invalid syntax.
     * <br>
     * This test:
     * - creates a session;
     * - attempts to create a browser with a syntactically invalid selector;
     * - asserts that InvalidSelectorException is thrown.
     */
    @Test
    void browserWithInvalidSelectorThrowsInvalidSelectorException(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // "index = = 1" is syntactically invalid according to the selector grammar
            // and must cause InvalidSelectorException to be thrown.
            assertThrows(InvalidSelectorException.class,
                    () -> session.createBrowser(queue, "index = = 1"),
                    "Creating a QueueBrowser with an invalid selector must throw InvalidSelectorException");
        }
    }

    /**
     * Verifies that a message is not visible to a QueueBrowser before its delivery time.
     * <br>
     * Section 4.1.6 requires that a QueueBrowser must not return a message before
     * its delivery time has been reached.
     * <br>
     * This test uses MessageProducer#setDeliveryDelay to set a future delivery time,
     * then checks that browsing immediately after send yields an empty enumeration,
     * and finally that the message becomes visible once its delivery time has elapsed.
     */
    @Test
    @VirtualhostContextVariable(name = "virtualhost.housekeepingCheckPeriod", value = "100")
    void messagesNotVisibleInBrowserBeforeDeliveryTime(final JmsSupport jms) throws Exception
    {
        final int deliveryDelayMs = 2000;

        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = jms.builder().queue().holdOnPublishEnabled(true).create();

            final MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryDelay(deliveryDelayMs);
            final Message message = session.createMessage();
            message.setIntProperty(INDEX, 1);
            producer.send(message);
            producer.close();

            final long deliveryTime = message.getJMSDeliveryTime();
            assertTrue(deliveryTime > System.currentTimeMillis(),
                    "Message delivery time should be in the future");

            final String messageId = message.getJMSMessageID();
            assertNotNull(messageId, "Sent message must have JMSMessageID");

            // Immediately after send, the message must not be visible in a browser.
            try (QueueBrowser browser = session.createBrowser(queue))
            {
                final Enumeration enumeration = browser.getEnumeration();
                assertFalse(enumeration.hasMoreElements(),
                        "Message must not be visible to a browser before its delivery time");
            }

            final Duration atMost = Duration.ofMillis(Math.max(Timeouts.receiveMillis(), deliveryDelayMs * 4L));
            jms(atMost).untilAsserted(() ->
            {
                try (QueueBrowser browser = session.createBrowser(queue))
                {
                    final Enumeration enumeration = browser.getEnumeration();
                    assertTrue(enumeration.hasMoreElements(),
                            "Message should become visible to a browser once delivery time has elapsed");
                }
            });

            // Verify the message is visible and still consumable.
            try (QueueBrowser browser = session.createBrowser(queue))
            {
                final Enumeration enumeration = browser.getEnumeration();
                assertTrue(enumeration.hasMoreElements(), "Expected message to be visible via browser");
                final Message browsed = (Message) enumeration.nextElement();
                assertEquals(messageId, browsed.getJMSMessageID(), "Browsed message has unexpected JMSMessageID");
                assertEquals(1, browsed.getIntProperty(INDEX), "Browsed message has unexpected index");
            }

            final MessageConsumer consumer = session.createConsumer(queue);
            final long receiveTimeout = Math.max(Timeouts.receiveMillis(), deliveryDelayMs * 2L);
            final Message received = consumer.receive(receiveTimeout);
            assertNotNull(received, "Message should be consumable after delivery time");
            assertEquals(messageId, received.getJMSMessageID(), "Consumed message has unexpected JMSMessageID");

            try (QueueBrowser browser = session.createBrowser(queue))
            {
                assertFalse(browser.getEnumeration().hasMoreElements(),
                        "Queue must be empty after the message is consumed");
            }
        }
    }

    /**
     * Verifies that using a QueueBrowser after its Session has been closed results
     * in an IllegalStateException.
     */
    @Test
    @Disabled("AMQP JMS client hangs")
    void usingBrowserAfterSessionCloseThrowsIllegalStateException(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var browser = session.createBrowser(queue);

            session.close();

            assertThrows(IllegalStateException.class,
                    browser::getEnumeration,
                    "Using a browser after its session is closed must throw IllegalStateException");
        }
    }

    /**
     * Helper that populates the given queue with messages having the INDEX
     * property set to the integers from the provided list.
     *
     * @param queue   the queue to populate
     * @param session the session used to create messages and producer
     * @param indices the list of index values to send as messages
     * @throws JMSException if message creation or sending fails
     */
    private void populateQueue(final Queue queue,
                               final Session session,
                               final List<Integer> indices) throws JMSException
    {
        try (final var producer = session.createProducer(queue))
        {
            indices.stream()
                    .map(i -> createMessage(session, i))
                    .forEach(x -> sendMessage(producer, x));
        }
    }

    /**
     * Helper that sends a single message, wrapping any JMSException into a
     * RuntimeException for convenience in stream operations.
     *
     * @param producer the producer used to send the message
     * @param message  the message to send
     */
    private void sendMessage(MessageProducer producer, final Message message)
    {
        try
        {
            producer.send(message);
        }
        catch (JMSException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper that creates a simple message with an INDEX property set to the
     * provided value.
     *
     * @param session       the session to use when creating the message
     * @param messageNumber the integer value to place in the INDEX property
     * @return the created Message
     */
    private Message createMessage(final Session session, final int messageNumber)
    {
        try
        {
            final var message = session.createMessage();
            message.setIntProperty(INDEX, messageNumber);
            return message;
        }
        catch (JMSException e)
        {
            throw new RuntimeException(e);
        }
    }
}
