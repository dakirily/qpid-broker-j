/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.qpid.systests.jms_3_1.selector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * System tests for message selector header field references.
 * <br>
 * Jakarta Messaging defines that message selectors may reference a restricted set of message header fields.
 * It also defines the NULL semantics for those header fields that may be absent.
 * <br>
 * The tests in this class are aligned with:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">
 *   3.8.1.1 "Message selector syntax"</a>
 */
@JmsSystemTest
@Tag("filters")
@Tag("queue")
class SelectorHeaderFieldsTest
{
    private static final String ID = "id";

    /**
     * Verifies selector evaluation using the {@code JMSPriority} header field reference.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states: "Message header field references are restricted to ... JMSPriority ...".
     */
    @Test
    void selectorWithJMSPriority(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);

            final var low = session.createMessage();
            low.setIntProperty(ID, 1);
            producer.send(low, DeliveryMode.NON_PERSISTENT, 4, Message.DEFAULT_TIME_TO_LIVE);

            final var high = session.createMessage();
            high.setIntProperty(ID, 2);
            producer.send(high, DeliveryMode.NON_PERSISTENT, 7, Message.DEFAULT_TIME_TO_LIVE);

            connection.start();

            try (final QueueBrowser browser = session.createBrowser(queue, "JMSPriority >= 5"))
            {
                assertEquals(Set.of(2), browseIntProperty(browser, ID), "Unexpected JMSPriority selector matches");
            }

            drainQueue(session, queue);
        }
    }

    /**
     * Verifies selector evaluation using the {@code JMSTimestamp} header field reference.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states: "Message header field references are restricted to ... JMSTimestamp ...".
     */
    @Test
    void selectorWithJMSTimestamp(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);

            final var message = session.createMessage();
            message.setIntProperty(ID, 1);
            producer.send(message);

            final long timestamp = message.getJMSTimestamp();

            connection.start();

            // Use the provider-set timestamp from the message after send().
            try (final QueueBrowser browser = session.createBrowser(queue, "JMSTimestamp = %d".formatted(timestamp)))
            {
                assertEquals(Set.of(1), browseIntProperty(browser, ID), "Unexpected JMSTimestamp selector matches");
            }

            drainQueue(session, queue);
        }
    }

    /**
     * Verifies selector evaluation using {@code JMSCorrelationID} header field reference, including NULL semantics.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states:
     * - "Message header field references are restricted to ... JMSCorrelationID ...".
     * - "JMSMessageID, JMSCorrelationID, and JMSType values may be null and if so are treated as a NULL value."
     */
    @Test
    void selectorWithJMSCorrelationID(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);

            final var hasCorrelationId = session.createMessage();
            hasCorrelationId.setIntProperty(ID, 1);
            hasCorrelationId.setJMSCorrelationID("c1");
            producer.send(hasCorrelationId);

            final var noCorrelationId = session.createMessage();
            noCorrelationId.setIntProperty(ID, 2);
            // JMSCorrelationID left unset (null).
            producer.send(noCorrelationId);

            connection.start();

            try (final QueueBrowser equals = session.createBrowser(queue, "JMSCorrelationID = 'c1'");
                 final QueueBrowser isNull = session.createBrowser(queue, "JMSCorrelationID IS NULL"))
            {
                assertEquals(Set.of(1), browseIntProperty(equals, ID), "Unexpected JMSCorrelationID='c1' selector matches");
                assertEquals(Set.of(2), browseIntProperty(isNull, ID), "Unexpected JMSCorrelationID IS NULL selector matches");
            }

            drainQueue(session, queue);
        }
    }

    /**
     * Verifies selector evaluation using {@code JMSType} header field reference, including NULL semantics.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states:
     * - "Message header field references are restricted to ... JMSType ...".
     * - "JMSMessageID, JMSCorrelationID, and JMSType values may be null and if so are treated as a NULL value."
     */
    @Test
    void selectorWithJMSType(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);

            final var hasType = session.createMessage();
            hasType.setIntProperty(ID, 1);
            hasType.setJMSType("t1");
            producer.send(hasType);

            final var noType = session.createMessage();
            noType.setIntProperty(ID, 2);
            // JMSType left unset (null).
            producer.send(noType);

            connection.start();

            try (final QueueBrowser equals = session.createBrowser(queue, "JMSType = 't1'");
                 final QueueBrowser isNull = session.createBrowser(queue, "JMSType IS NULL"))
            {
                assertEquals(Set.of(1), browseIntProperty(equals, ID), "Unexpected JMSType='t1' selector matches");
                assertEquals(Set.of(2), browseIntProperty(isNull, ID), "Unexpected JMSType IS NULL selector matches");
            }

            drainQueue(session, queue);
        }
    }

    private static Set<Integer> browseIntProperty(final QueueBrowser browser, final String property) throws Exception
    {
        final Set<Integer> values = new HashSet<>();
        final Enumeration<?> enumeration = browser.getEnumeration();
        while (enumeration.hasMoreElements())
        {
            final Message message = (Message) enumeration.nextElement();
            values.add(message.getIntProperty(property));
        }
        return values;
    }

    private static void drainQueue(final Session session, final Queue queue) throws Exception
    {
        // Keep test queues clean to avoid accumulating unconsumed messages across the suite.
        try (final MessageConsumer consumer = session.createConsumer(queue))
        {
            Message m;
            while ((m = consumer.receive(Timeouts.noMessagesMillis())) != null)
            {
                // drain
            }
        }
    }
}