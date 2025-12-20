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

package org.apache.qpid.systests.jms_3_1.persistence;

import static jakarta.jms.DeliveryMode.NON_PERSISTENT;
import static jakarta.jms.DeliveryMode.PERSISTENT;
import static jakarta.jms.Session.CLIENT_ACKNOWLEDGE;
import static jakarta.jms.Session.SESSION_TRANSACTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.qpid.systests.support.AsyncSupport;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@JmsSystemTest
@Tag("transactions")
class PersistentMessagingTest
{
    private static final int MSG_COUNT = 3;
    private static final String INT_PROPERTY = "index";
    private static final String STRING_PROPERTY = "string";

    @BeforeEach
    void setUp(final JmsSupport jms)
    {
        assumeTrue(jms.brokerAdmin().supportsRestart(), "Tests requires persistent store");
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void committedPersistentMessagesSurviveBrokerRestart(final JmsSupport jms,
                                                         final AsyncSupport async) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final List<Message> sentMessages = new ArrayList<>();

        try (final var sendingConnection = jms.builder().connection().create();
             final var session = sendingConnection.createSession(true, SESSION_TRANSACTED);
             final var producer = session.createProducer(queue))
        {
            sentMessages.addAll(sendMessages(session, producer, PERSISTENT, 0, MSG_COUNT));
            sendMessages(session, producer, NON_PERSISTENT, MSG_COUNT, 1);
        }

        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        verifyQueueContents(jms, queue, sentMessages);
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void uncommittedPersistentMessagesDoNotSurviveBrokerRestart(final JmsSupport jms,
                                                                final AsyncSupport async) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var sendingConnection = jms.builder().connection().create();
             final var session = sendingConnection.createSession(true, SESSION_TRANSACTED);
             final var producer = session.createProducer(queue))
        {
            producer.send(session.createMessage());
            // do not commit
        }

        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        try (final var receivingConnection = jms.builder().connection().create())
        {
            receivingConnection.start();
            final var session = receivingConnection.createSession(true, SESSION_TRANSACTED);
            final var consumer = session.createConsumer(queue);
            final var unexpectedMessage = consumer.receiveNoWait();
            assertNull(unexpectedMessage, "Unexpected message [%s] received".formatted(unexpectedMessage));
        }
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void transactedAcknowledgementPersistence(final JmsSupport jms,
                                              final AsyncSupport async) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final List<Message> remainingMessages = new ArrayList<>();

        try (final var initialConnection = jms.builder().connection().create())
        {
            initialConnection.start();
            final var session = initialConnection.createSession(true, SESSION_TRANSACTED);
            final var producer = session.createProducer(queue);

            final var initialMessage = sendMessages(session, producer, PERSISTENT, 0, 1);
            remainingMessages.addAll(sendMessages(session, producer, PERSISTENT, 1, 1));

            // Receive first message and commit
            final var consumer = session.createConsumer(queue);
            receiveAndVerifyMessages(jms, session, consumer, initialMessage);
            // Receive second message but do not commit
            final var peek = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(peek);
        }

        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        verifyQueueContents(jms, queue, remainingMessages);
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void clientAckAcknowledgementPersistence(final JmsSupport jms,
                                             final AsyncSupport async) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final List<Message> remainingMessages = new ArrayList<>();

        try (final var initialConnection = jms.builder().connection().create())
        {
            initialConnection.start();
            final var publishingSession = initialConnection.createSession(true, SESSION_TRANSACTED);
            final var producer = publishingSession.createProducer(queue);

            final var initialMessages = sendMessages(publishingSession, producer, PERSISTENT, 0, 1);
            remainingMessages.addAll(sendMessages(publishingSession, producer, PERSISTENT, 1, 1));

            Session consumingSession = initialConnection.createSession(false, CLIENT_ACKNOWLEDGE);

            // Receive first message and ack
            final var consumer = consumingSession.createConsumer(queue);
            receiveAndVerifyMessages(jms, consumingSession, consumer, initialMessages);

            // Receive second but do not ack
            final var peek = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(peek);
        }

        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        verifyQueueContents(jms, queue, remainingMessages);
    }

    private List<Message> sendMessages(final Session session,
                                       final MessageProducer producer,
                                       final int deliveryMode,
                                       final int startIndex,
                                       final int count) throws Exception
    {
        final List<Message> sentMessages = new ArrayList<>();
        for (int i = startIndex; i < startIndex + count; i++)
        {
            final var message = session.createTextMessage(UUID.randomUUID().toString());
            message.setIntProperty(INT_PROPERTY, i);
            message.setStringProperty(STRING_PROPERTY, UUID.randomUUID().toString());

            producer.send(message, deliveryMode, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            sentMessages.add(message);
        }

        session.commit();
        return sentMessages;
    }

    private void verifyQueueContents(final JmsSupport jms, final Queue queue, final List<Message> expectedMessages) throws Exception
    {
        try (final var receivingConnection = jms.builder().connection().create())
        {
            receivingConnection.start();
            final var session = receivingConnection.createSession(true, SESSION_TRANSACTED);
            final var consumer = session.createConsumer(queue);

            receiveAndVerifyMessages(jms, session, consumer, expectedMessages);

            final var unexpectedMessage = consumer.receiveNoWait();
            assertNull(unexpectedMessage, "Unexpected additional message [%s] received".formatted(unexpectedMessage));
        }
    }

    private void receiveAndVerifyMessages(final JmsSupport jms,
                                          final Session session,
                                          final MessageConsumer consumer,
                                          final List<Message> expectedMessages) throws Exception
    {

        for (final var expected : expectedMessages)
        {
            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message not received when expecting message %d".formatted(expected.getIntProperty(INT_PROPERTY)));

            final var textMessageExpected = assertInstanceOf(TextMessage.class, expected, "Unexpected type");
            assertEquals(expected.getIntProperty(INT_PROPERTY), received.getIntProperty(INT_PROPERTY), "Unexpected index");
            assertEquals(expected.getStringProperty(STRING_PROPERTY), received.getStringProperty(STRING_PROPERTY),
                    "Unexpected string property");
            assertEquals(textMessageExpected.getText(), ((TextMessage) received).getText(),
                    "Unexpected message content");

            final int acknowledgeMode = session.getAcknowledgeMode();
            if (acknowledgeMode == SESSION_TRANSACTED)
            {
                session.commit();
            }
            else if (acknowledgeMode == CLIENT_ACKNOWLEDGE)
            {
                received.acknowledge();
            }
        }
    }
}
