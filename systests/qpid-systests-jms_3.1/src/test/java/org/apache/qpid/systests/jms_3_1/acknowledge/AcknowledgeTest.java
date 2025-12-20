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

package org.apache.qpid.systests.jms_3_1.acknowledge;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;

import javax.naming.NamingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System tests focusing on message acknowledgement semantics for the classic
 * acknowledgement modes defined by Jakarta Messaging (AUTO_ACKNOWLEDGE,
 * DUPS_OK_ACKNOWLEDGE, CLIENT_ACKNOWLEDGE, SESSION_TRANSACTED).
 *
 * The tests in this class are aligned with:
 * - 3.6    "Message acknowledgement"
 * - 6.2.7  "Transactions"
 * - 6.2.9  "Message consumption"
 * - 6.2.10 "Message acknowledgement"
 * - 6.2.11 "Duplicate delivery of messages"
 * - 8.7    "Receiving messages asynchronously"
 */
@JmsSystemTest
@Tag("queue")
class AcknowledgeTest
{
    private static final int TIMEOUT = 30000;

    /**
     * Verifies basic AUTO_ACKNOWLEDGE behaviour for synchronous receive:
     * - messages are delivered in order sent to a single consumer (6.2.9.1 "Order of message receipt");
     * - they are automatically acknowledged when receive() returns successfully
     *   (6.2.10 "Message acknowledgment" AUTO_ACKNOWLEDGE case).
     *
     * The follow-up check in verifyLeftOvers() ensures that, in the absence of failures,
     * no messages remain or are redelivered on a new connection (6.2.11 "Duplicate delivery of messages").
     */
    @Test
    void autoAcknowledgement(final JmsSupport jms) throws Exception
    {
        doAcknowledgementTest(jms, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * Verifies that a consumer using DUPS_OK_ACKNOWLEDGE can consume messages in order
     * and see each message at least once.
     *
     * Section 6.2.10 states that DUPS_OK_ACKNOWLEDGE allows the session to lazily acknowledge
     * delivery of messages and that this may result in duplicates if the provider fails,
     * but it does not require duplicates in normal operation.
     *
     * This test therefore only checks basic delivery; verifyLeftOvers() intentionally does
     * not make a strong assertion about redelivery for this mode, in line with the spec.
     */
    @Test
    void dupsOkAcknowledgement(final JmsSupport jms) throws Exception
    {
        doAcknowledgementTest(jms, Session.DUPS_OK_ACKNOWLEDGE);
    }

    /**
     * Verifies basic CLIENT_ACKNOWLEDGE behaviour for synchronous receive:
     * - the application calls Message.acknowledge() explicitly (3.6 "Message acknowledgment");
     * - only messages delivered up to the point of the acknowledge() call are considered acknowledged;
     * - a later message that was delivered but not covered by an acknowledge() call must be
     *   redelivered on a new session (6.2.10 "Message acknowledgment").
     *
     * Concretely, this test acknowledges message 0 and checks that message 1 is re-delivered
     * on a new connection, showing it was not implicitly acknowledged.
     */
    @Test
    void clientAcknowledgement(final JmsSupport jms) throws Exception
    {
        doAcknowledgementTest(jms, Session.CLIENT_ACKNOWLEDGE);
    }

    /**
     * Verifies acknowledgement semantics for SESSION_TRANSACTED using a MessageListener:
     * - a transacted session groups consumed messages into a transaction whose input is
     *   acknowledged only on Session.commit() (6.2.7 "Transactions" and 6.2.10);
     * - messages consumed in a transaction that is not committed remain unacknowledged
     *   and will be recovered on rollback or when a new session consumes them.
     *
     * doMessageListenerAcknowledgementTest() commits after receiving the first message
     * and then ensures that the remaining messages are delivered once and that
     * verifyLeftOvers() sees no unacknowledged leftovers.
     */
    @Test
    void sessionTransactedAcknowledgement(final JmsSupport jms) throws Exception
    {
        doAcknowledgementTest(jms, Session.SESSION_TRANSACTED);
    }

    /**
     * Verifies AUTO_ACKNOWLEDGE semantics for asynchronous receive:
     * - messages are delivered asynchronously via MessageListener as described in
     *   section 8.7 "Receiving messages asynchronously";
     * - messages are automatically acknowledged when onMessage(...) returns successfully
     *   (6.2.10 AUTO_ACKNOWLEDGE case).
     *
     * verifyLeftOvers() asserts that no messages remain on the queue for this mode.
     */
    @Test
    void autoAcknowledgementMessageListener(final JmsSupport jms) throws Exception
    {
        doMessageListenerAcknowledgementTest(jms, Session.AUTO_ACKNOWLEDGE);
    }

    /**
     * Verifies CLIENT_ACKNOWLEDGE semantics with asynchronous MessageListener processing:
     * - a listener calls Message.acknowledge() for the first delivered message;
     * - acknowledgement is cumulative for all messages delivered so far in the session
     *   (6.2.10 "Message acknowledgment", CLIENT_ACKNOWLEDGE case);
     * - messages delivered after the acknowledge() call remain unacknowledged and must be
     *   redelivered on a new connection.
     *
     * verifyLeftOvers() confirms that only the message that was not covered by
     * the original acknowledge() call is redelivered.
     */
    @Test
    void clientAcknowledgementMessageListener(final JmsSupport jms) throws Exception
    {
        doMessageListenerAcknowledgementTest(jms, Session.CLIENT_ACKNOWLEDGE);
    }

    /**
     * Verifies SESSION_TRANSACTED semantics with asynchronous MessageListener processing:
     * - a listener commits the session after processing the first message,
     *   which acknowledges all messages consumed in the current transaction (6.2.7, 6.2.10);
     * - messages consumed after that commit are part of a new transaction and must be
     *   acknowledged by a later commit.
     *
     * verifyLeftOvers() ensures that messages consumed but not committed are redelivered
     * when a new consumer is created.
     */
    @Test
    void sessionTransactedAcknowledgementMessageListener(final JmsSupport jms) throws Exception
    {
        doMessageListenerAcknowledgementTest(jms, Session.SESSION_TRANSACTED);
    }

    /**
     * Verifies that CLIENT_ACKNOWLEDGE is cumulative across all messages delivered
     * in the session up to the point of the acknowledge() call.
     *
     * Section 6.2.10 "Message acknowledgment" (CLIENT_ACKNOWLEDGE) states:
     * "Acknowledging a consumed message automatically acknowledges the receipt of all messages
     * that have been delivered by its session."
     *
     * This test:
     * - sends three messages with indexes 0,1,2;
     * - consumes all three and calls acknowledge() only on the last (index 2);
     * - verifies that no messages are available on a new connection, i.e. all three
     *   delivered messages were acknowledged cumulatively.
     */
    @Test
    void clientAcknowledgementCumulativeAcksAllDeliveredMessages(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);

            // Produce three ordered messages.
            for (int i = 0; i < 3; i++)
            {
                final var message = session.createMessage();
                message.setIntProperty(INDEX, i);
                producer.send(message);
            }

            connection.start();

            // Receive messages 0, 1 and 2 in order.
            Message first = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(first, "First message was not received");
            assertEquals(0, first.getIntProperty(INDEX), "Unexpected index for first message");

            Message second = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(second, "Second message was not received");
            assertEquals(1, second.getIntProperty(INDEX), "Unexpected index for second message");

            Message third = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(third, "Third message was not received");
            assertEquals(2, third.getIntProperty(INDEX), "Unexpected index for third message");

            // Acknowledge only the last consumed message. Per 6.2.10 this must
            // cumulatively acknowledge all messages delivered in this session.
            third.acknowledge();

            // No more messages should be available in this session.
            Message none = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(none, "No additional messages expected after cumulative acknowledge");
        }

        // On a new connection, all three messages must be gone if cumulative
        // CLIENT_ACKNOWLEDGE semantics are correctly implemented.
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            final var message = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(message, "No messages should be redelivered after cumulative CLIENT_ACKNOWLEDGE");
        }
    }

    /**
     * Verifies that explicit calls to Message.acknowledge() are ignored when the session
     * uses AUTO_ACKNOWLEDGE.
     *
     * Section 3.6 "Message acknowledgment" specifies that if automatic acknowledgment is used,
     * calls to acknowledge() have no effect. In particular, calling acknowledge() must not:
     * - cause a failure;
     * - prevent subsequent messages from being delivered and automatically acknowledged.
     *
     * This test:
     * - uses AUTO_ACKNOWLEDGE;
     * - calls acknowledge() explicitly on the first message;
     * - verifies that the second message is still delivered and that no messages remain
     *   on a new connection.
     */
    @Test
    void autoAcknowledgementExplicitAcknowledgeIsIgnored(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);

            // Produce two ordered messages.
            for (int i = 0; i < 2; i++)
            {
                final var message = session.createMessage();
                message.setIntProperty(INDEX, i);
                producer.send(message);
            }

            connection.start();

            // Receive first message and explicitly call acknowledge().
            Message first = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(first, "First message has not been received");
            assertEquals(0, first.getIntProperty(INDEX), "Unexpected first message index");

            // In AUTO_ACKNOWLEDGE mode this must be ignored by the provider (3.6).
            first.acknowledge();

            // Second message must still be delivered normally and automatically acknowledged.
            var second = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(second, "Second message has not been received");
            assertEquals(1, second.getIntProperty(INDEX), "Unexpected second message index");

            // No more messages should be available in this session.
            var none = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(none, "No additional messages expected after AUTO_ACKNOWLEDGE receive");
        }

        // On a new connection, there must be no messages remaining on the queue
        // if AUTO_ACKNOWLEDGE and ignore-ack semantics are correctly implemented.
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            final var message = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(message, "Unexpected message received on new connection after AUTO_ACKNOWLEDGE");
        }
    }

    /**
     * Verifies that explicit calls to Message.acknowledge() are ignored when the session
     * uses DUPS_OK_ACKNOWLEDGE.
     *
     * Section 3.6 "Message acknowledgment" together with 6.2.10 "Message acknowledgment"
     * state that when the session uses an automatic acknowledgement mode such as
     * DUPS_OK_ACKNOWLEDGE, it is the session's responsibility to acknowledge messages,
     * and calls to Message.acknowledge() must not change the acknowledgement semantics.
     *
     * Concretely, this test:
     * - uses DUPS_OK_ACKNOWLEDGE;
     * - calls acknowledge() explicitly on the first message;
     * - verifies that the second message is still delivered and that no messages remain
     *   on a new connection, demonstrating that the explicit acknowledge() was ignored.
     */
    @Test
    void dupsOkAcknowledgementExplicitAcknowledgeIsIgnored(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);

            // Produce two ordered messages.
            for (int i = 0; i < 2; i++)
            {
                final var message = session.createMessage();
                message.setIntProperty(INDEX, i);
                producer.send(message);
            }

            connection.start();

            // Receive first message and explicitly call acknowledge().
            Message first = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(first, "First message has not been received");
            assertEquals(0, first.getIntProperty(INDEX), "Unexpected first message index");

            // In DUPS_OK_ACKNOWLEDGE mode this must be ignored by the provider (3.6, 6.2.10).
            first.acknowledge();

            // Second message must still be delivered normally and automatically acknowledged.
            Message second = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(second, "Second message has not been received");
            assertEquals(1, second.getIntProperty(INDEX), "Unexpected second message index");

            // No more messages should be available in this session.
            Message none = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(none, "No additional messages expected after DUPS_OK_ACKNOWLEDGE receive");
        }

        // On a new connection, there must be no messages remaining on the queue
        // if DUPS_OK_ACKNOWLEDGE and ignore-ack semantics are correctly implemented
        // in the absence of provider failures (6.2.10, 6.2.11).
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            final var message = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(message, "Unexpected message received on new connection after DUPS_OK_ACKNOWLEDGE");
        }
    }

    /**
     * Verifies that closing a connection does NOT force an acknowledge of
     * unacknowledged messages in a CLIENT_ACKNOWLEDGE session.
     *
     * Section 6.1.8 "Closing a connection" states that closing a connection:
     * - must NOT force an acknowledge of client acknowledged sessions;
     * - must not cause messages to be lost for queues and durable subscriptions
     *   that require reliable processing by a subsequent execution.
     *
     * This test:
     * - sends two messages 0 and 1 to a queue;
     * - consumes message 0 in CLIENT_ACKNOWLEDGE mode without calling
     *   acknowledge();
     * - closes the connection;
     * - creates a new connection/session and verifies that both messages
     *   0 and 1 are still available and can be consumed in order.
     */
    @Test
    void clientAcknowledgeClosingConnectionDoesNotForceAcknowledge(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        // First connection: send two messages and consume the first one
        // without acknowledging it, then close the connection.
        Message firstReceived;
        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);

            for (int i = 0; i < 2; i++)
            {
                final var message = session.createMessage();
                message.setIntProperty(INDEX, i);
                producer.send(message);
            }

            connection.start();

            firstReceived = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(firstReceived, "First message was not received before connection close");
            assertEquals(0, firstReceived.getIntProperty(INDEX),
                    "Unexpected index for first message before connection close");

            // Do NOT call acknowledge(). When the try-with-resources block exits,
            // the connection (and its session) will be closed. Per 6.1.8 this
            // must not force an acknowledge of unacknowledged messages.
        }

        // Second connection: both messages 0 and 1 must still be available.
        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            Message redelivered0 = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered0,
                    "Message with index 0 should be redelivered after connection close");
            assertEquals(0, redelivered0.getIntProperty(INDEX),
                    "Unexpected index for redelivered first message");

            Message redelivered1 = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered1,
                    "Message with index 1 should still be available after connection close");
            assertEquals(1, redelivered1.getIntProperty(INDEX),
                    "Unexpected index for second message after connection close");

            // Acknowledge only the second message; per CLIENT_ACKNOWLEDGE semantics
            // this cumulatively acknowledges both redelivered messages (6.2.10).
            redelivered1.acknowledge();

            Message none = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(none, "No additional messages expected after cumulative acknowledge");
        }
    }

    /**
     * Verifies that calling Message.acknowledge() on a message received from a
     * connection which has since been closed throws IllegalStateException.
     *
     * Section 6.1.8 "Closing a connection" states that:
     * - closing a connection does NOT force an acknowledge of client acknowledged
     *   sessions;
     * - invoking the acknowledge method of a received message from a closed
     *   connection's sessions must throw IllegalStateException;
     * - it is otherwise valid to continue using message objects created or
     *   received via the connection.
     *
     * This test:
     * - receives a message in CLIENT_ACKNOWLEDGE mode without acknowledging it;
     * - closes the connection;
     * - verifies that a subsequent call to acknowledge() on that message
     *   throws jakarta.jms.IllegalStateException.
     */
    @Test
    void clientAcknowledgeOnMessageAfterConnectionCloseThrowsIllegalStateException(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        Message received;
        // Receive a message on a CLIENT_ACKNOWLEDGE session.
        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);

            final var message = session.createMessage();
            message.setIntProperty(INDEX, 0);
            producer.send(message);

            connection.start();

            received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message was not received before closing connection");
            assertEquals(0, received.getIntProperty(INDEX),
                    "Unexpected index for message before closing connection");

            // Do NOT acknowledge here; simply let the connection be closed
            // by the try-with-resources block.
        }

        // After the connection is closed, calling acknowledge() on a message
        // received from that connection must throw IllegalStateException (6.1.8).
        assertThrows(IllegalStateException.class,
                received::acknowledge,
                "acknowledge() on a message from a closed connection's session must throw IllegalStateException");
    }

    /**
     * Verifies CLIENT_ACKNOWLEDGE semantics for a durable subscription on a Topic.
     *
     * According to section 6.11 "Durable Subscriptions" and 6.2.10 "Message acknowledgment":
     * - a durable subscriber retains messages while the subscription is inactive;
     * - acknowledging a consumed message in CLIENT_ACKNOWLEDGE mode acknowledges all
     *   messages delivered so far to the session;
     * - unacknowledged messages must be redelivered when the durable subscriber
     *   reconnects, while acknowledged messages must not be delivered again.
     *
     * This test:
     * - creates a durable subscription on a Topic in CLIENT_ACKNOWLEDGE mode;
     * - produces three messages 0,1,2;
     * - on the first connection, consumes and acknowledges message 0, consumes
     *   message 1 without acknowledging it, and then closes the connection;
     * - on a second connection, recreates the same durable subscriber and verifies
     *   that:
     *   - message 0 is NOT redelivered;
     *   - message 1 is redelivered (as it was unacknowledged);
     *   - message 2, which was never consumed, is delivered once;
     * - acknowledges the last consumed message on the second connection, which
     *   cumulatively acknowledges all delivered messages;
     * - verifies that no messages remain afterward.
     */
    @Test
    void clientAcknowledgeDurableSubscriptionRedeliversUnacknowledgedMessages(final JmsSupport jms) throws Exception
    {
        Topic topic = jms.builder().topic().create();
        final String clientId = jms.testMethodName() + "_CLIENT";
        final String subscriptionName = jms.testMethodName() + "_SUB";

        // Create durable subscriber
        try (final var consumerConnection = jms.builder().connection().clientId(clientId).create())
        {
            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            consumerSession.createDurableSubscriber(topic, subscriptionName);
        }

        // Produce three ordered messages 0,1,2 to the topic.
        try (final var producerConnection = jms.builder().connection().create())
        {
            Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = producerSession.createProducer(topic);

            for (int i = 0; i < 3; i++)
            {
                final var message = producerSession.createMessage();
                message.setIntProperty(INDEX, i);
                producer.send(message);
            }
        }

        // First connection: durable subscriber receives and acknowledges message 0,
        // receives message 1 without acknowledging it, then closes.
        try (final var consumerConnection = jms.builder().connection().clientId(clientId).create())
        {
            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            TopicSubscriber subscriber = consumerSession.createDurableSubscriber(topic, subscriptionName);

            consumerConnection.start();

            // Receive and acknowledge message 0.
            Message first = subscriber.receive(Timeouts.receiveMillis());
            assertNotNull(first, "First message (index 0) was not received on first connection");
            assertEquals(0, first.getIntProperty(INDEX),
                    "Unexpected index for first message on first connection");
            first.acknowledge();

            // Receive message 1 but do NOT acknowledge it.
            Message second = subscriber.receive(Timeouts.receiveMillis());
            assertNotNull(second, "Second message (index 1) was not received on first connection");
            assertEquals(1, second.getIntProperty(INDEX),
                    "Unexpected index for second message on first connection");

            // Do not consume message 2 on this connection; leave it pending.
            // Closing the connection must NOT implicitly acknowledge message 1
            // (6.1.8, 6.2.10). It must remain available for redelivery.
        }

        // Second connection: recreate the same durable subscription.
        // Message 0 must NOT be redelivered, message 1 must be redelivered,
        // and message 2 must be delivered for the first time.
        try (final var consumerConnection = jms.builder().connection().clientId(clientId).create())
        {
            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            TopicSubscriber subscriber = consumerSession.createDurableSubscriber(topic, subscriptionName);

            consumerConnection.start();

            Message redelivered1 = subscriber.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered1,
                    "Redelivered message 1 was not received on second connection");
            assertEquals(1, redelivered1.getIntProperty(INDEX),
                    "Unexpected index for first message on second connection "
                            + "(message 0 must not be redelivered)");

            Message third = subscriber.receive(Timeouts.receiveMillis());
            assertNotNull(third,
                    "Message 2 was not received on second connection");
            assertEquals(2, third.getIntProperty(INDEX),
                    "Unexpected index for second message on second connection");

            // Acknowledge the last consumed message. In CLIENT_ACKNOWLEDGE this
            // cumulatively acknowledges all messages delivered so far on the session
            // (6.2.10), i.e. both 1 and 2.
            third.acknowledge();

            Message none = subscriber.receive(Timeouts.noMessagesMillis());
            assertNull(none,
                    "No additional messages expected for durable subscription after cumulative acknowledge");
        }
    }

    /**
     * Verifies AUTO_ACKNOWLEDGE semantics for a durable subscription on a Topic.
     *
     * According to section 6.11 "Durable Subscriptions" and 6.2.10
     * "Message acknowledgment":
     * - a durable subscriber retains messages sent while it is inactive and
     *   delivers them when it reconnects;
     * - in AUTO_ACKNOWLEDGE mode, messages are acknowledged automatically when
     *   receive() or onMessage() returns successfully;
     * - acknowledged messages must not be redelivered on a subsequent connection,
     *   while messages that were not yet delivered remain available.
     *
     * This test:
     * - creates a durable subscription on a Topic in AUTO_ACKNOWLEDGE mode;
     * - produces three messages 0,1,2;
     * - on the first connection, consumes only message 0 and then closes
     *   the connection;
     * - on a second connection, recreates the same durable subscriber and
     *   verifies that:
     *   - message 0 is not redelivered (it was auto-acknowledged);
     *   - messages 1 and 2, which were never delivered, are received once;
     *   - no additional messages remain afterwards.
     */
    @Test
    void autoAcknowledgeDurableSubscriptionOnlyDeliversUnconsumedMessagesOnReconnect(final JmsSupport jms) throws Exception
    {
        Topic topic = jms.builder().topic().create();
        final String clientId = jms.testMethodName() + "_CLIENT";
        final String subscriptionName = jms.testMethodName() + "_SUB";

        // Create durable subscriber
        try (final var consumerConnection = jms.builder().connection().clientId(clientId).create())
        {
            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            consumerSession.createDurableSubscriber(topic, subscriptionName);
        }

        // Produce three ordered messages 0,1,2 to the topic.
        try (final var producerConnection = jms.builder().connection().create())
        {
            Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = producerSession.createProducer(topic);

            for (int i = 0; i < 3; i++)
            {
                final var message = producerSession.createMessage();
                message.setIntProperty(INDEX, i);
                producer.send(message);
            }
        }

        // First connection: durable subscriber consumes only message 0,
        // which is automatically acknowledged in AUTO_ACKNOWLEDGE mode.
        try (final var consumerConnection = jms.builder().connection().clientId(clientId).create())
        {
            Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = consumerSession.createDurableSubscriber(topic, subscriptionName);

            consumerConnection.start();

            Message first = subscriber.receive(Timeouts.receiveMillis());
            assertNotNull(first, "First message (index 0) was not received on first connection");
            assertEquals(0, first.getIntProperty(INDEX),
                    "Unexpected index for first message on first connection");

            // Do not attempt to receive further messages on this connection.
            // Message 0 is considered acknowledged automatically when receive()
            // returns successfully (6.2.10). Messages 1 and 2 remain pending
            // in the durable subscription.
        }

        // Second connection: recreate the same durable subscription.
        // Message 0 must NOT be redelivered (it was acknowledged), messages 1
        // and 2 must be delivered once.
        try (final var consumerConnection = jms.builder().connection().clientId(clientId).create())
        {
            Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = consumerSession.createDurableSubscriber(topic, subscriptionName);

            consumerConnection.start();

            Message second = subscriber.receive(Timeouts.receiveMillis());
            assertNotNull(second,
                    "Message 1 was not received on second connection");
            assertEquals(1, second.getIntProperty(INDEX),
                    "Unexpected index for first message on second connection "
                            + "(message 0 must not be redelivered)");

            Message third = subscriber.receive(Timeouts.receiveMillis());
            assertNotNull(third,
                    "Message 2 was not received on second connection");
            assertEquals(2, third.getIntProperty(INDEX),
                    "Unexpected index for second message on second connection");

            Message none = subscriber.receive(Timeouts.noMessagesMillis());
            assertNull(none,
                    "No additional messages expected for durable subscription after AUTO_ACKNOWLEDGE reconnect");
        }
    }

    /**
     * Helper used by several tests to verify basic acknowledgement and redelivery behaviour
     * for the four classic acknowledgement styles:
     * - AUTO_ACKNOWLEDGE, DUPS_OK_ACKNOWLEDGE, CLIENT_ACKNOWLEDGE, SESSION_TRANSACTED
     *   (6.2.10 "Message acknowledgement").
     *
     * It sends two messages, consumes them in order, optionally acknowledges (or commits)
     * the first one depending on the ack mode, and finally delegates to verifyLeftOvers()
     * to check what remains on the queue.
     */
    private void doAcknowledgementTest(final JmsSupport jms, final int ackMode) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(ackMode == Session.SESSION_TRANSACTED, ackMode);
            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            jms.messages().send(session, queue, 2);

            var message = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "First message has not been received");
            assertEquals(0, message.getIntProperty(INDEX), "Unexpected message index received");

            acknowledge(ackMode, session, message);

            message = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Second message has not been received");
            assertEquals(1, message.getIntProperty(INDEX), "Unexpected message index received");
        }

        verifyLeftOvers(jms, ackMode, queue);
    }

    /**
     * Helper used by tests which exercise acknowledgement semantics when messages are
     * delivered asynchronously using a MessageListener (section 8.7 "Receiving messages asynchronously").
     *
     * It:
     * - sends three messages 0,1,2;
     * - installs a MessageListener which removes each index from a set as it is seen;
     * - for index 0, it calls acknowledge(...) or commit() depending on ack mode;
     * - waits for all messages to be delivered, then delegates to verifyLeftOvers()
     *   to check what remains on the queue for the given ack mode.
     */
    private void doMessageListenerAcknowledgementTest(final JmsSupport jms, final int ackMode) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(ackMode == Session.SESSION_TRANSACTED, ackMode);
            MessageConsumer consumer = session.createConsumer(queue);
            MessageProducer producer = session.createProducer(queue);
            connection.start();

            List<Integer> messageIndices = IntStream.rangeClosed(0, 2).boxed().toList();

            // Produce three ordered messages 0,1,2.
            messageIndices.forEach(integer ->
            {
                try
                {
                    final var message = session.createMessage();
                    message.setIntProperty(INDEX, integer);
                    producer.send(message);
                }
                catch (JMSException e)
                {
                    throw new RuntimeException(e);
                }
            });

            if (session.getTransacted())
            {
                // For a transacted session, ensure produced messages are committed
                // before we start consuming (6.2.7 "Transactions").
                session.commit();
            }

            Set<Integer> pendingMessages = new HashSet<>(messageIndices);
            AtomicReference<Exception> exception = new AtomicReference<>();
            CountDownLatch completionLatch = new CountDownLatch(1);

            consumer.setMessageListener(message ->
            {
                try
                {
                    Object index = message.getObjectProperty(INDEX);
                    boolean removed = index instanceof Integer && pendingMessages.remove(index);
                    if (!removed)
                    {
                        throw new IllegalStateException("Message with unexpected index '%s' received".formatted(index));
                    }

                    // For ack modes that require an explicit acknowledge/commit,
                    // acknowledge only the first message; the others will be
                    // covered by cumulative or transactional semantics.
                    if (Integer.valueOf(0).equals(index))
                    {
                        try
                        {
                            acknowledge(ackMode, session, message);
                        }
                        catch (JMSException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                }
                catch (Exception e)
                {
                    exception.set(e);
                }
                finally
                {
                    if (pendingMessages.isEmpty() || exception.get() != null)
                    {
                        completionLatch.countDown();
                    }
                }
            });

            boolean completed = completionLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);
            assertTrue(completed, "Message listener did not receive all messages within permitted timeout %d ms".formatted(TIMEOUT));
            assertNull(exception.get(), "Message listener encountered unexpected exception");
        }

        verifyLeftOvers(jms, ackMode, queue);
    }

    /**
     * Verifies, for each acknowledgement mode, which messages are expected to remain
     * on the queue (or be redelivered) after a new connection is created.
     *
     * The logic is directly aligned with 6.2.10 "Message acknowledgment" and 6.2.11
     * "Duplicate delivery of messages":
     * - SESSION_TRANSACTED / CLIENT_ACKNOWLEDGE:
     *   only messages that were delivered but not covered by a commit()/acknowledge()
     *   call may be redelivered; acknowledged messages must never be delivered again;
     * - AUTO_ACKNOWLEDGE:
     *   all successfully-returned messages must be acknowledged automatically; absent
     *   failures, no message may be redelivered on a new connection;
     * - DUPS_OK_ACKNOWLEDGE:
     *   the spec explicitly allows duplicates if the provider fails, so we do not make
     *   a strong assertion here beyond basic delivery tests elsewhere.
     */
    private void verifyLeftOvers(final JmsSupport jms, final int ackMode, final Queue queue) throws JMSException, NamingException
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(ackMode == Session.SESSION_TRANSACTED, ackMode);
            MessageConsumer consumer = session.createConsumer(queue);

            if (ackMode == Session.SESSION_TRANSACTED || ackMode == Session.CLIENT_ACKNOWLEDGE)
            {
                // In these modes, one message (index 1) is intentionally left unacknowledged
                // by the test logic and must therefore be redelivered on a new connection.
                final var message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Second message has not been received on new connection");
                assertEquals(1, message.getIntProperty(INDEX),
                        "Unexpected message index received after restart");

                acknowledge(ackMode, session, message);
            }
            else if (ackMode == Session.AUTO_ACKNOWLEDGE)
            {
                // For AUTO_ACKNOWLEDGE, all messages should have been acknowledged
                // when receive() or onMessage() returned; no messages should remain.
                final var message = consumer.receive(Timeouts.noMessagesMillis());
                assertNull(message, "Unexpected message received on new connection");
            }
            // With Session.DUPS_OK_ACKNOWLEDGE the spec allows (but does not require)
            // duplicates if the provider fails; we therefore do not assert anything
            // about leftovers for this mode here.
        }
    }

    /**
     * Helper that performs the appropriate acknowledgement operation for the given mode:
     * - SESSION_TRANSACTED: commit() acknowledges all messages consumed in the transaction
     *   (6.2.7 "Transactions", 6.2.10 "Message acknowledgment");
     * - CLIENT_ACKNOWLEDGE: Message.acknowledge() acknowledges all messages delivered
     *   so far in the session (6.2.10);
     * - AUTO_ACKNOWLEDGE / DUPS_OK_ACKNOWLEDGE: no explicit action is required here,
     *   as the session is responsible for acknowledging automatically (6.2.10).
     */
    private void acknowledge(final int ackMode, final Session session, final Message message) throws JMSException
    {
        switch (ackMode)
        {
            case Session.SESSION_TRANSACTED:
                session.commit();
                break;
            case Session.CLIENT_ACKNOWLEDGE:
                message.acknowledge();
                break;
            default:
                // AUTO_ACKNOWLEDGE and DUPS_OK_ACKNOWLEDGE: nothing to do.
        }
    }
}
