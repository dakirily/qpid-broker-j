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

package org.apache.qpid.systests.jms_3_1.acknowledge;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System tests focusing on recover() and related redelivery semantics for the
 * various acknowledgement models defined by Jakarta Messaging.
 *
 * The tests in this class are aligned with:
 * - 3.4.7 "JMSRedelivered"
 * - 3.5.11 "JMSXDeliveryCount"
 * - 6.2.7  "Transactions"
 * - 6.2.9  "Message consumption"
 * - 6.2.10 "Message acknowledgment"
 * - 6.2.11 "Duplicate delivery of messages"
 * - 8.7    "Receiving messages asynchronously"
 */
@JmsSystemTest
@Tag("queue")
class RecoverTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoverTest.class);
    private static final int SENT_COUNT = 4;

    /**
     * JMSXDeliveryCount is a mandatory Jakarta Messaging–defined property which must be set
     * by the provider when a message is delivered to the consumer (section 3.5.11 "JMSXDeliveryCount").
     * It is incremented on redelivery and must be >= 2 whenever JMSRedelivered is true
     * (sections 3.4.7 "JMSRedelivered" and 3.5.11).
     */
    private static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";

    /**
     * Verifies basic recover() behaviour for CLIENT_ACKNOWLEDGE using synchronous receive():
     *
     * According to 6.2.10 "Message acknowledgment" (CLIENT_ACKNOWLEDGE):
     * - acknowledging a message acknowledges all messages delivered so far to the session;
     * - recover() restarts delivery from the first unacknowledged message after the last
     *   acknowledged one.
     *
     * This test:
     * - sends 4 ordered messages 0..3;
     * - acknowledges message 0;
     * - consumes messages 1 and 2 without acknowledging them;
     * - calls recover();
     * - verifies that messages 1 and 2 are redelivered, followed by message 3,
     *   while message 0 is not redelivered.
     */
    @Test
    void testRecoverForClientAcknowledge(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            jms.messages().send(connection, queue, SENT_COUNT);
            connection.start();

            final var message = receiveAndValidateMessage(jms, consumer, 0);
            message.acknowledge();

            receiveAndValidateMessage(jms, consumer, 1);
            receiveAndValidateMessage(jms, consumer, 2);
            session.recover();

            receiveAndValidateMessage(jms, consumer, 1);
            receiveAndValidateMessage(jms, consumer, 2);
            receiveAndValidateMessage(jms, consumer, 3);
        }
    }

    /**
     * Stress-tests recover() in CLIENT_ACKNOWLEDGE to ensure stable message order
     * and correct redelivery behaviour across multiple recover() calls.
     *
     * Section 6.2.9.1 "Order of message receipt" requires that a single consumer
     * on a queue sees messages in the order in which they were sent.
     * Section 6.2.10 "Message acknowledgment" defines that recover() must restart
     * delivery from the first unacknowledged message.
     *
     * This test:
     * - sends SENT_COUNT ordered messages;
     * - for each message index, repeatedly calls recover() several times without
     *   acknowledging;
     * - verifies that the same message index is redelivered until it is acknowledged;
     * - acknowledges the message only after it has been received multiple times;
     * - then verifies that the session advances to the next index, preserving order
     *   and without skipping or duplicating extra messages.
     */
    @Test
    void testMessageOrderForClientAcknowledge(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            jms.messages().send(connection, queue, SENT_COUNT);
            connection.start();

            int messageSeen = 0;
            int expectedIndex = 0;
            while (expectedIndex < SENT_COUNT)
            {
                final var message = receiveAndValidateMessage(jms, consumer, expectedIndex);

                //don't ack the message until we receive it 5 times
                if (messageSeen < 5)
                {
                    LOGGER.debug("Recovering message with index {}", expectedIndex);
                    session.recover();
                    messageSeen++;
                }
                else
                {
                    LOGGER.debug("Acknowledging message with index {}", expectedIndex);
                    messageSeen = 0;
                    expectedIndex++;
                    message.acknowledge();
                }
            }
        }
    }

    /**
     * Verifies acknowledgement boundaries for CLIENT_ACKNOWLEDGE in a session with
     * multiple consumers.
     *
     * Section 6.2.10 "Message acknowledgment" (CLIENT_ACKNOWLEDGE) states that
     * acknowledge() confirms all messages that have been delivered to the session
     * up to that point. Messages not yet delivered when acknowledge() is called
     * must not be affected.
     *
     * This test:
     * - creates one CLIENT_ACKNOWLEDGE session with two consumers on separate queues;
     * - produces "msg1" for queue1 and "msg2" for queue2;
     * - consumes and acknowledges "msg2" on consumer2;
     * - calls recover() on the session;
     * - verifies that "msg1" is delivered on consumer1, proving that a message
     *   which was not yet delivered at the time of acknowledge() remains available
     *   and is not implicitly acknowledged.
     */
    @Test
    void testAcknowledgePerConsumer(final JmsSupport jms) throws Exception
    {
        Queue queue1 = jms.builder().queue("Q1").create();
        Queue queue2 = jms.builder().queue("Q2").create();

        try (final var consumerConnection = jms.builder().connection().create())
        {
            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer1 = consumerSession.createConsumer(queue1);
            MessageConsumer consumer2 = consumerSession.createConsumer(queue2);

            try (final var producerConnection = jms.builder().connection().create())
            {
                Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer1 = producerSession.createProducer(queue1);
                MessageProducer producer2 = producerSession.createProducer(queue2);

                producer1.send(producerSession.createTextMessage("msg1"));
                producer2.send(producerSession.createTextMessage("msg2"));
            }
            consumerConnection.start();

            TextMessage message2 = (TextMessage) consumer2.receive(Timeouts.receiveMillis());
            assertNotNull(message2);
            assertEquals("msg2", message2.getText());

            message2.acknowledge();
            consumerSession.recover();

            TextMessage message1 = (TextMessage) consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(message1);
            assertEquals("msg1", message1.getText());
        }
    }

    /**
     * Verifies recover() behaviour when used from within a MessageListener on an
     * AUTO_ACKNOWLEDGE session.
     *
     * Section 6.2.10 "Message acknowledgment" specifies that messages in
     * AUTO_ACKNOWLEDGE are acknowledged automatically when onMessage() returns
     * successfully. Section 3.4.7 "JMSRedelivered" describes that a redelivered
     * message must have JMSRedelivered=true.
     *
     * This test:
     * - creates an AUTO_ACKNOWLEDGE session with a MessageListener;
     * - on first delivery, it calls recover() from onMessage();
     * - on second delivery, it verifies that JMSRedelivered is true;
     * - asserts that the message is delivered exactly twice and that no unexpected
     *   exceptions occur in the listener.
     */
    @Test
    void testRecoverInAutoAckListener(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            jms.messages().send(connection, queue, 1);

            final CountDownLatch awaitMessages = new CountDownLatch(2);
            final AtomicReference<Throwable> listenerCaughtException = new AtomicReference<>();
            final AtomicInteger deliveryCounter = new AtomicInteger();
            consumer.setMessageListener(message -> {
                try
                {
                    deliveryCounter.incrementAndGet();
                    assertEquals(deliveryCounter.get() > 1, message.getJMSRedelivered(), "Unexpected JMSRedelivered");
                    if (deliveryCounter.get() == 1)
                    {
                        consumerSession.recover();
                    }
                }
                catch (Throwable e)
                {
                    LOGGER.error("Unexpected failure on message receiving", e);
                    listenerCaughtException.set(e);
                }
                finally
                {
                    awaitMessages.countDown();
                }
            });

            connection.start();

            assertTrue(awaitMessages.await(Timeouts.receiveMillis() * 2, TimeUnit.MILLISECONDS),
                    "Message is not received in timely manner");
            assertEquals(2, deliveryCounter.get(), "Message not received the correct number of times.");
            assertNull(listenerCaughtException.get(),
                    "No exception should be caught by listener : " + listenerCaughtException.get());
        }
    }

    /**
     * Verifies recover() behaviour for CLIENT_ACKNOWLEDGE together with
     * JMSRedelivered and JMSXDeliveryCount requirements.
     *
     * According to 6.2.10:
     * - recover() "restarts" the session from the first unacknowledged message, after
     *   the last acknowledged one;
     * - messages redelivered due to recover() must have JMSRedelivered set and
     *   JMSXDeliveryCount incremented (also 3.4.7 "JMSRedelivered", 3.5.11 "JMSXDeliveryCount").
     *
     * This test:
     * - acknowledges message 0;
     * - consumes message 1 without acknowledging it;
     * - calls recover();
     * - verifies that message 1 is redelivered, JMSRedelivered is true and
     *   JMSXDeliveryCount has increased compared to the first delivery;
     * - then consumes message 2 normally.
     */
    @Test
    void clientAcknowledgementRecoverRedeliversUnacknowledgedWithRedeliveredAndDeliveryCount(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);

            // Produce three ordered messages 0,1,2.
            for (int i = 0; i < 3; i++)
            {
                final var message = session.createMessage();
                message.setIntProperty(INDEX, i);
                producer.send(message);
            }

            connection.start();

            // 1) Consume and acknowledge message 0.
            Message first = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(first, "First message was not received");
            assertEquals(0, first.getIntProperty(INDEX), "Unexpected index for first message");
            first.acknowledge();

            // 2) Consume message 1 but DO NOT acknowledge it.
            Message second = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(second, "Second message was not received");
            assertEquals(1, second.getIntProperty(INDEX), "Unexpected index for second message");

            int firstDeliveryCount = second.getIntProperty(JMSX_DELIVERY_COUNT);
            assertTrue(firstDeliveryCount >= 1,
                    "Initial JMSXDeliveryCount should be present and >= 1 on first delivery");

            // 3) recover() should restart the session from the first unacknowledged message,
            // i.e. message 1 (6.2.10 "Message acknowledgment").
            session.recover();

            // 4) After recover(), message 1 must be redelivered with JMSRedelivered=true
            // and JMSXDeliveryCount incremented (6.2.10, 3.4.7, 3.5.11).
            Message redelivered = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered, "Redelivered message was not received after recover()");
            assertEquals(1, redelivered.getIntProperty(INDEX),
                    "Unexpected index for redelivered message after recover()");
            assertTrue(redelivered.getJMSRedelivered(),
                    "JMSRedelivered must be true for a message redelivered due to recover()");

            int redeliveryCount = redelivered.getIntProperty(JMSX_DELIVERY_COUNT);
            assertTrue(redeliveryCount >= firstDeliveryCount + 1,
                    "JMSXDeliveryCount should be incremented on redelivery via recover()");

            // Acknowledge the redelivered message; this also acknowledges any other
            // messages delivered so far in CLIENT_ACKNOWLEDGE (6.2.10).
            redelivered.acknowledge();

            // 5) Consume the remaining message 2 and ensure it is delivered once.
            Message third = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(third, "Third message was not received");
            assertEquals(2, third.getIntProperty(INDEX), "Unexpected index for third message");

            // No further messages expected in this session.
            Message none = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(none, "No additional messages expected after recover()/ack scenario");
        }
    }

    /**
     * Verifies rollback semantics for SESSION_TRANSACTED together with
     * JMSRedelivered and JMSXDeliveryCount.
     *
     * Sections 6.2.7 "Transactions" and 6.2.10 "Message acknowledgment" define that:
     * - in a transacted session, message acknowledgment is tied to commit();
     * - rollback() destroys produced messages and automatically recovers consumed messages;
     * - a message redelivered due to recovery must have JMSRedelivered set and
     *   JMSXDeliveryCount incremented (6.2.10, 3.4.7, 3.5.11).
     *
     * This test:
     * - produces two messages in a committed transaction;
     * - consumes message 0 and records its JMSXDeliveryCount;
     * - rolls back, expecting message 0 to be redelivered with JMSRedelivered=true
     *   and increased JMSXDeliveryCount;
     * - then commits consumption of the redelivered message and consumes message 1.
     */
    @Test
    void sessionTransactedRollbackRedeliversMessagesWithRedeliveredAndDeliveryCount(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);

            // Produce two ordered messages in a single transaction.
            for (int i = 0; i < 2; i++)
            {
                final var message = session.createMessage();
                message.setIntProperty(INDEX, i);
                producer.send(message);
            }
            session.commit();

            connection.start();

            // 1) Consume message 0 within a new transaction.
            Message first = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(first, "First message was not received");
            assertEquals(0, first.getIntProperty(INDEX), "Unexpected index for first message");

            int initialDeliveryCount = first.getIntProperty(JMSX_DELIVERY_COUNT);
            assertTrue(initialDeliveryCount >= 1,
                    "Initial JMSXDeliveryCount should be present and >= 1 on first delivery");

            // 2) Roll back the transaction. Consumed messages must be automatically recovered
            // (6.2.7 "Transactions").
            session.rollback();

            // 3) After rollback, message 0 must be redelivered with JMSRedelivered=true
            // and JMSXDeliveryCount incremented (6.2.10, 3.4.7, 3.5.11).
            Message redelivered = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered, "Redelivered message was not received after rollback");
            assertEquals(0, redelivered.getIntProperty(INDEX),
                    "Unexpected index for redelivered message after rollback");
            assertTrue(redelivered.getJMSRedelivered(),
                    "JMSRedelivered must be true for a message redelivered due to rollback");

            int redeliveryCount = redelivered.getIntProperty(JMSX_DELIVERY_COUNT);
            assertTrue(redeliveryCount >= initialDeliveryCount + 1,
                    "JMSXDeliveryCount should be incremented on redelivery via rollback");

            // 4) Commit the transaction that consumes the redelivered message.
            session.commit();

            // 5) Now consume message 1 and commit to acknowledge it as well.
            Message second = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(second, "Second message was not received");
            assertEquals(1, second.getIntProperty(INDEX), "Unexpected index for second message");
            session.commit();

            // No further messages should remain.
            Message none = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(none, "No additional messages expected after commit/rollback scenario");
        }
    }

    /**
     * Verifies recover() behaviour for AUTO_ACKNOWLEDGE when messages are consumed
     * synchronously using receive().
     *
     * Section 6.2.10 "Message acknowledgment" states that in AUTO_ACKNOWLEDGE mode
     * messages are automatically acknowledged when receive() returns successfully.
     * Section 6.2.11 "Duplicate delivery of messages" says that, in the absence of
     * failures, acknowledged messages must not be delivered again.
     *
     * This test:
     * - uses AUTO_ACKNOWLEDGE;
     * - consumes a message synchronously via receive();
     * - calls recover();
     * - verifies that the message is not redelivered, showing that recover()
     *   does not resurrect already acknowledged messages in AUTO_ACKNOWLEDGE mode.
     */
    @Test
    void autoAcknowledgeRecoverAfterSynchronousReceiveDoesNotRedeliver(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);

            // Produce a single message.
            final var message = session.createMessage();
            message.setIntProperty(INDEX, 0);
            producer.send(message);

            connection.start();

            // Consume the message synchronously. In AUTO_ACKNOWLEDGE mode this
            // should automatically acknowledge it (6.2.10).
            Message first = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(first, "First message was not received");
            assertEquals(0, first.getIntProperty(INDEX), "Unexpected index for first message");

            // recover() must not redeliver an already acknowledged message.
            session.recover();

            Message redelivered = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(redelivered,
                    "AUTO_ACKNOWLEDGE message must not be redelivered after recover()");
        }
    }

    /**
     * Verifies recover() behaviour for DUPS_OK_ACKNOWLEDGE together with
     * JMSRedelivered and JMSXDeliveryCount when messages are delivered via a
     * MessageListener.
     *
     * Section 6.2.10 "Message acknowledgment" states that DUPS_OK_ACKNOWLEDGE is an
     * automatic acknowledgement mode which may lazily acknowledge messages and can
     * result in duplicates if the provider fails. However, for any redelivered
     * message, the provider must still obey the JMSRedelivered (3.4.7) and
     * JMSXDeliveryCount (3.5.11) requirements.
     *
     * This test:
     * - creates a DUPS_OK_ACKNOWLEDGE session and sends a single message;
     * - on the first delivery, records JMSXDeliveryCount and calls recover();
     * - on the second delivery, verifies that JMSRedelivered is true and that
     *   JMSXDeliveryCount has increased compared to the first delivery.
     */
    @Test
    void dupsOkRecoverInListenerSetsRedeliveredAndDeliveryCount(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            MessageProducer producer = session.createProducer(queue);

            // Produce a single message.
            Message sent = session.createMessage();
            sent.setIntProperty(INDEX, 0);
            producer.send(sent);

            CountDownLatch awaitMessages = new CountDownLatch(2);
            AtomicReference<Throwable> listenerCaughtException = new AtomicReference<>();
            AtomicInteger deliveryCounter = new AtomicInteger();
            AtomicReference<Integer> firstDeliveryCount = new AtomicReference<>();

            consumer.setMessageListener(message -> {
                try
                {
                    int attempt = deliveryCounter.incrementAndGet();
                    int deliveryCount = message.getIntProperty(JMSX_DELIVERY_COUNT);
                    if (attempt == 1)
                    {
                        // First delivery: JMSRedelivered must be false and deliveryCount >= 1.
                        assertFalse(message.getJMSRedelivered(),
                                "JMSRedelivered must be false on first delivery");
                        assertTrue(deliveryCount >= 1,
                                "Initial JMSXDeliveryCount should be present and >= 1");
                        firstDeliveryCount.set(deliveryCount);
                        // Trigger redelivery via recover().
                        session.recover();
                    }
                    else
                    {
                        // Second delivery after recover(): JMSRedelivered must be true and
                        // JMSXDeliveryCount must have increased (3.4.7, 3.5.11, 6.2.10).
                        assertTrue(message.getJMSRedelivered(),
                                "JMSRedelivered must be true after recover() in DUPS_OK_ACKNOWLEDGE");
                        assertTrue(deliveryCount >= firstDeliveryCount.get() + 1,
                                "JMSXDeliveryCount should be incremented on redelivery via recover()");
                    }
                }
                catch (Throwable e)
                {
                    listenerCaughtException.set(e);
                }
                finally
                {
                    awaitMessages.countDown();
                }
            });

            connection.start();

            assertTrue(awaitMessages.await(Timeouts.receiveMillis() * 2, TimeUnit.MILLISECONDS),
                    "Message was not delivered the expected number of times");
            assertEquals(2, deliveryCounter.get(),
                    "Message not received the correct number of times.");
            assertNull(listenerCaughtException.get(),
                    "No exception should be caught by listener: " + listenerCaughtException.get());
        }
    }

    /**
     * Verifies that recover() must not be called on a transacted session.
     *
     * The Jakarta Messaging API specifies (via the Session javadoc and section 6.2.7
     * "Transactions") that Session.recover() is only valid for non-transacted sessions.
     * For a transacted session (SESSION_TRANSACTED), recover() must throw
     * IllegalStateException.
     *
     * This test:
     * - creates a SESSION_TRANSACTED session;
     * - calls recover();
     * - asserts that IllegalStateException is thrown.
     */
    @Test
    void recoverOnTransactedSessionThrowsIllegalStateException(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            @SuppressWarnings("unused")
            MessageConsumer consumer = session.createConsumer(queue);

            assertThrows(IllegalStateException.class,
                    session::recover,
                    "recover() must throw IllegalStateException for transacted sessions");
        }
    }

    /**
     * Verifies that CLIENT_ACKNOWLEDGE acknowledgement is cumulative across all
     * consumers in the same session, not just for a single consumer.
     *
     * Section 6.2.10 "Message acknowledgment" (CLIENT_ACKNOWLEDGE) states that:
     * "Acknowledging a consumed message automatically acknowledges the receipt of all
     * messages that have been delivered by its session."
     *
     * This test:
     * - creates a CLIENT_ACKNOWLEDGE session with two MessageConsumers on two different queues;
     * - sends one message to each queue and consumes both;
     * - calls acknowledge() only on the message received by the second consumer;
     * - verifies on a new connection that no messages are redelivered on either queue,
     *   demonstrating that the acknowledge() was cumulative across both consumers in
     *   the same session.
     */
    @Test
    void clientAcknowledgeIsCumulativeAcrossConsumers(final JmsSupport jms) throws Exception
    {
        Queue queue1 = jms.builder().queue().nameSuffix("_Q1").create();
        Queue queue2 = jms.builder().queue().nameSuffix("_Q2").create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer1 = session.createConsumer(queue1);
            MessageConsumer consumer2 = session.createConsumer(queue2);
            MessageProducer producer1 = session.createProducer(queue1);
            MessageProducer producer2 = session.createProducer(queue2);

            // Produce one message for each queue.
            Message message1 = session.createMessage();
            message1.setIntProperty(INDEX, 0);
            producer1.send(message1);

            Message message2 = session.createMessage();
            message2.setIntProperty(INDEX, 1);
            producer2.send(message2);

            connection.start();

            // Consume both messages, one from each consumer.
            Message received1 = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(received1, "Message from queue1 was not received");
            assertEquals(0, received1.getIntProperty(INDEX),
                    "Unexpected index for message from queue1");

            Message received2 = consumer2.receive(Timeouts.receiveMillis());
            assertNotNull(received2, "Message from queue2 was not received");
            assertEquals(1, received2.getIntProperty(INDEX),
                    "Unexpected index for message from queue2");

            // Acknowledge only the message from the second consumer. Per 6.2.10 this
            // must cumulatively acknowledge all messages delivered by the session,
            // including those received by other consumers.
            received2.acknowledge();
        }

        // On a new connection, neither queue should have any messages left if cumulative
        // CLIENT_ACKNOWLEDGE semantics are correctly implemented across consumers.
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer1 = session.createConsumer(queue1);
            MessageConsumer consumer2 = session.createConsumer(queue2);

            Message leftover1 = consumer1.receive(Timeouts.noMessagesMillis());
            assertNull(leftover1,
                    "No messages should be redelivered on queue1 after cumulative CLIENT_ACKNOWLEDGE");

            Message leftover2 = consumer2.receive(Timeouts.noMessagesMillis());
            assertNull(leftover2,
                    "No messages should be redelivered on queue2 after cumulative CLIENT_ACKNOWLEDGE");
        }
    }

    /**
     * Verifies that recover() in AUTO_ACKNOWLEDGE mode when using a
     * MessageListener increments JMSXDeliveryCount on redelivery as required
     * by the specification.
     *
     * According to:
     * - 3.4.7 "JMSRedelivered": redelivered messages must have JMSRedelivered=true;
     * - 3.5.11 "JMSXDeliveryCount": JMSXDeliveryCount must be set to the number
     *   of times the message has been delivered, >= 2 whenever JMSRedelivered
     *   is true;
     * - 6.2.10 "Message acknowledgment": recover() restarts message delivery
     *   from the first unacknowledged message and causes redelivery.
     *
     * This test:
     * - uses AUTO_ACKNOWLEDGE with a MessageListener;
     * - on first delivery, records JMSXDeliveryCount, asserts JMSRedelivered=false
     *   and calls recover();
     * - on second delivery, asserts JMSRedelivered=true and
     *   JMSXDeliveryCount >= firstDeliveryCount + 1.
     */
    @Test
    void autoAcknowledgeRecoverInListenerIncrementsDeliveryCount(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            MessageProducer producer = session.createProducer(queue);

            // Produce a single message.
            Message sent = session.createMessage();
            sent.setIntProperty(INDEX, 0);
            producer.send(sent);

            CountDownLatch awaitMessages = new CountDownLatch(2);
            AtomicReference<Throwable> listenerCaughtException = new AtomicReference<>();
            AtomicInteger deliveryCounter = new AtomicInteger();
            AtomicReference<Integer> firstDeliveryCount = new AtomicReference<>();

            consumer.setMessageListener(message -> {
                try
                {
                    int attempt = deliveryCounter.incrementAndGet();
                    int deliveryCount = message.getIntProperty(JMSX_DELIVERY_COUNT);
                    assertTrue(deliveryCount >= 1,
                            "JMSXDeliveryCount should be present and >= 1 on each delivery");

                    if (attempt == 1)
                    {
                        // First delivery: JMSRedelivered must be false and
                        // deliveryCount must be >= 1.
                        assertFalse(message.getJMSRedelivered(),
                                "JMSRedelivered must be false on first delivery");
                        firstDeliveryCount.set(deliveryCount);

                        // Trigger redelivery via recover().
                        session.recover();
                    }
                    else
                    {
                        // Second delivery after recover(): JMSRedelivered must be true and
                        // JMSXDeliveryCount must have increased compared to the first delivery.
                        assertTrue(message.getJMSRedelivered(),
                                "JMSRedelivered must be true on redelivery via recover()");
                        assertTrue(deliveryCount >= firstDeliveryCount.get() + 1,
                                "JMSXDeliveryCount should be incremented on redelivery via recover()");
                    }
                }
                catch (Throwable e)
                {
                    listenerCaughtException.set(e);
                }
                finally
                {
                    awaitMessages.countDown();
                }
            });

            connection.start();

            assertTrue(awaitMessages.await(Timeouts.receiveMillis() * 2L, TimeUnit.MILLISECONDS),
                    "Message was not delivered the expected number of times");
            assertEquals(2, deliveryCounter.get(),
                    "Message not received the correct number of times.");
            assertNull(listenerCaughtException.get(),
                    "No exception should be caught by listener: " + listenerCaughtException.get());
        }
    }

    /**
     * Verifies that closing a connection rolls back any in-progress transactions
     * on its transacted sessions, causing consumed but uncommitted messages to
     * be redelivered.
     *
     * Section 6.1.8 "Closing a connection" states that:
     * - closing a connection must rollback the transactions in progress on its
     *   transacted sessions;
     * - these semantics ensure that closing a connection does not cause messages
     *   to be lost for queues and durable subscriptions which require reliable
     *   processing by a subsequent execution of their client.
     *
     * This test:
     * - creates a SESSION_TRANSACTED session and produces two messages 0 and 1
     *   in a committed transaction;
     * - starts a new transaction, consumes message 0 but does NOT commit;
     * - closes the connection (which must roll back the transaction);
     * - on a new connection, verifies that both messages 0 and 1 are still
     *   available and can be consumed and committed in order.
     */
    @Test
    void sessionTransactedConnectionCloseRollsBackUncommittedTransaction(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        // First connection: produce messages and consume the first one in a
        // transaction that is never committed.
        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);

            // Produce two ordered messages 0 and 1 in a single transaction.
            for (int i = 0; i < 2; i++)
            {
                final var message = session.createMessage();
                message.setIntProperty(INDEX, i);
                producer.send(message);
            }
            session.commit();

            // Start consuming in a new transaction.
            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            Message first = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(first, "First message was not received before connection close");
            assertEquals(0, first.getIntProperty(INDEX),
                    "Unexpected index for first message before connection close");

            // Do NOT call commit() or rollback() explicitly. Exiting the
            // try-with-resources block will close the connection. Per 6.1.8 this
            // must rollback the in-progress transaction, causing message 0 to be
            // redelivered on a subsequent connection.
        }

        // Second connection: both messages 0 and 1 must still be available.
        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            Message redelivered0 = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered0,
                    "First message was not redelivered after connection close");
            assertEquals(0, redelivered0.getIntProperty(INDEX),
                    "Unexpected index for first message after connection close rollback");
            session.commit();

            Message second = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(second,
                    "Second message was not received after connection close rollback");
            assertEquals(1, second.getIntProperty(INDEX),
                    "Unexpected index for second message after connection close rollback");
            session.commit();

            Message none = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(none,
                    "No additional messages expected after transactional close rollback scenario");
        }
    }

    /**
     * Helper that receives the next message from the given consumer and verifies
     * that it has the expected INDEX property.
     *
     * Section 6.2.9.1 "Order of message receipt" requires that a single consumer
     * on a queue sees messages in the order in which they were sent. This method
     * enforces that property by asserting the expected index for each received
     * message.
     *
     * @param consumer     the consumer to receive from
     * @param messageIndex the expected INDEX value
     * @return the received Message
     * @throws JMSException if the receive or property access fails
     */
    private Message receiveAndValidateMessage(final JmsSupport jms,
                                              final MessageConsumer consumer,
                                              final int messageIndex)
            throws JMSException
    {
        final var message = consumer.receive(Timeouts.receiveMillis());
        assertNotNull(message, "Expected message '%d' is not received".formatted(messageIndex));
        assertEquals(messageIndex, message.getIntProperty(INDEX), "Received message out of order");
        return message;
    }
}