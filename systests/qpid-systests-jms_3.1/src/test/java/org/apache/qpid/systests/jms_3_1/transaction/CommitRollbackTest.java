/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 *
 */

package org.apache.qpid.systests.jms_3_1.transaction;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.IllegalStateException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("transactions")
class CommitRollbackTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitRollbackTest.class);

    @Test
    void produceMessageAndAbortTransactionByClosingConnection(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED))
        {
            final var messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("A"));
        }

        try (final var connection2 = jms.builder().connection().create())
        {
            connection2.start();
            final var session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("B"));

            final var messageConsumer = session.createConsumer(queue);
            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Text message should be received");
            assertEquals("B", textMessage.getText(), "Unexpected message received");
        }
    }

    @Test
    void produceMessageAndAbortTransactionByClosingSession(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var transactedProducer = transactedSession.createProducer(queue))
        {
            transactedProducer.send(transactedSession.createTextMessage("A"));
            transactedSession.close();

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("B"));

            connection.start();
            final var messageConsumer = session.createConsumer(queue);
            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Text message should be received");
            assertEquals("B", textMessage.getText(), "Unexpected message received");
        }
    }

    @Test
    void produceMessageAndRollbackTransaction(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var messageProducer = session.createProducer(queue))
        {
            messageProducer.send(session.createTextMessage("A"));
            session.rollback();
        }

        try (final var connection2 = jms.builder().connection().create();
             final var session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var messageProducer = session.createProducer(queue))
        {
            messageProducer.send(session.createTextMessage("B"));

            final var messageConsumer = session.createConsumer(queue);
            connection2.start();

            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Text message should be received");
            assertEquals("B", textMessage.getText(), "Unexpected message received");
        }
    }

    @Test
    void produceMessageAndCommitTransaction(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var messageProducer = session.createProducer(queue))
        {
            messageProducer.send(session.createTextMessage("A"));
            session.commit();
        }

        try (final var connection2 = jms.builder().connection().create();
             final var session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var messageConsumer = session.createConsumer(queue))
        {
            connection2.start();

            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Text message should be received");
            assertEquals("A", textMessage.getText(), "Unexpected message received");
        }
    }

    @Test
    void receiveMessageAndAbortTransactionByClosingConnection(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            jms.messages().send(connection, queue, "A");

            connection.start();
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var messageConsumer = session.createConsumer(queue);
            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Text message should be received");
            assertEquals("A", textMessage.getText(), "Unexpected message received");
        }

        try (final var connection2 = jms.builder().connection().create())
        {
            connection2.start();
            final var session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var messageConsumer = session.createConsumer(queue);

            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Text message should be received");
            assertEquals("A", textMessage.getText(), "Unexpected message received");
        }
    }

    @Test
    void receiveMessageAndAbortTransactionByClosingSession(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            jms.messages().send(connection, queue, "A");

            connection.start();
            final var transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var transactedConsumer = transactedSession.createConsumer(queue);
            final var message = transactedConsumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Text message should be received");
            assertEquals("A", textMessage.getText(), "Unexpected message received");

            transactedSession.close();

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var messageConsumer = session.createConsumer(queue);

            final var message2 = messageConsumer.receive(Timeouts.receiveMillis());
            final var textMessage2 = assertInstanceOf(TextMessage.class, message2, "Text message should be received");
            assertEquals("A", textMessage2.getText(), "Unexpected message received");
        }
    }

    @Test
    void receiveMessageAndRollbackTransaction(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            jms.messages().send(connection, queue, "A");

            connection.start();
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var messageConsumer = session.createConsumer(queue);
            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Text message should be received");
            assertEquals("A", textMessage.getText(), "Unexpected message received");
            session.rollback();
        }

        try (final var connection2 = jms.builder().connection().create();
             final var session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var messageConsumer = session.createConsumer(queue))
        {
            connection2.start();

            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Text message should be received");
            assertEquals("A", textMessage.getText(), "Unexpected message received");
        }
    }

    @Test
    void receiveMessageAndCommitTransaction(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            jms.messages().send(connection, queue, 2);

            connection.start();
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var messageConsumer = session.createConsumer(queue);
            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Message not received");
            assertEquals(0, message.getIntProperty(INDEX), "Unexpected message received");
            session.commit();
        }

        try (final var connection2 = jms.builder().connection().create();
             final var session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var messageConsumer = session.createConsumer(queue))
        {
            connection2.start();

            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Message not received");
            assertEquals(1, message.getIntProperty(INDEX), "Unexpected message received");
        }
    }

    @Test
    void receiveMessageCloseConsumerAndCommitTransaction(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            jms.messages().send(connection, queue, 2);

            connection.start();
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var messageConsumer = session.createConsumer(queue);
            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Message not received");
            assertEquals(0, message.getIntProperty(INDEX), "Unexpected message received");
            messageConsumer.close();
            session.commit();
        }

        try (final var connection2 = jms.builder().connection().create();
             final var session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var messageConsumer = session.createConsumer(queue))
        {
            connection2.start();

            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Message not received");
            assertEquals(1, message.getIntProperty(INDEX), "Unexpected message received");
        }
    }

    @Test
    void receiveMessageCloseConsumerAndRollbackTransaction(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            jms.messages().send(connection, queue, 2);

            connection.start();
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var messageConsumer = session.createConsumer(queue);
            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Message not received");
            assertEquals(0, message.getIntProperty(INDEX), "Unexpected message received");
            messageConsumer.close();
            session.rollback();
        }

        try (final var connection2 = jms.builder().connection().create();
             final var session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var messageConsumer = session.createConsumer(queue))
        {
            connection2.start();

            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Message not received");
            assertEquals(0, message.getIntProperty(INDEX), "Unexpected message received");
        }
    }

    @Test
    void transactionSharedByConsumers(final JmsSupport jms) throws Exception
    {
        final var queue1 = jms.builder().queue("Q1").create();
        final var queue2 = jms.builder().queue("Q2").create();

        try (final var connection = jms.builder().connection().create())
        {
            jms.messages().send(connection, queue1, "queue1Message1");
            jms.messages().send(connection, queue1, "queue1Message2");
            jms.messages().send(connection, queue2, "queue2Message1");
            jms.messages().send(connection, queue2, "queue2Message2");

            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var messageConsumer1 = session.createConsumer(queue1);
            final var messageConsumer2 = session.createConsumer(queue2);
            connection.start();

            var message1 = messageConsumer1.receive(Timeouts.receiveMillis());
            var textMessage1 = assertInstanceOf(TextMessage.class, message1, "Text message not received from first queue");
            assertEquals("queue1Message1", textMessage1.getText(),
                    "Unexpected message received from first queue");

            var message2 = messageConsumer2.receive(Timeouts.receiveMillis());
            var textMessage2 = assertInstanceOf(TextMessage.class, message2, "Text message not received from second queue");
            assertEquals("queue2Message1", textMessage2.getText(),
                    "Unexpected message received from second queue");

            session.rollback();

            message1 = messageConsumer1.receive(Timeouts.receiveMillis());
            textMessage1 = assertInstanceOf(TextMessage.class, message1, "Text message not received from first queue");
            assertEquals("queue1Message1", textMessage1.getText(),
                    "Unexpected message received from first queue");

            message2 = messageConsumer2.receive(Timeouts.receiveMillis());
            textMessage2 = assertInstanceOf(TextMessage.class, message2, "Text message not received from second queue");
            assertEquals("queue2Message1", textMessage2.getText(),
                    "Unexpected message received from second queue");

            session.commit();

            final var message3 = messageConsumer1.receive(Timeouts.receiveMillis());
            final var textMessage3 = assertInstanceOf(TextMessage.class, message3, "Text message not received from first queue");
            assertEquals("queue1Message2", textMessage3.getText(),
                    "Unexpected message received from first queue");

            final var message4 = messageConsumer2.receive(Timeouts.receiveMillis());
            final var textMessage4 = assertInstanceOf(TextMessage.class, message4, "Text message not received from second queue");
            assertEquals("queue2Message2", textMessage4.getText(),
                    "Unexpected message received from second queue");
        }
    }

    @Test
    void commitWithinMessageListener(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        int messageNumber = 2;
        try (final var connection = jms.builder().connection().create())
        {
            jms.messages().send(connection, queue, messageNumber);
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var receiveLatch = new CountDownLatch(messageNumber);
            final var commitCounter = new AtomicInteger();
            final AtomicReference<Throwable> messageConsumerThrowable = new AtomicReference<>();
            final var messageConsumer = session.createConsumer(queue);

            messageConsumer.setMessageListener(message ->
            {
                try
                {
                    LOGGER.info("received message {}", message);
                    assertEquals(commitCounter.get(), message.getIntProperty(INDEX), "Unexpected message received");
                    LOGGER.info("commit session");
                    session.commit();
                    commitCounter.incrementAndGet();
                }
                catch (Throwable e)
                {
                    messageConsumerThrowable.set(e);
                    LOGGER.error("Unexpected exception", e);
                }
                finally
                {
                    receiveLatch.countDown();
                }
            });
            connection.start();

            assertTrue(receiveLatch.await(Timeouts.receiveMillis() * messageNumber, TimeUnit.MILLISECONDS),
                    "Messages not received in expected time");
            assertNull(messageConsumerThrowable.get(), "Unexpected exception: " + messageConsumerThrowable.get());
            assertEquals(messageNumber, commitCounter.get(), "Unexpected number of commits");
        }
    }

    @Test
    void rollbackWithinMessageListener(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var consumer = session.createConsumer(queue))
        {
            jms.messages().send(session, queue, 2);
            connection.start();
            final var receiveLatch = new CountDownLatch(2);
            final var receiveCounter = new AtomicInteger();
            final AtomicReference<Throwable> messageListenerThrowable = new AtomicReference<>();
            consumer.setMessageListener(message -> {
                try
                {
                    if (receiveCounter.incrementAndGet()<3)
                    {
                        session.rollback();
                    }
                    else
                    {
                        session.commit();
                        receiveLatch.countDown();
                    }
                }
                catch (Throwable e)
                {
                    messageListenerThrowable.set(e);
                }
            });

            assertTrue(receiveLatch.await(Timeouts.receiveMillis() * 4, TimeUnit.MILLISECONDS),
                                  "Timeout waiting for messages");
            assertNull(messageListenerThrowable.get(),
                                  "Exception occurred: " + messageListenerThrowable.get());
            assertEquals(4, receiveCounter.get(), "Unexpected number of received messages");
        }
    }

    @Test
    void exhaustedPrefetchInTransaction(final JmsSupport jms) throws Exception
    {
        final int maxPrefetch = 2;
        final int messageNumber = maxPrefetch + 1;

        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().prefetch(maxPrefetch).create())
        {
            jms.messages().send(connection, queue, messageNumber);
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var messageConsumer = session.createConsumer(queue);
            connection.start();

            for (int i = 0; i < maxPrefetch; i++)
            {
                final var message = messageConsumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Message %d not received".formatted(i));
                assertEquals(i, message.getIntProperty(INDEX), "Unexpected message received");
            }

            session.rollback();

            for (int i = 0; i < maxPrefetch; i++)
            {
                final var message = messageConsumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Message %d not received after rollback".formatted(i));
                assertEquals(i, message.getIntProperty(INDEX), "Unexpected message received after rollback");
            }

            session.commit();

            final var message = messageConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Message %d not received".formatted(maxPrefetch));
            assertEquals(maxPrefetch, message.getIntProperty(INDEX), "Unexpected message received");
        }
    }

    @Test
    void messageOrder(final JmsSupport jms) throws Exception
    {
        final int messageNumber = 4;
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var consumer = session.createConsumer(queue))
        {
            jms.messages().send(connection, queue, messageNumber);
            connection.start();

            int messageSeen = 0;
            int expectedIndex = 0;
            while (expectedIndex < messageNumber)
            {
                final var message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Expected message '%d' is not received".formatted(expectedIndex));
                assertEquals(expectedIndex, message.getIntProperty(INDEX), "Received message out of order");

                //don't commit transaction for the message until we receive it 5 times
                if (messageSeen < 5)
                {
                    // receive remaining
                    for (int m = expectedIndex + 1; m < messageNumber; m++)
                    {
                        Message remaining = consumer.receive(Timeouts.receiveMillis());
                        assertNotNull(message, "Expected remaining message '%d' is not received".formatted(m));
                        assertEquals(m, remaining.getIntProperty(INDEX), "Received remaining message out of order");
                    }

                    LOGGER.debug("Rolling back transaction for message with index {}", expectedIndex);
                    session.rollback();
                    messageSeen++;
                }
                else
                {
                    LOGGER.debug("Committing transaction for message with index {}", expectedIndex);
                    messageSeen = 0;
                    expectedIndex++;
                    session.commit();
                }
            }
        }
    }

    @Test
    void commitOnClosedConnection(final JmsSupport jms) throws Exception
    {
        Session transactedSession;

        try (final var connection = jms.builder().connection().create())
        {
            transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        }

        assertNotNull(transactedSession, "Session cannot be null");

        assertThrows(IllegalStateException.class, transactedSession::commit,
                "Commit on closed connection should throw IllegalStateException!");
    }

    @Test
    void commitOnClosedSession(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED))
        {
            transactedSession.close();
            assertThrows(IllegalStateException.class, transactedSession::commit,
                    "Commit on closed session should throw IllegalStateException!");
        }
    }

    @Test
    void rollbackOnClosedSession(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED))
        {
            transactedSession.close();
            assertThrows(IllegalStateException.class, transactedSession::rollback,
                    "Rollback on closed session should throw IllegalStateException!");
        }
    }

    @Test
    void getTransactedOnClosedSession(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED))
        {
            transactedSession.close();
            assertThrows(IllegalStateException.class,
                    transactedSession::getTransacted,
                    "According to Sun TCK invocation of Session#getTransacted on closed session should throw IllegalStateException!");
        }
    }
}
