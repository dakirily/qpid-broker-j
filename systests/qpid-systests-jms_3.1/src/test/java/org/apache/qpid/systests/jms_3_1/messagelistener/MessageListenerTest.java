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

package org.apache.qpid.systests.jms_3_1.messagelistener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("queue")
class MessageListenerTest
{
    private static final int MSG_COUNT = 10;
    private static final long RECEIVE_TIMEOUT = Timeouts.receiveMillis() * 2;

    @Test
    void messageListener(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            jms.messages().send(session, queue, MSG_COUNT);

            connection.start();
            final var consumer = session.createConsumer(queue);

            final var countingMessageListener = new CountingMessageListener(MSG_COUNT);
            consumer.setMessageListener(countingMessageListener);

            final var completed = countingMessageListener.awaitMessages(RECEIVE_TIMEOUT);
            assertTrue(completed, () -> "Timed out waiting for messages; outstanding = " + countingMessageListener.getOutstandingCount());
            assertEquals(0, countingMessageListener.getOutstandingCount(),
                    "Unexpected number of outstanding messages");
        }
    }

    @Test
    void synchronousReceiveFollowedByMessageListener(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            jms.messages().send(session, queue, MSG_COUNT);

            connection.start();
            final var consumer = session.createConsumer(queue);
            assertNotNull(consumer.receive(RECEIVE_TIMEOUT),
                    "Could not receive first message synchronously");

            final var countingMessageListener = new CountingMessageListener(MSG_COUNT - 1);
            consumer.setMessageListener(countingMessageListener);

            final var completed = countingMessageListener.awaitMessages(RECEIVE_TIMEOUT);
            assertTrue(completed, () -> "Timed out waiting for listener delivery; outstanding = " + countingMessageListener.getOutstandingCount());
            assertEquals(0, countingMessageListener.getOutstandingCount(),
                    "Unexpected number of outstanding messages");
        }
    }

    @Test
    void connectionStopThenStart(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            jms.messages().send(session, queue, MSG_COUNT);

            connection.start();

            final var consumer = session.createConsumer(queue);
            final int messageToReceivedBeforeConnectionStop = MSG_COUNT / 2;
            final var countingMessageListener = new CountingMessageListener(MSG_COUNT, messageToReceivedBeforeConnectionStop);
            consumer.setMessageListener(countingMessageListener);

            final var halfReceived = countingMessageListener.awaitMessages(RECEIVE_TIMEOUT);
            assertTrue(halfReceived, () -> "Timed out waiting for initial delivery; received = " + countingMessageListener.getReceivedCount());
            connection.stop();
            assertTrue(countingMessageListener.getReceivedCount() >= messageToReceivedBeforeConnectionStop,
                    "Too few messages received after Connection#stop()");

            countingMessageListener.resetLatch();
            connection.start();

            final var altReceived = countingMessageListener.awaitMessages(RECEIVE_TIMEOUT);
            assertTrue(altReceived, () -> "Timed out waiting for delivery after restart; outstanding = " + countingMessageListener.getOutstandingCount());
            assertEquals(0, countingMessageListener.getOutstandingCount(),
                    "Unexpected number of outstanding messages");
        }
    }

    @Test
    void connectionStopAndMessageListenerChange(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            jms.messages().send(session, queue, MSG_COUNT);

            connection.start();

            final var consumer = session.createConsumer(queue);
            final int messageToReceivedBeforeConnectionStop = MSG_COUNT / 2;
            final var countingMessageListener1 = new CountingMessageListener(MSG_COUNT, messageToReceivedBeforeConnectionStop);
            consumer.setMessageListener(countingMessageListener1);

            final var halfReceived = countingMessageListener1.awaitMessages(RECEIVE_TIMEOUT);
            assertTrue(halfReceived, () -> "Timed out waiting for initial delivery; received = " + countingMessageListener1.getReceivedCount());
            connection.stop();
            assertTrue(countingMessageListener1.getReceivedCount() >= messageToReceivedBeforeConnectionStop,
                    "Too few messages received after Connection#stop()");

            final var countingMessageListener2 = new CountingMessageListener(countingMessageListener1.getOutstandingCount());

            consumer.setMessageListener(countingMessageListener2);
            connection.start();

            final var altReceived = countingMessageListener2.awaitMessages(RECEIVE_TIMEOUT);
            assertTrue(altReceived, () -> "Timed out waiting for delivery after restart; outstanding = " + countingMessageListener2.getOutstandingCount());
            assertEquals(0, countingMessageListener2.getOutstandingCount(),
                    "Unexpected number of outstanding messages");
        }
    }

    @Test
    void connectionStopHaltsDeliveryToListener(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            jms.messages().send(session, queue, MSG_COUNT);

            connection.start();

            final var consumer = session.createConsumer(queue);
            final int messageToReceivedBeforeConnectionStop = MSG_COUNT / 2;
            final var countingMessageListener = new CountingMessageListener(MSG_COUNT, messageToReceivedBeforeConnectionStop);
            consumer.setMessageListener(countingMessageListener);

            final var halfReceived = countingMessageListener.awaitMessages(RECEIVE_TIMEOUT);
            assertTrue(halfReceived, () -> "Timed out waiting for initial delivery; received = " + countingMessageListener.getReceivedCount());
            connection.stop();

            final int outstandingCountAtStop = countingMessageListener.getOutstandingCount();
            countingMessageListener.resetLatch();
            final var completedWhileStopped = countingMessageListener.awaitMessages(RECEIVE_TIMEOUT);
            assertFalse(completedWhileStopped, "Messages unexpectedly delivered after connection stop");
            assertEquals(outstandingCountAtStop, countingMessageListener.getOutstandingCount(),
                    "Unexpected number of outstanding messages");
        }
    }

    @Test
    void consumerCloseHaltsDeliveryToListener(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            jms.messages().send(session, queue, MSG_COUNT);

            connection.start();

            final var consumer = session.createConsumer(queue);
            final int messageToReceivedBeforeConnectionStop = MSG_COUNT / 2;
            final var countingMessageListener = new CountingMessageListener(MSG_COUNT, messageToReceivedBeforeConnectionStop);
            consumer.setMessageListener(countingMessageListener);

            final var halfReceived = countingMessageListener.awaitMessages(RECEIVE_TIMEOUT);
            assertTrue(halfReceived, () -> "Timed out waiting for initial delivery; received = " + countingMessageListener.getReceivedCount());
            consumer.close();

            final int outstandingCountAtStop = countingMessageListener.getOutstandingCount();
            countingMessageListener.resetLatch();
            final var completedAfterClose = countingMessageListener.awaitMessages(RECEIVE_TIMEOUT);
            assertFalse(completedAfterClose, "Messages unexpectedly delivered after consumer close");
            assertEquals(outstandingCountAtStop, countingMessageListener.getOutstandingCount(),
                    "Unexpected number of outstanding messages");
        }
    }

    @Test
    void twoMessageListeners(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection1 = jms.builder().connection().prefetch(0).create();
             final var session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var session2 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            jms.messages().send(session1, queue, MSG_COUNT);

            final var consumer1 = session1.createConsumer(queue);
            final var consumer2 = session2.createConsumer(queue);

            final var countingMessageListener = new CountingMessageListener(MSG_COUNT);
            consumer1.setMessageListener(countingMessageListener);
            consumer2.setMessageListener(countingMessageListener);

            connection1.start();

            final var completed = countingMessageListener.awaitMessages(RECEIVE_TIMEOUT);
            assertTrue(completed, "Timed out waiting for messages; outstanding = " + countingMessageListener.getOutstandingCount());
            assertEquals(0, countingMessageListener.getOutstandingCount(),
                    "Unexpected number of outstanding messages");
        }
    }

    @Test
    void messageListenerDisallowsSynchronousReceive(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            jms.messages().send(session, queue, MSG_COUNT);

            connection.start();

            final var consumer = session.createConsumer(queue);
            consumer.setMessageListener(message -> { });

            assertThrows(JMSException.class, consumer::receive, "Exception not thrown");
        }
    }

    private static final class CountingMessageListener implements MessageListener
    {
        private final AtomicInteger _receivedCount;
        private final AtomicInteger _outstandingMessageCount;
        private volatile CountDownLatch _awaitMessages;

        CountingMessageListener(final int totalExpectedMessageCount)
        {
            this(totalExpectedMessageCount, totalExpectedMessageCount);
        }

        CountingMessageListener(int totalExpectedMessageCount, int numberOfMessagesToAwait)
        {
            _receivedCount = new AtomicInteger(0);
            _outstandingMessageCount = new AtomicInteger(totalExpectedMessageCount);
            _awaitMessages = new CountDownLatch(numberOfMessagesToAwait);
        }

        int getOutstandingCount()
        {
            return _outstandingMessageCount.get();
        }

        int getReceivedCount()
        {
            return _receivedCount.get();
        }

        void resetLatch()
        {
            _awaitMessages = new CountDownLatch(_outstandingMessageCount.get());
        }

        @Override
        public void onMessage(Message message)
        {
            _receivedCount.incrementAndGet();
            _outstandingMessageCount.decrementAndGet();
            _awaitMessages.countDown();
        }

        boolean awaitMessages(long timeout) throws Exception
        {
            return _awaitMessages.await(timeout, TimeUnit.MILLISECONDS);
        }
    }
}
