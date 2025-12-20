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

package org.apache.qpid.systests.jms_3_1.extensions.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("policy")
@Tag("queue")
class PriorityQueueTest
{
    private static final int MSG_COUNT = 50;

    @Test
    void priority(final JmsSupport jms) throws Exception
    {
        final int priorities = 10;
        final var queue = jms.builder().queue().priorities(priorities).create();
        try (final var producerConnection = jms.builder().connection().create();
             final var producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
             final var producer = producerSession.createProducer(queue))
        {
            for (int msg = 0; msg < MSG_COUNT; msg++)
            {
                producer.setPriority(msg % priorities);
                producer.send(nextMessage(producerSession, msg));
            }
            producerSession.commit();
        }

        try (final var consumerConnection = jms.builder().connection().create();
             final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = consumerSession.createConsumer(queue))
        {
            consumerConnection.start();
            Message previous = null;
            for (int messageCount = 0, expectedPriority = priorities - 1; messageCount < MSG_COUNT; messageCount++)
            {
                final var received = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(received, "Message '%d' is not received".formatted(messageCount));
                assertEquals(expectedPriority, received.getJMSPriority(),
                        "Unexpected message '%d' priority".formatted(messageCount));
                if (previous != null)
                {
                    assertTrue(previous.getJMSPriority() > received.getJMSPriority() ||
                            (previous.getJMSPriority() == received.getJMSPriority() &&
                                    previous.getIntProperty("msg") < received.getIntProperty("msg")),
                            "Messages '%d' arrived in unexpected order : previous message '%d' priority is '%d', received message '%d' priority is '%d'"
                                    .formatted(messageCount, previous.getIntProperty("msg"), previous.getJMSPriority(),
                                    received.getIntProperty("msg"), received.getJMSPriority()));
                }
                previous = received;
                if (messageCount > 0 && (messageCount + 1) % (MSG_COUNT / priorities) == 0)
                {
                    expectedPriority--;
                }
            }
        }
    }
    
    @Test
    void oddOrdering(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().priorities(3).create();
        try (final var producerConnection = jms.builder().connection().create();
             final var producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
             final var producer = producerSession.createProducer(queue))
        {
            // In order ABC
            producer.setPriority(9);
            producer.send(nextMessage(producerSession, 1));
            producer.setPriority(4);
            producer.send(nextMessage(producerSession, 2));
            producer.setPriority(1);
            producer.send(nextMessage(producerSession, 3));

            // Out of order BAC
            producer.setPriority(4);
            producer.send(nextMessage(producerSession, 4));
            producer.setPriority(9);
            producer.send(nextMessage(producerSession, 5));
            producer.setPriority(1);
            producer.send(nextMessage(producerSession, 6));

            // Out of order BCA
            producer.setPriority(4);
            producer.send(nextMessage(producerSession, 7));
            producer.setPriority(1);
            producer.send(nextMessage(producerSession, 8));
            producer.setPriority(9);
            producer.send(nextMessage(producerSession, 9));

            // Reverse order CBA
            producer.setPriority(1);
            producer.send(nextMessage(producerSession, 10));
            producer.setPriority(4);
            producer.send(nextMessage(producerSession, 11));
            producer.setPriority(9);
            producer.send(nextMessage(producerSession, 12));
            producerSession.commit();
        }

        try (final var consumerConnection = jms.builder().connection().create();
             final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = consumerSession.createConsumer(queue))
        {
            consumerConnection.start();

            var msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(1, msg.getIntProperty("msg"));
            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(5, msg.getIntProperty("msg"));
            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(9, msg.getIntProperty("msg"));
            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(12, msg.getIntProperty("msg"));

            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(2, msg.getIntProperty("msg"));
            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(4, msg.getIntProperty("msg"));
            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(7, msg.getIntProperty("msg"));
            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(11, msg.getIntProperty("msg"));

            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(3, msg.getIntProperty("msg"));
            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(6, msg.getIntProperty("msg"));
            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(8, msg.getIntProperty("msg"));
            msg = consumer.receive(Timeouts.receiveMillis());
            assertEquals(10, msg.getIntProperty("msg"));
        }
    }

    /**
     * Test that after sending an initial  message with priority 0, it is able to be repeatedly reflected back to the queue using
     * default priority and then consumed again, with separate transacted sessions with prefetch 1 for producer and consumer.
     * <br>
     * Highlighted defect with PriorityQueues resolved in QPID-3927.
     */
    @Test
    void messageReflectionWithPriorityIncreaseOnTransactedSessionsWithPrefetch1(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().priorities(10).create();
        try (final var connection = jms.builder().connection().prefetch(1).create())
        {
            connection.start();
            final var producerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);

            //create the consumer, producer, add message listener
            final CountDownLatch latch = new CountDownLatch(5);
            final var consumer = producerSession.createConsumer(queue);
            final var producer = producerSession.createProducer(queue);

            final var listener = new ReflectingMessageListener(producerSession, producer, consumerSession, latch);
            consumer.setMessageListener(listener);

            //Send low priority 0 message to kick start the asynchronous reflection process
            producer.setPriority(0);
            producer.send(nextMessage(producerSession, 1));
            producerSession.commit();

            //wait for the reflection process to complete
            assertTrue(latch.await(10, TimeUnit.SECONDS), "Test process failed to complete in allowed time");
            assertNull(listener.getThrown(), "Unexpected throwable encountered");
        }
    }

    private Message nextMessage(final Session producerSession, final int msg) throws JMSException
    {
        final var message = producerSession.createTextMessage("Message: " + msg);
        message.setIntProperty("msg", msg);
        return message;
    }

    private static class ReflectingMessageListener implements MessageListener
    {
        private static final Logger LOGGER = LoggerFactory.getLogger(ReflectingMessageListener.class);

        private final Session _producerSession;
        private final Session _consumerSession;
        private final CountDownLatch _latch;
        private final MessageProducer _producer;
        private final long _origCount;
        private Throwable _lastThrown;

        ReflectingMessageListener(final Session producerSession, final MessageProducer producer,
                                  final Session consumerSession, final CountDownLatch latch)
        {
            _latch = latch;
            _origCount = _latch.getCount();
            _producerSession = producerSession;
            _consumerSession = consumerSession;
            _producer = producer;
        }

        @Override
        public void onMessage(final Message message)
        {
            try
            {
                _latch.countDown();
                long msgNum = _origCount - _latch.getCount();
                LOGGER.info("Received message {} with ID: {}", msgNum, message.getIntProperty("msg"));

                if (_latch.getCount() > 0)
                {
                    //reflect the message, updating its ID and using default priority
                    message.clearProperties();
                    message.setIntProperty("msg", (int) msgNum + 1);
                    _producer.setPriority(Message.DEFAULT_PRIORITY);
                    _producer.send(message);
                    _producerSession.commit();
                }

                //commit the consumer session to consume the message
                _consumerSession.commit();
            }
            catch (Throwable t)
            {
                LOGGER.error(t.getMessage(), t);
                _lastThrown = t;
            }
        }

        public Throwable getThrown()
        {
            return _lastThrown;
        }
    }
}
