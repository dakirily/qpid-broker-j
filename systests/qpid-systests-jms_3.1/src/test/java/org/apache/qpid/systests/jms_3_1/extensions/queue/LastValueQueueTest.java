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

import javax.naming.NamingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
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
class LastValueQueueTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LastValueQueueTest.class);

    private static final String MESSAGE_SEQUENCE_NUMBER_PROPERTY = "msg";
    private static final String KEY_PROPERTY = "key";
    private static final int MSG_COUNT = 400;
    private static final int NUMBER_OF_UNIQUE_KEY_VALUES = 10;

    @Test
    void conflation(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().lvqKey(KEY_PROPERTY).create();
        try (final var producerConnection = jms.builder().connection().create();
             final var producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = producerSession.createProducer(queue))
        {
            final var message = producerSession.createMessage();

            message.setStringProperty(KEY_PROPERTY, "A");
            message.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, 1);
            producer.send(message);

            message.setStringProperty(KEY_PROPERTY, "B");
            message.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, 2);
            producer.send(message);

            message.setStringProperty(KEY_PROPERTY, "A");
            message.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, 3);
            producer.send(message);

            message.setStringProperty(KEY_PROPERTY, "B");
            message.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, 4);
            producer.send(message);
        }

        try (final var consumerConnection = jms.builder().connection().create();
             final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = consumerSession.createConsumer(queue))
        {
            consumerConnection.start();

            final var received1 = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received1, "First message is not received");
            assertEquals("A", received1.getStringProperty(KEY_PROPERTY), "Unexpected key property value");
            assertEquals(3, received1.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY),
                    "Unexpected sequence property value");

            final var received2 = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received2, "Second message is not received");
            assertEquals("B", received2.getStringProperty(KEY_PROPERTY), "Unexpected key property value");
            assertEquals(4, received2.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY),
                    "Unexpected sequence property value");

            assertNull(consumer.receive(Timeouts.noMessagesMillis()), "Unexpected message is received");
        }
    }

    @Test
    void conflationWithRelease(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().lvqKey(KEY_PROPERTY).create();

        sendMessages(jms, queue, 0, MSG_COUNT / 2);

        try (final var consumerConnection = jms.builder().connection().create();
             final var consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
             final var consumer = consumerSession.createConsumer(queue))
        {
            consumerConnection.start();

            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final var received = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(received, "Message with key %d is not received".formatted(i));
                assertEquals(MSG_COUNT / 2 - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                        received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY),
                        "Unexpected message received");
            }
        }

        sendMessages(jms, queue, MSG_COUNT / 2, MSG_COUNT);

        try (final var consumerConnection = jms.builder().connection().create();
             final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = consumerSession.createConsumer(queue))
        {
            consumerConnection.start();

            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final var received = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(received, "Message with key %d is not received".formatted(i));
                assertEquals(MSG_COUNT - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                        received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY),
                        "Unexpected message received");
            }
        }
    }

    @Test
    void conflationWithReleaseAfterNewPublish(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().lvqKey(KEY_PROPERTY).create();

        sendMessages(jms, queue, 0, MSG_COUNT / 2);

        try (final var consumerConnection = jms.builder().connection().create();
             final var consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
             final var consumer = consumerSession.createConsumer(queue))
        {
            consumerConnection.start();

            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final var received = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(received, "Message with key %d is not received".formatted(i));
                assertEquals(MSG_COUNT / 2 - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                        received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY),
                        "Unexpected message received");
            }

            sendMessages(jms, queue, MSG_COUNT / 2, MSG_COUNT);
        }

        try (final var consumerConnection = jms.builder().connection().create())
        {
            consumerConnection.start();

            final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer = consumerSession.createConsumer(queue);

            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final var received = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(received, "Message with key %d is not received".formatted(i));
                assertEquals(MSG_COUNT - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                        received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY),
                        "Unexpected message received");
            }
        }
    }

    @Test
    void conflatedQueueDepth(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().lvqKey(KEY_PROPERTY).create();

        sendMessages(jms, queue, 0, MSG_COUNT);

        assertEquals(NUMBER_OF_UNIQUE_KEY_VALUES, jms.virtualhost().totalDepthOfQueuesMessages());
    }

    @Test
    void conflationBrowser(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().lvqKey(KEY_PROPERTY).ensureNondestructiveConsumers(true).create();

        sendMessages(jms, queue, 0, MSG_COUNT);

        try (final var consumerConnection = jms.builder().connection().create();
             final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = consumerSession.createConsumer(queue))
        {
            consumerConnection.start();
            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final var received = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(received, "Message with key %d is not received".formatted(i));
                assertEquals(MSG_COUNT - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                        received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY),
                        "Unexpected message received");
            }

            sendMessages(jms, queue, MSG_COUNT, MSG_COUNT + 1);

            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message with key %d is not received".formatted(0));
            assertEquals(MSG_COUNT, received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY),
                    "Unexpected message received");
        }
    }

    @Test
    void conflation2Browsers(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().lvqKey(KEY_PROPERTY).ensureNondestructiveConsumers(true).create();

        sendMessages(jms, queue, 0, MSG_COUNT);

        try (final var consumerConnection = jms.builder().connection().create();
             final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = consumerSession.createConsumer(queue);
             final var consumer2 = consumerSession.createConsumer(queue))
        {
            consumerConnection.start();

            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final var received = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(received, "Message with key %d is not received by first consumer".formatted(i));
                assertEquals(MSG_COUNT - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                        received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY),
                        "Unexpected message received by first consumer");

                final var received2 = consumer2.receive(Timeouts.receiveMillis());
                assertNotNull(received2, "Message with key %d is not received by second consumer".formatted(i));
                assertEquals(MSG_COUNT - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                        received2.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY),
                        "Unexpected message received by second consumer");
            }
        }
    }

    @Test
    void parallelProductionAndConsumption(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().lvqKey(KEY_PROPERTY).create();

        int numberOfUniqueKeyValues = 2;
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        try
        {
            // Start producing threads that send messages
            final var messageProducer1 = new BackgroundMessageProducer(jms, queue, numberOfUniqueKeyValues);
            final var messageProducer2 = new BackgroundMessageProducer(jms, queue, numberOfUniqueKeyValues);

            final Future<?> future1 = executorService.submit(messageProducer1);
            final Future<?> future2 = executorService.submit(messageProducer2);

            final Map<String, Integer> lastReceivedMessages = receiveMessages(jms, messageProducer1, queue);

            future1.get(Timeouts.receiveMillis() * MSG_COUNT, TimeUnit.MILLISECONDS);
            future2.get(Timeouts.receiveMillis() * MSG_COUNT, TimeUnit.MILLISECONDS);

            final Map<String, Integer> lastSentMessages1 = messageProducer1.getMessageSequenceNumbersByKey();
            assertEquals(numberOfUniqueKeyValues, lastSentMessages1.size(),
                    "Unexpected number of last sent messages sent by producer1");
            final Map<String, Integer> lastSentMessages2 = messageProducer2.getMessageSequenceNumbersByKey();
            assertEquals(lastSentMessages1, lastSentMessages2);

            assertEquals(lastSentMessages1, lastReceivedMessages,
                    "The last message sent for each key should match the last message received for that key");

            assertNull(messageProducer1.getException(),
                    "Unexpected exception from background producer thread");
        }
        finally
        {
            executorService.shutdown();
        }
    }

    private Map<String, Integer> receiveMessages(final JmsSupport jms, BackgroundMessageProducer producer, final Queue queue) throws Exception
    {
        producer.waitUntilQuarterOfMessagesSentToEncourageConflation();

        final Map<String, Integer> messageSequenceNumbersByKey = new HashMap<>();

        try (final var consumerConnection = jms.builder().connection().prefetch(1).create();
             final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            LOGGER.info("Starting to receive");

            final var consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            Message message;
            int numberOfShutdownsReceived = 0;
            int numberOfMessagesReceived = 0;
            while (numberOfShutdownsReceived < 2)
            {
                message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "null received after " + numberOfMessagesReceived + " messages and " +
                        numberOfShutdownsReceived + " shutdowns");

                if (message.propertyExists(BackgroundMessageProducer.SHUTDOWN))
                {
                    numberOfShutdownsReceived++;
                }
                else
                {
                    numberOfMessagesReceived++;
                    putMessageInMap(message, messageSequenceNumbersByKey);
                }
            }

            LOGGER.info("Finished receiving.  Received {} message(s) in total", numberOfMessagesReceived);
        }
        return messageSequenceNumbersByKey;
    }

    private void putMessageInMap(Message message, Map<String, Integer> messageSequenceNumbersByKey) throws JMSException
    {
        String keyValue = message.getStringProperty(KEY_PROPERTY);
        Integer messageSequenceNumber = message.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY);
        messageSequenceNumbersByKey.put(keyValue, messageSequenceNumber);
    }

    private final class BackgroundMessageProducer implements Runnable
    {
        static final String SHUTDOWN = "SHUTDOWN";

        private final JmsSupport _jms;
        private final Queue _queue;
        private final Map<String, Integer> _messageSequenceNumbersByKey = new HashMap<>();
        private final CountDownLatch _quarterOfMessagesSentLatch = new CountDownLatch(MSG_COUNT / 4);
        private final int _numberOfUniqueKeyValues;

        private volatile Exception _exception;

        BackgroundMessageProducer(final JmsSupport jms, Queue queue, final int numberOfUniqueKeyValues)
        {
            _jms = jms;
            _queue = queue;
            _numberOfUniqueKeyValues = numberOfUniqueKeyValues;
        }

        void waitUntilQuarterOfMessagesSentToEncourageConflation() throws InterruptedException
        {
            final long latchTimeout = 60000;
            boolean success = _quarterOfMessagesSentLatch.await(latchTimeout, TimeUnit.MILLISECONDS);
            assertTrue(success,"Failed to be notified that 1/4 of the messages have been sent within " +
                    latchTimeout + " ms.");
            LOGGER.info("Quarter of messages sent");
        }

        public Exception getException()
        {
            return _exception;
        }

        Map<String, Integer> getMessageSequenceNumbersByKey()
        {
            return Collections.unmodifiableMap(_messageSequenceNumbersByKey);
        }

        @Override
        public void run()
        {
            try
            {
                LOGGER.info("Starting to send in background thread");
                try (final var producerConnection = _jms.builder().connection().create();
                     final var producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
                     final var backgroundProducer = producerSession.createProducer(_queue))
                {
                    for (int messageNumber = 0; messageNumber < MSG_COUNT; messageNumber++)
                    {

                        final var message = nextMessage(messageNumber, producerSession, _numberOfUniqueKeyValues);
                        backgroundProducer.send(message);
                        producerSession.commit();

                        putMessageInMap(message, _messageSequenceNumbersByKey);
                        _quarterOfMessagesSentLatch.countDown();
                    }

                    final var shutdownMessage = producerSession.createMessage();
                    shutdownMessage.setBooleanProperty(SHUTDOWN, true);
                    // make sure the shutdown messages have distinct keys because the Qpid Cpp Broker will
                    // otherwise consider them to have the same key.
                    shutdownMessage.setStringProperty(KEY_PROPERTY, Thread.currentThread().getName());

                    backgroundProducer.send(shutdownMessage);
                    producerSession.commit();
                }

                LOGGER.info("Finished sending in background thread");
            }
            catch (Exception e)
            {
                _exception = e;
                LOGGER.warn("Unexpected exception in publisher", e);
            }
        }
    }

    private Message nextMessage(int msg, Session producerSession) throws JMSException
    {
        return nextMessage(msg, producerSession, NUMBER_OF_UNIQUE_KEY_VALUES);
    }

    private Message nextMessage(int msg, Session producerSession, int numberOfUniqueKeyValues) throws JMSException
    {
        final var send = producerSession.createTextMessage("Message: " + msg);
        final String keyValue = String.valueOf(msg % numberOfUniqueKeyValues);
        send.setStringProperty(KEY_PROPERTY, keyValue);
        send.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, msg);
        return send;
    }

    private void sendMessages(final JmsSupport jms, final Queue queue, final int fromIndex, final int toIndex)
            throws JMSException, NamingException
    {
        try (final var producerConnection = jms.builder().connection().create();
             final var producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
             final var producer = producerSession.createProducer(queue))
        {
            for (int msg = fromIndex; msg < toIndex; msg++)
            {
                producer.send(nextMessage(msg, producerSession));
                producerSession.commit();
            }
        }
    }
}
