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

package org.apache.qpid.systests.jms_3_1.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.Topic;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("message")
@Tag("queue")
@Tag("topic")
class JMSDestinationTest
{
    @Test
    void messageSentToQueueComesBackWithTheSameJMSDestination(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = session.createConsumer(queue))
        {
            jms.messages().send(session, queue, 1);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "Message should not be null");

            final var receivedDestination = receivedMessage.getJMSDestination();
            assertNotNull(receivedDestination, "JMSDestination should not be null");
            assertInstanceOf(Queue.class, receivedDestination, "Unexpected destination type");
            assertEquals(queue.getQueueName(), ((Queue) receivedDestination).getQueueName(),
                    "Unexpected destination name");
        }
    }

    @Test
    void messageSentToTopicComesBackWithTheSameJMSDestination(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = session.createConsumer(topic))
        {
            jms.messages().send(session, topic, 1);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "Message should not be null");

            final var receivedDestination = receivedMessage.getJMSDestination();
            assertNotNull(receivedDestination, "JMSDestination should not be null");
            assertInstanceOf(Topic.class, receivedDestination, "Unexpected destination type");
            assertEquals(topic.getTopicName(), ((Topic) receivedDestination).getTopicName(),
                    "Unexpected destination name");
        }
    }

    @Test
    void messageSentToQueueComesBackWithTheSameJMSDestinationWhenReceivedAsynchronously(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = session.createConsumer(queue);)
        {
            jms.messages().send(session, queue, 1);

            connection.start();

            final var receiveLatch = new CountDownLatch(1);
            final AtomicReference<Message> messageHolder = new AtomicReference<>();
            consumer.setMessageListener(message ->
            {
                messageHolder.set(message);
                receiveLatch.countDown();
            });
            assertTrue(receiveLatch.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "Timed out waiting for message to be received ");
            assertNotNull(messageHolder.get(), "Message should not be null");

            final var receivedDestination = messageHolder.get().getJMSDestination();
            assertNotNull(receivedDestination, "JMSDestination should not be null");
            assertInstanceOf(Queue.class, receivedDestination, "Unexpected destination type");
            assertEquals(queue.getQueueName(), ((Queue) receivedDestination).getQueueName(),
                    "Unexpected destination name");
        }
    }

    @Test
    void receiveResend(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = session.createConsumer(queue))
        {
            jms.messages().send(session, queue, 1);

            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "Message should not be null");

            final var receivedDestination = receivedMessage.getJMSDestination();

            assertNotNull(receivedDestination, "JMSDestination should not be null");
            assertInstanceOf(Queue.class, receivedDestination, "Unexpected destination type");
            assertEquals(queue.getQueueName(), ((Queue) receivedDestination).getQueueName(),
                    "Unexpected destination name");

            final var producer = session.createProducer(queue);
            producer.send(receivedMessage);

            final var message = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Message should not be null");

            final var destination = message.getJMSDestination();

            assertNotNull(destination, "JMSDestination should not be null");
            assertInstanceOf(Queue.class, destination, "Unexpected destination type");
            assertEquals(queue.getQueueName(), ((Queue) destination).getQueueName(),
                    "Unexpected destination name");
        }
    }
}
