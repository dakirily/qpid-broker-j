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

package org.apache.qpid.server.store.rocksdb.systests;

import static javax.jms.DeliveryMode.PERSISTENT;
import static javax.jms.Session.CLIENT_ACKNOWLEDGE;
import static javax.jms.Session.SESSION_TRANSACTED;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public class RocksDBStorePersistenceTest extends JmsTestBase
{
    @BeforeAll
    public static void verifyStoreType()
    {
        String type = System.getProperty("virtualhostnode.type", "");
        assumeTrue("ROCKSDB".equalsIgnoreCase(type), "Tests require virtualhostnode.type=ROCKSDB");
    }

    @BeforeEach
    public void ensurePersistentStore()
    {
        assumeTrue(getBrokerAdmin().supportsRestart(), "Tests require persistent store");
    }

    @Test
    public void committedPersistentMessagesSurviveRestart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        List<String> sent = sendTextMessages(queue, 3);

        restartBroker();

        List<String> received = receiveTextMessages(queue, sent.size());
        assertEquals(new HashSet<>(sent), new HashSet<>(received), "Unexpected message content after restart");
    }

    @Test
    public void uncommittedMessagesDoNotSurviveRestart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("uncommitted"), PERSISTENT, Message.DEFAULT_PRIORITY,
                          Message.DEFAULT_TIME_TO_LIVE);
            // do not commit
        }
        finally
        {
            connection.close();
        }

        restartBroker();

        Connection receivingConnection = getConnection();
        try
        {
            receivingConnection.start();
            Session session = receivingConnection.createSession(true, SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receiveNoWait();
            assertNull(message, "Unexpected message after restart");
        }
        finally
        {
            receivingConnection.close();
        }
    }

    @Test
    public void largeMessageSurvivesRestart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        byte[] payload = new byte[256 * 1024];
        for (int i = 0; i < payload.length; i++)
        {
            payload[i] = (byte) (i & 0xff);
        }

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(payload);
            producer.send(message, PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            session.commit();
        }
        finally
        {
            connection.close();
        }

        restartBroker();

        Connection receivingConnection = getConnection();
        try
        {
            receivingConnection.start();
            Session session = receivingConnection.createSession(true, SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(getReceiveTimeout());
            assertNotNull(message, "Expected message after restart");
            BytesMessage bytesMessage = (BytesMessage) message;
            byte[] received = new byte[(int) bytesMessage.getBodyLength()];
            bytesMessage.readBytes(received);
            session.commit();
            assertArrayEquals(payload, received, "Payload mismatch after restart");
        }
        finally
        {
            receivingConnection.close();
        }
    }

    @Test
    public void committedDequeuesSurviveRestart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        sendTextMessages(queue, 3);

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(true, SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(getReceiveTimeout());
            assertNotNull(message, "Expected message to dequeue");
            session.commit();
        }
        finally
        {
            connection.close();
        }

        restartBroker();

        List<String> remaining = receiveTextMessages(queue, 2);
        assertEquals(2, remaining.size(), "Unexpected remaining message count after restart");
    }

    @Test
    public void uncommittedAcksAreRedeliveredAfterRestart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        sendTextMessages(queue, 2);

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(true, SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(getReceiveTimeout());
            assertNotNull(message, "Expected message to peek");
            // do not commit
        }
        finally
        {
            connection.close();
        }

        restartBroker();

        List<String> remaining = receiveTextMessages(queue, 2);
        assertEquals(2, remaining.size(), "Unexpected remaining message count after restart");
    }

    @Test
    public void queueDepthPersistsAcrossRestart() throws Exception
    {
        assumeTrue(getBrokerAdmin().isQueueDepthSupported(), "Queue depth stats not supported");
        String queueName = getTestName();
        Queue queue = createQueue(queueName);
        sendTextMessages(queue, 2);

        int depthBefore = getBrokerAdmin().getQueueDepthMessages(queueName);
        assertEquals(2, depthBefore, "Unexpected queue depth before restart");

        restartBroker();

        int depthAfter = getBrokerAdmin().getQueueDepthMessages(queueName);
        assertEquals(2, depthAfter, "Unexpected queue depth after restart");
    }

    @Test
    public void multipleQueuesPersistAcrossRestart() throws Exception
    {
        Queue queueA = createQueue(getTestName() + "_A");
        Queue queueB = createQueue(getTestName() + "_B");

        sendTextMessages(queueA, 2);
        sendTextMessages(queueB, 3);

        restartBroker();

        assertEquals(2, receiveTextMessages(queueA, 2).size(), "Unexpected count for queue A after restart");
        assertEquals(3, receiveTextMessages(queueB, 3).size(), "Unexpected count for queue B after restart");
    }

    @Test
    public void durableSubscriptionSurvivesRestart() throws Exception
    {
        String topicName = getTestName() + "_topic";
        Topic topic = createTopic(topicName);
        String clientId = "cid-" + UUID.randomUUID();
        String subscriptionName = "sub-" + UUID.randomUUID();

        Connection subConnection = getConnectionBuilder().setClientId(clientId).build();
        try
        {
            subConnection.start();
            Session subSession = subConnection.createSession(true, SESSION_TRANSACTED);
            MessageConsumer subscriber = subSession.createDurableSubscriber(topic, subscriptionName);
            subscriber.close();
            subSession.commit();
        }
        finally
        {
            subConnection.close();
        }

        sendTextMessagesToTopic(topic, 3);

        restartBroker();

        Connection receiveConnection = getConnectionBuilder().setClientId(clientId).build();
        try
        {
            receiveConnection.start();
            Session receiveSession = receiveConnection.createSession(true, SESSION_TRANSACTED);
            MessageConsumer durableSubscriber = receiveSession.createDurableSubscriber(topic, subscriptionName);
            for (int i = 0; i < 3; i++)
            {
                Message message = durableSubscriber.receive(getReceiveTimeout());
                assertNotNull(message, "Expected durable subscription message " + i);
            }
            receiveSession.commit();
            durableSubscriber.close();
            receiveSession.unsubscribe(subscriptionName);
        }
        finally
        {
            receiveConnection.close();
        }
    }

    private List<String> sendTextMessages(final Queue queue, final int count) throws Exception
    {
        List<String> payloads = new ArrayList<>();
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
            for (int i = 0; i < count; i++)
            {
                String payload = UUID.randomUUID().toString();
                TextMessage message = session.createTextMessage(payload);
                producer.send(message, PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                payloads.add(payload);
            }
            session.commit();
        }
        finally
        {
            connection.close();
        }
        return payloads;
    }

    private List<String> receiveTextMessages(final Queue queue, final int count) throws Exception
    {
        List<String> payloads = new ArrayList<>();
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(true, SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            for (int i = 0; i < count; i++)
            {
                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull(message, "Expected message " + i);
                payloads.add(((TextMessage) message).getText());
            }
            session.commit();
        }
        finally
        {
            connection.close();
        }
        return payloads;
    }

    private void sendTextMessagesToTopic(final Topic topic, final int count) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(topic);
            for (int i = 0; i < count; i++)
            {
                TextMessage message = session.createTextMessage(UUID.randomUUID().toString());
                producer.send(message, PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            }
            session.commit();
        }
        finally
        {
            connection.close();
        }
    }

    private void restartBroker() throws Exception
    {
        getBrokerAdmin().restart().get(60, TimeUnit.SECONDS);
    }
}
