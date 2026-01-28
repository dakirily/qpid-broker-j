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
import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static javax.jms.Session.SESSION_TRANSACTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

@ConfigItem(name = "qpid.broker.rocksdb.committerNotifyThreshold", value = "1000")
@ConfigItem(name = "qpid.broker.rocksdb.committerWaitTimeoutMs", value = "60000")
public class RocksDBAsyncCommitRecoveryTest extends JmsTestBase
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
    public void autoCommitEnqueueSurvivesImmediateRestart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        List<String> sent = sendAutoCommitMessages(queue, 3);

        restartBroker();

        List<String> received = receiveCommittedMessages(queue, sent.size());
        assertEquals(new HashSet<>(sent), new HashSet<>(received), "Unexpected message content after restart");
    }

    @Test
    public void autoCommitAcksSurviveImmediateRestart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        sendCommittedMessages(queue, 2);

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            for (int i = 0; i < 2; i++)
            {
                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull(message, "Expected message " + i);
            }
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
            Session session = receivingConnection.createSession(false, AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receiveNoWait();
            assertNull(message, "Unexpected message after restart");
        }
        finally
        {
            receivingConnection.close();
        }
    }

    private List<String> sendAutoCommitMessages(final Queue queue, final int count) throws Exception
    {
        List<String> payloads = new ArrayList<>();
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            for (int i = 0; i < count; i++)
            {
                String payload = UUID.randomUUID().toString();
                TextMessage message = session.createTextMessage(payload);
                producer.send(message, PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
                payloads.add(payload);
            }
        }
        finally
        {
            connection.close();
        }
        return payloads;
    }

    private void sendCommittedMessages(final Queue queue, final int count) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
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

    private List<String> receiveCommittedMessages(final Queue queue, final int count) throws Exception
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

    private void restartBroker() throws Exception
    {
        getBrokerAdmin().restart().get(60, TimeUnit.SECONDS);
    }
}
