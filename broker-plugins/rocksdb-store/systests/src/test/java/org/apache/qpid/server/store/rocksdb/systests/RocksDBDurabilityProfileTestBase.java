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
import static javax.jms.Session.SESSION_TRANSACTED;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;

public abstract class RocksDBDurabilityProfileTestBase extends JmsTestBase
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
    public void committedMessageSurvivesRestart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        String payload = UUID.randomUUID().toString();

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(payload), PERSISTENT, Message.DEFAULT_PRIORITY,
                          Message.DEFAULT_TIME_TO_LIVE);
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
            session.commit();
        }
        finally
        {
            receivingConnection.close();
        }
    }

    @Test
    public void uncommittedMessageDoesNotSurviveRestart() throws Exception
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

    protected void restartBroker() throws Exception
    {
        getBrokerAdmin().restart().get(60, TimeUnit.SECONDS);
    }
}
