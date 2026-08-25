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
package org.apache.qpid.server.embedded;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.test.utils.UnitTestBase;

public class EmbeddedQpidBrokerExtensionTest extends UnitTestBase
{
    @RegisterExtension
    public static final EmbeddedQpidBrokerExtension BROKER = EmbeddedQpidBrokerExtension.builder()
            .virtualHost("extension-test")
            .credentials("test-user", "test-password")
            .exchange("events", ExchangeType.FANOUT)
            .queue("orders")
            .binding("events", "orders", "")
            .build();

    @Test
    public void testJmsSendAndReceive(final EmbeddedQpidBroker injectedBroker) throws Exception
    {
        assertSame(BROKER.getBroker(), injectedBroker);
        assertTrue(injectedBroker.isRunning());

        final JmsConnectionFactory connectionFactory = new JmsConnectionFactory(BROKER.getAmqpUri().toString());
        Connection connection = null;
        try
        {
            connection = connectionFactory.createConnection(BROKER.getUsername(), BROKER.getPassword());
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(session.createQueue("orders"));
            final MessageProducer producer = session.createProducer(session.createTopic("events"));
            connection.start();

            producer.send(session.createTextMessage("embedded-message"));
            final Message message = consumer.receive(5000L);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            assertEquals("embedded-message", ((TextMessage) message).getText());
        }
        finally
        {
            if (connection != null)
            {
                connection.close();
            }
        }
    }
}
