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

package org.apache.qpid.systests.jms_3_1.extensions.routing;

import static org.apache.qpid.systests.EntityTypes.DIRECT_EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.QUEUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("routing")
class MessageRoutingTest
{
    private static final String EXCHANGE_NAME = "testExchange";
    private static final String QUEUE_NAME = "testQueue";
    private static final String ROUTING_KEY = "testRoute";

    @BeforeEach
    void beforeEach(final JmsSupport jms) throws Exception
    {
        jms.management().create(EXCHANGE_NAME, DIRECT_EXCHANGE,
                Map.of(Exchange.UNROUTABLE_MESSAGE_BEHAVIOUR, "REJECT"));
        jms.management().create(QUEUE_NAME, QUEUE, Map.of());

        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", QUEUE_NAME);
        arguments.put("bindingKey", ROUTING_KEY);
        jms.management().perform(EXCHANGE_NAME, "bind", EXCHANGE, arguments);
    }

    @Test
    void routingWithSubjectSetAsJMSMessageType(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            final var sendingDestination = session.createTopic(EXCHANGE_NAME);
            final var receivingDestination = session.createQueue(QUEUE_NAME);

            final var message = session.createTextMessage("test");
            message.setJMSType(ROUTING_KEY);

            final var messageProducer = session.createProducer(sendingDestination);
            messageProducer.send(message);

            final var messageConsumer = session.createConsumer(receivingDestination);
            final var receivedMessage = messageConsumer.receive(Timeouts.receiveMillis());

            assertNotNull(receivedMessage, "Message not received");
            assertEquals("test", message.getText());
        }
    }

    @Test
    void anonymousRelayRoutingWithSubjectSetAsJMSMessageType(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var sendingDestination = session.createTopic(EXCHANGE_NAME);
            final var receivingDestination = session.createQueue(QUEUE_NAME);

            final var message = session.createTextMessage("test");
            message.setJMSType(ROUTING_KEY);

            final var messageProducer = session.createProducer(null);
            messageProducer.send(sendingDestination, message);

            final var messageConsumer = session.createConsumer(receivingDestination);
            final var receivedMessage = messageConsumer.receive(Timeouts.receiveMillis());

            assertNotNull(receivedMessage, "Message not received");
            assertEquals("test", message.getText());
        }
    }

    @Test
    void routingWithRoutingKeySetAsJMSProperty(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var sendingDestination = session.createTopic(EXCHANGE_NAME);
            final var receivingDestination = session.createQueue(QUEUE_NAME);
            final var message = session.createTextMessage("test");
            message.setStringProperty("routing_key", ROUTING_KEY);

            final var messageProducer = session.createProducer(sendingDestination);
            messageProducer.send(message);

            final var messageConsumer = session.createConsumer(receivingDestination);
            final var receivedMessage = messageConsumer.receive(Timeouts.receiveMillis());

            assertNotNull(receivedMessage, "Message not received");
            assertEquals("test", message.getText());
        }
    }

    @Test
    void routingWithExchangeAndRoutingKeyDestination(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var sendingDestination = session.createTopic(EXCHANGE_NAME + "/" + ROUTING_KEY);
            final var receivingDestination = session.createQueue(QUEUE_NAME);
            final var message = session.createTextMessage("test");

            final var messageProducer = session.createProducer(sendingDestination);
            messageProducer.send(message);

            final var messageConsumer = session.createConsumer(receivingDestination);
            final var receivedMessage = messageConsumer.receive(Timeouts.receiveMillis());

            assertNotNull(receivedMessage, "Message not received");
            assertEquals("test", message.getText());
        }
    }

    @Test
    void anonymousRelayRoutingWithExchangeAndRoutingKeyDestination(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var sendingDestination = session.createTopic(EXCHANGE_NAME + "/" + ROUTING_KEY);
            final var receivingDestination = session.createQueue(QUEUE_NAME);
            final var message = session.createTextMessage("test");

            final var messageProducer = session.createProducer(null);
            messageProducer.send(sendingDestination, message);

            final var messageConsumer = session.createConsumer(receivingDestination);
            final var receivedMessage = messageConsumer.receive(Timeouts.receiveMillis());

            assertNotNull(receivedMessage, "Message not received");
            assertEquals("test", message.getText());
        }
    }

    @Test
    void routingToQueue(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var sendingDestination = session.createQueue(QUEUE_NAME);
            final var receivingDestination = session.createQueue(QUEUE_NAME);
            final var message = session.createTextMessage("test");

            final var messageProducer = session.createProducer(sendingDestination);
            messageProducer.send(message);

            final var messageConsumer = session.createConsumer(receivingDestination);
            final var receivedMessage = messageConsumer.receive(Timeouts.receiveMillis());

            assertNotNull(receivedMessage, "Message not received");
            assertEquals("test", message.getText());
        }
    }

    @Test
    void anonymousRelayRoutingToQueue(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var sendingDestination = session.createQueue(QUEUE_NAME);
            final var receivingDestination = session.createQueue(QUEUE_NAME);
            final var message = session.createTextMessage("test");

            final var messageProducer = session.createProducer(null);
            messageProducer.send(sendingDestination, message);

            final var messageConsumer = session.createConsumer(receivingDestination);
            final var receivedMessage = messageConsumer.receive(Timeouts.receiveMillis());

            assertNotNull(receivedMessage, "Message not received");
            assertEquals("test", message.getText());
        }
    }
}
