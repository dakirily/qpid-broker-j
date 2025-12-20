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

package org.apache.qpid.systests.jms_3_1.extensions.autocreation;

import static org.apache.qpid.systests.EntityTypes.DIRECT_EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.FANOUT_EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.QUEUE;
import static org.apache.qpid.systests.EntityTypes.VIRTUALHOST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.virtualhost.NodeAutoCreationPolicy;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

@JmsSystemTest
@Tag("policy")
@Tag("routing")
class NodeAutoCreationPolicyTest
{
    private final static ObjectMapper OBJECT_MAPPER = ConfiguredObjectJacksonModule.newObjectMapper(false);
    private static final String DEAD_LETTER_QUEUE_SUFFIX = "_DLQ";
    private static final String DEAD_LETTER_EXCHANGE_SUFFIX = "_DLE";
    private static final String AUTO_CREATION_POLICIES = createAutoCreationPolicies();
    private static final String TEST_MESSAGE = "Hello world!";
    private static final String VALID_QUEUE_NAME = "fooQueue";

    private static String createAutoCreationPolicies()
    {
        try
        {
            final NodeAutoCreationPolicy[] policies = {
                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return "fooQ.*";
                        }

                        @Override
                        public boolean isCreatedOnPublish()
                        {
                            return true;
                        }

                        @Override
                        public boolean isCreatedOnConsume()
                        {
                            return true;
                        }

                        @Override
                        public String getNodeType()
                        {
                            return "Queue";
                        }

                        @Override
                        public Map<String, Object> getAttributes()
                        {
                            return Collections.emptyMap();
                        }
                    },
                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return "barE.*";
                        }

                        @Override
                        public boolean isCreatedOnPublish()
                        {
                            return true;
                        }

                        @Override
                        public boolean isCreatedOnConsume()
                        {
                            return false;
                        }

                        @Override
                        public String getNodeType()
                        {
                            return "Exchange";
                        }

                        @Override
                        public Map<String, Object> getAttributes()
                        {
                            return Collections.singletonMap(Exchange.TYPE, "fanout");
                        }
                    },
                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return ".*" + DEAD_LETTER_QUEUE_SUFFIX;
                        }

                        @Override
                        public boolean isCreatedOnPublish()
                        {
                            return true;
                        }

                        @Override
                        public boolean isCreatedOnConsume()
                        {
                            return true;
                        }

                        @Override
                        public String getNodeType()
                        {
                            return "Queue";
                        }

                        @Override
                        public Map<String, Object> getAttributes()
                        {
                            return Collections.emptyMap();
                        }
                    },
                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return ".*" + DEAD_LETTER_EXCHANGE_SUFFIX;
                        }

                        @Override
                        public boolean isCreatedOnPublish()
                        {
                            return true;
                        }

                        @Override
                        public boolean isCreatedOnConsume()
                        {
                            return false;
                        }

                        @Override
                        public String getNodeType()
                        {
                            return "Exchange";
                        }

                        @Override
                        public Map<String, Object> getAttributes()
                        {
                            return Collections.singletonMap(Exchange.TYPE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS);
                        }
                    }
            };

            return OBJECT_MAPPER.writeValueAsString(Arrays.asList(policies));
        }
        catch (JacksonException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void updateAutoCreationPolicies(final JmsSupport jms) throws Exception
    {
        jms.management().update(jms.virtualHostName(), VIRTUALHOST, Collections.singletonMap(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES, AUTO_CREATION_POLICIES));
    }

    @Test
    void sendingToQueuePattern(final JmsSupport jms) throws Exception
    {
        updateAutoCreationPolicies(jms);

        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var queue = session.createQueue(VALID_QUEUE_NAME);
            final var producer = session.createProducer(queue);
            producer.send(session.createTextMessage(TEST_MESSAGE));

            final var consumer = session.createConsumer(queue);
            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received);
            final var textMessage = assertInstanceOf(TextMessage.class, received);
            assertEquals(TEST_MESSAGE, textMessage.getText());
        }
    }

    @Test
    void concurrentQueueCreation(final JmsSupport jms) throws Exception
    {
        updateAutoCreationPolicies(jms);

        final String destination = VALID_QUEUE_NAME;
        final int numberOfActors = 3;
        final var connections = new Connection[numberOfActors];
        try
        {
            final var sessions = new Session[numberOfActors];
            for (int i = 0; i < numberOfActors; i++)
            {
                final var connection = jms.builder().connection().create();
                connections[i] = connection;
                sessions[i] = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }

            final List<CompletableFuture<MessageProducer>> futures = new ArrayList<>(numberOfActors);
            final ExecutorService executorService = Executors.newFixedThreadPool(numberOfActors);
            try
            {
                Stream.of(sessions).forEach(session ->
                        futures.add(CompletableFuture.supplyAsync(() -> publishMessage(session, destination), executorService)));
                final CompletableFuture<Void> combinedFuture =
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[numberOfActors]));
                combinedFuture.get(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS);
            }
            finally
            {
                executorService.shutdown();
                assertTrue(executorService.awaitTermination(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS));
            }

            try (final var connection = jms.builder().connection().create())
            {
                connection.start();
                final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final var queue = session.createQueue(destination);
                final var consumer = session.createConsumer(queue);

                for (int i = 0; i < numberOfActors; i++)
                {
                    final var received = consumer.receive(Timeouts.receiveMillis());
                    assertNotNull(received);
                    final var textMessage = assertInstanceOf(TextMessage.class, received);
                    assertEquals(TEST_MESSAGE, textMessage.getText());
                }
            }
        }
        finally
        {
            for (final var connection : connections)
            {
                if (connection != null)
                {
                    connection.close();
                }
            }
        }
    }

    private MessageProducer publishMessage(final Session session, final String destination)
    {
        try
        {
            final var queue = session.createQueue(destination);
            final var producer = session.createProducer(queue);
            producer.send(session.createTextMessage(TEST_MESSAGE));
            return producer;
        }
        catch (JMSException e)
        {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void sendingToNonMatchingQueuePattern(final JmsSupport jms) throws Exception
    {
        updateAutoCreationPolicies(jms);

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var queue = session.createQueue("foQueue");

            assertThrows(JMSException.class,
                    () -> session.createProducer(queue),
                    "Creating producer should fail");
        }
    }

    @Test
    void sendingToExchangePattern(final JmsSupport jms) throws Exception
    {
        updateAutoCreationPolicies(jms);

        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var topic = session.createTopic("barExchange/foo");
            final var producer = session.createProducer(topic);
            producer.send(session.createTextMessage(TEST_MESSAGE));

            final var consumer = session.createConsumer(topic);
            var received = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(received);

            producer.send(session.createTextMessage("Hello world2!"));
            received = consumer.receive(Timeouts.receiveMillis());

            assertNotNull(received);

            final var textMessage = assertInstanceOf(TextMessage.class, received);
            assertEquals("Hello world2!", textMessage.getText());
        }
    }

    @Test
    void sendingToNonMatchingTopicPattern(final JmsSupport jms) throws Exception
    {
        updateAutoCreationPolicies(jms);

        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var topic = session.createTopic("baa");

            assertThrows(JMSException.class,
                    () -> session.createProducer(topic),
                    "Creating producer should fail");
        }
    }

    @Test
    void queueAlternateBindingCreation(final JmsSupport jms) throws Exception
    {
        updateAutoCreationPolicies(jms);

        final String queueName = jms.testMethodName();
        final String deadLetterQueueName = queueName + DEAD_LETTER_QUEUE_SUFFIX;

        final Map<String, Object> attributes = new HashMap<>();
        final Map<String, Object> expectedAlternateBinding = Map.of(AlternateBinding.DESTINATION, deadLetterQueueName);
        attributes.put(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING,
                       OBJECT_MAPPER.writeValueAsString(expectedAlternateBinding));
        jms.management().create(queueName, QUEUE, attributes);

        final Map<String, Object> queueAttributes = jms.management().read(queueName, QUEUE, true);

        final var actualAlternateBinding = queueAttributes.get(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING);
        final Map<String, Object> actualAlternateBindingMap = convertIfNecessary(actualAlternateBinding);
        assertEquals(new HashMap<>(expectedAlternateBinding), new HashMap<>(actualAlternateBindingMap),
                "Unexpected alternate binding");

        final Map<String, Object> dlqAttributes = jms.management().read(deadLetterQueueName, QUEUE, true);
        assertNotNull(dlqAttributes, "Cannot get dead letter queue");
    }

    @Test
    void exchangeAlternateBindingCreation(final JmsSupport jms) throws Exception
    {
        updateAutoCreationPolicies(jms);

        final String exchangeName = jms.testMethodName();
        final String deadLetterExchangeName = exchangeName + DEAD_LETTER_EXCHANGE_SUFFIX;

        final Map<String, Object> attributes = new HashMap<>();
        final Map<String, Object> expectedAlternateBinding = Map.of(AlternateBinding.DESTINATION, deadLetterExchangeName);
        attributes.put(Exchange.ALTERNATE_BINDING, OBJECT_MAPPER.writeValueAsString(expectedAlternateBinding));
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        jms.management().create(exchangeName, DIRECT_EXCHANGE, attributes);

        final Map<String, Object> exchangeAttributes = jms.management().read(exchangeName, EXCHANGE, true);

        final var actualAlternateBinding = exchangeAttributes.get(Exchange.ALTERNATE_BINDING);
        final Map<String, Object> actualAlternateBindingMap = convertIfNecessary(actualAlternateBinding);
        assertEquals(new HashMap<>(expectedAlternateBinding), new HashMap<>(actualAlternateBindingMap),
                "Unexpected alternate binding");

        final Map<String, Object> dlqExchangeAttributes = jms.management().read(deadLetterExchangeName, FANOUT_EXCHANGE, true);
        assertNotNull(dlqExchangeAttributes, "Cannot get dead letter exchange");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> convertIfNecessary(final Object actualAlternateBinding)
    {
        final Map<String, Object> actualAlternateBindingMap;
        if (actualAlternateBinding instanceof String)
        {
            actualAlternateBindingMap = OBJECT_MAPPER.readValue((String) actualAlternateBinding, Map.class);
        }
        else
        {
            actualAlternateBindingMap = (Map<String, Object>) actualAlternateBinding;
        }
        return actualAlternateBindingMap;
    }
}
