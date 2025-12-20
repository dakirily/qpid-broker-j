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

import static org.apache.qpid.systests.EntityTypes.QUEUE;
import static org.apache.qpid.systests.EntityTypes.TOPIC_EXCHANGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HashMap;
import java.util.Map;

import jakarta.jms.DeliveryMode;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.support.AsyncSupport;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.systests.AmqpManagementFacade;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@JmsSystemTest
@Tag("policy")
@Tag("queue")
class QueueMessageDurabilityTest
{
    private static final String DURABLE_ALWAYS_PERSIST_NAME = "DURABLE_QUEUE_ALWAYS_PERSIST";
    private static final String DURABLE_NEVER_PERSIST_NAME = "DURABLE_QUEUE_NEVER_PERSIST";
    private static final String DURABLE_DEFAULT_PERSIST_NAME = "DURABLE_QUEUE_DEFAULT_PERSIST";
    private static final String NONDURABLE_ALWAYS_PERSIST_NAME = "NONDURABLE_QUEUE_ALWAYS_PERSIST";

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void sendPersistentMessageToAll(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        assumeTrue(jms.brokerAdmin().supportsRestart());

        prepare(jms);

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var producer = session.createProducer(null))
        {
            producer.send(session.createTopic(getTestTopic("Y.Y.Y.Y")), session.createTextMessage("test"));
            session.commit();
        }

        assertEquals(1, jms.queue(DURABLE_NEVER_PERSIST_NAME).depthMessages());
        assertEquals(1, jms.queue( NONDURABLE_ALWAYS_PERSIST_NAME).depthMessages());
        assertEquals(1, jms.queue(DURABLE_ALWAYS_PERSIST_NAME).depthMessages());
        assertEquals(1, jms.queue(DURABLE_DEFAULT_PERSIST_NAME).depthMessages());

        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        assertEquals(0, jms.queue(DURABLE_NEVER_PERSIST_NAME).depthMessages());
        assertEquals(1, jms.queue(DURABLE_ALWAYS_PERSIST_NAME).depthMessages());
        assertEquals(1, jms.queue(DURABLE_DEFAULT_PERSIST_NAME).depthMessages());
        assertFalse(isQueueExist(jms, NONDURABLE_ALWAYS_PERSIST_NAME));
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void sendNonPersistentMessageToAll(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        assumeTrue(jms.brokerAdmin().supportsRestart());

        prepare(jms);

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var producer = session.createProducer(null))
        {
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            connection.start();
            producer.send(session.createTopic(getTestTopic("Y.Y.Y.Y")), session.createTextMessage("test"));
            session.commit();
        }

        assertEquals(1, jms.queue(DURABLE_NEVER_PERSIST_NAME).depthMessages());
        assertEquals(1, jms.queue(NONDURABLE_ALWAYS_PERSIST_NAME).depthMessages());
        assertEquals(1, jms.queue(DURABLE_ALWAYS_PERSIST_NAME).depthMessages());
        assertEquals(1, jms.queue(DURABLE_DEFAULT_PERSIST_NAME).depthMessages());

        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        assertEquals(0, jms.queue(DURABLE_NEVER_PERSIST_NAME).depthMessages());
        assertEquals(1, jms.queue(DURABLE_ALWAYS_PERSIST_NAME).depthMessages());
        assertEquals(0, jms.queue(DURABLE_DEFAULT_PERSIST_NAME).depthMessages());
        assertFalse(isQueueExist(jms, NONDURABLE_ALWAYS_PERSIST_NAME));
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void nonPersistentContentRetained(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        assumeTrue(jms.brokerAdmin().supportsRestart());

        prepare(jms);

        try (final var connection = jms.builder().connection().create();)
        {
            connection.start();

            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var producer = session.createProducer(null);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            producer.send(session.createTopic(getTestTopic("N.N.Y.Y")), session.createTextMessage("test1"));
            producer.send(session.createTopic(getTestTopic("Y.N.Y.Y")), session.createTextMessage("test2"));
            session.commit();

            final var consumer = session.createConsumer(session.createQueue(DURABLE_ALWAYS_PERSIST_NAME));
            final var msg = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(msg);
            final var textMessage = assertInstanceOf(TextMessage.class, msg);
            assertEquals("test2", textMessage.getText());
            session.rollback();
        }

        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        assertEquals(0, jms.queue(DURABLE_NEVER_PERSIST_NAME).depthMessages());
        assertEquals(1, jms.queue(DURABLE_ALWAYS_PERSIST_NAME).depthMessages());
        assertEquals(0, jms.queue(DURABLE_DEFAULT_PERSIST_NAME).depthMessages());

        try (final var connection2 = jms.builder().connection().create())
        {
            connection2.start();
            final var session = connection2.createSession(true, Session.SESSION_TRANSACTED);

            final var consumer = session.createConsumer(session.createQueue(DURABLE_ALWAYS_PERSIST_NAME));
            final var msg = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(msg);
            final var textMessage = assertInstanceOf(TextMessage.class, msg);
            assertEquals("test2", textMessage.getText());
            session.commit();
        }
    }

    @Test
    void persistentContentRetainedOnTransientQueue(final JmsSupport jms) throws Exception
    {
        prepare(jms);

        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var producer = session.createProducer(null);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            producer.send(session.createTopic(getTestTopic("N.N.Y.Y")), session.createTextMessage("test1"));
            session.commit();
            var consumer =
                    session.createConsumer(session.createQueue(getTestQueue(DURABLE_DEFAULT_PERSIST_NAME)));
            var msg = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(msg);
            final var textMessage = assertInstanceOf(TextMessage.class, msg);
            assertEquals("test1", textMessage.getText());
            session.commit();

            consumer = session.createConsumer(session.createQueue(getTestQueue(NONDURABLE_ALWAYS_PERSIST_NAME)));
            msg = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(msg);
            final var textMsg = assertInstanceOf(TextMessage.class, msg);
            assertEquals("test1", textMsg.getText());
            session.commit();
        }
    }

    private boolean isQueueExist(final JmsSupport jms, final String queueName) throws Exception
    {
        try
        {
            jms.management().perform(queueName, "READ", QUEUE, Map.of());
            return true;
        }
        catch (AmqpManagementFacade.OperationUnsuccessfulException e)
        {
            if (e.getStatusCode() == 404)
            {
                return false;
            }
            else
            {
                throw e;
            }
        }
    }

    private void prepare(final JmsSupport jms) throws Exception
    {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.ALWAYS.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, true);
        jms.management().create(DURABLE_ALWAYS_PERSIST_NAME, QUEUE, arguments);

        arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.NEVER.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, true);
        jms.management().create(DURABLE_NEVER_PERSIST_NAME, QUEUE, arguments);

        arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.DEFAULT.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, true);
        jms.management().create(DURABLE_DEFAULT_PERSIST_NAME, QUEUE, arguments);

        arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.ALWAYS.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, false);
        jms.management().create(NONDURABLE_ALWAYS_PERSIST_NAME, QUEUE, arguments);

        arguments = new HashMap<>();
        arguments.put("destination", DURABLE_ALWAYS_PERSIST_NAME);
        arguments.put("bindingKey", "Y.*.*.*");
        jms.management().perform("amq.topic", "bind", TOPIC_EXCHANGE, arguments);

        arguments = new HashMap<>();
        arguments.put("destination", DURABLE_NEVER_PERSIST_NAME);
        arguments.put("bindingKey", "*.Y.*.*");
        jms.management().perform("amq.topic", "bind", TOPIC_EXCHANGE, arguments);

        arguments = new HashMap<>();
        arguments.put("destination", DURABLE_DEFAULT_PERSIST_NAME);
        arguments.put("bindingKey", "*.*.Y.*");
        jms.management().perform("amq.topic", "bind", TOPIC_EXCHANGE, arguments);

        arguments = new HashMap<>();
        arguments.put("destination", NONDURABLE_ALWAYS_PERSIST_NAME);
        arguments.put("bindingKey", "*.*.*.Y");
        jms.management().perform("amq.topic", "bind", TOPIC_EXCHANGE, arguments);
    }

    private String getTestTopic(final String routingKey)
    {
        return "amq.topic/%s".formatted(routingKey);
    }

    private String getTestQueue(final String name)
    {
        return "%s".formatted(name);
    }
}
