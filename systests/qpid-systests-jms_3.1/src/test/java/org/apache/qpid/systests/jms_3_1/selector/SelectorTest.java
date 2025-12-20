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

package org.apache.qpid.systests.jms_3_1.selector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.DeliveryMode;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.Message;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("filters")
@Tag("queue")
class SelectorTest
{
    private static final String INVALID_SELECTOR = "Cost LIKE 5";

    @Test
    void invalidSelector(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            assertThrows(InvalidSelectorException.class,
                    () -> session.createConsumer(queue, INVALID_SELECTOR),
                    "Exception not thrown");

            assertThrows(InvalidSelectorException.class,
                    () -> session.createBrowser(queue, INVALID_SELECTOR),
                    "Exception not thrown");
        }
    }

    @Test
    void runtimeSelectorError(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = session.createConsumer(queue , "testproperty % 5 = 1");
             final var producer = session.createProducer(queue))
        {
            final var message = session.createMessage();
            message.setIntProperty("testproperty", 1); // 1 % 5
            producer.send(message);

            connection.start();

            var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "Message matching selector should be received");

            message.setStringProperty("testproperty", "hello"); // "hello" % 5 would cause a runtime error
            producer.send(message);
            receivedMessage = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(receivedMessage, "Message causing runtime selector error should not be received");

            final var consumerWithoutSelector = session.createConsumer(queue);
            receivedMessage = consumerWithoutSelector.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage,
                    "Message that previously caused a runtime error should be consumable by another consumer");
        }
    }

    @Test
    void selectorWithJMSMessageID(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var producer = session.createProducer(queue);
            final var firstMessage = session.createMessage();
            final var secondMessage = session.createMessage();
            producer.send(firstMessage);
            producer.send(secondMessage);

            assertNotNull(firstMessage.getJMSMessageID());
            assertNotNull(secondMessage.getJMSMessageID());
            assertNotEquals(firstMessage.getJMSMessageID(), secondMessage.getJMSMessageID());

            final var consumer = session.createConsumer(queue, "JMSMessageID = '%s'".formatted(secondMessage.getJMSMessageID()));

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertEquals(secondMessage.getJMSMessageID(), receivedMessage.getJMSMessageID(), "Unexpected message received");
        }
    }

    @Test
    void selectorWithJMSDeliveryMode(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var producer = session.createProducer(queue);
            final var firstMessage = session.createMessage();
            final var secondMessage = session.createMessage();
            producer.send(firstMessage, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            producer.send(secondMessage, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            final var consumer = session.createConsumer(queue, "JMSDeliveryMode = 'PERSISTENT'");

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertEquals(secondMessage.getJMSMessageID(), receivedMessage.getJMSMessageID(), "Unexpected message received");
        }
    }
}
