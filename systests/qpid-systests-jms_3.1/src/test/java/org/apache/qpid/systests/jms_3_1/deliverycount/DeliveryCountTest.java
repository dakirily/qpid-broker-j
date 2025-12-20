package org.apache.qpid.systests.jms_3_1.deliverycount;

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

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@JmsSystemTest
@Tag("message")
@Tag("queue")
class DeliveryCountTest
{
    private static final int MAX_DELIVERY_ATTEMPTS = 3;
    private static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";

    @BeforeEach
    void beforeEach(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().maximumDeliveryAttempts(MAX_DELIVERY_ATTEMPTS).create();
        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var session = connection.createSession(Session.CLIENT_ACKNOWLEDGE))
        {
            connection.start();
            jms.messages().send(session, queue, 1);
        }
    }

    @Test
    void testDeliveryCountChangedOnRollback(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().get();
        try (final var connection = jms.builder().connection().prefetch(0).create())
        {
            final var session = connection.createSession(JMSContext.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            for (int i = 0; i < MAX_DELIVERY_ATTEMPTS; i++)
            {
                final var message = consumer.receive(Timeouts.receiveMillis());
                session.rollback();
                assertDeliveryCountHeaders(message, i);
            }
            final var message = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(message, "Message should be discarded");
        }
    }

    @Test
    void testDeliveryCountChangedOnSessionClose(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().get();
        try (final var connection = jms.builder().connection().prefetch(0).create())
        {
            connection.start();
            for (int i = 0; i < MAX_DELIVERY_ATTEMPTS; i++)
            {
                Session consumingSession = connection.createSession(JMSContext.SESSION_TRANSACTED);
                MessageConsumer consumer = consumingSession.createConsumer(queue);
                final var message = consumer.receive(Timeouts.receiveMillis());
                assertDeliveryCountHeaders(message, i);
                consumingSession.close();
            }

            final var session = connection.createSession(JMSContext.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            final var message = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(message, "Message should be discarded");
        }
    }

    private void assertDeliveryCountHeaders(final Message message, final int deliveryAttempt) throws JMSException
    {
        assertNotNull(message, "Message is not received on attempt %d".formatted(deliveryAttempt));
        assertEquals(deliveryAttempt > 0, message.getJMSRedelivered(),
                "Unexpected redelivered flag on attempt %d".formatted(deliveryAttempt));
        assertEquals(deliveryAttempt + 1, message.getIntProperty(JMSX_DELIVERY_COUNT),
                "Unexpected message delivery count on attempt %d".formatted(deliveryAttempt + 1));
    }
}

