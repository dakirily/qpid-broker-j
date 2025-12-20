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

package org.apache.qpid.systests.jms_3_1.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("queue")
@Tag("message")
@Tag("timing")
@Tag("simplified-api")
class JMSProducerOptionsTest
{
    @Test
    void deliveryModePriorityAndTtlApplied(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var ctx = jms.builder().connection().connectionFactory().createContext();
             final var consumer = ctx.createConsumer(queue))
        {
            ctx.createProducer()
                    .setDeliveryMode(DeliveryMode.PERSISTENT)
                    .setPriority(7)
                    .setTimeToLive(5_000)
                    .send(queue, "payload");

            final Message message = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(message);

            assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
            assertEquals(7, message.getJMSPriority());
            assertTrue(message.getJMSExpiration() > 0, "JMSExpiration should be set when TTL > 0");
        }
    }

    @Test
    void disableMessageIdAndTimestamp(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var ctx = jms.builder().connection().connectionFactory().createContext();
             final var consumer = ctx.createConsumer(queue))
        {
            ctx.createProducer()
                    .setDisableMessageID(true)
                    .setDisableMessageTimestamp(true)
                    .send(queue, "payload");

            final Message message = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(message);

            assertNull(message.getJMSMessageID(), "JMSMessageID should be null when disabled");
            assertEquals(0L, message.getJMSTimestamp(), "JMSTimestamp should be 0 when disabled");
        }
    }

    @Test
    void producerPropertiesAreAppliedAndCanBeCleared(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var ctx = jms.builder().connection().connectionFactory().createContext();
             final var consumer = ctx.createConsumer(queue))
        {
            final var producer = ctx.createProducer()
                    .setProperty("p1", "v1")
                    .setProperty("p2", 42);

            producer.send(queue, "payload");

            final var message1 = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(message1);
            assertEquals("v1", message1.getStringProperty("p1"));
            assertEquals(42, message1.getIntProperty("p2"));

            producer.clearProperties();
            producer.send(queue, "payload2");

            final var message2 = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(message2);
            assertFalse(message2.propertyExists("p1"));
            assertFalse(message2.propertyExists("p2"));
        }
    }
}

