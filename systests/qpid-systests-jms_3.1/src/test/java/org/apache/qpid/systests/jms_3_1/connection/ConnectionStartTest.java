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

package org.apache.qpid.systests.jms_3_1.connection;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@JmsSystemTest
@Tag("connection")
class ConnectionStartTest
{
    @Test
    void testConsumerCanReceiveMessageAfterConnectionStart(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            jms.messages().send(connection, queue, 1);

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            assertNull(consumer.receive(Timeouts.receiveMillis() / 2),
                    "No messages should be delivered when the connection is stopped");
            connection.start();
            assertNotNull(consumer.receive(Timeouts.receiveMillis()),
                    "There should be messages waiting for the consumer");
        }
    }

    @Test
    void testMessageListenerCanReceiveMessageAfterConnectionStart(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            jms.messages().send(connection, queue, 1);

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            final CountDownLatch awaitMessage = new CountDownLatch(1);
            final AtomicLong deliveryTime = new AtomicLong();
            consumer.setMessageListener(message -> {
                try {
                    deliveryTime.set(System.currentTimeMillis());
                } finally {
                    awaitMessage.countDown();
                }
            });

            long beforeStartTime = System.currentTimeMillis();
            connection.start();

            assertTrue(awaitMessage.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "Message is not received in timely manner");
            assertTrue(deliveryTime.get() >= beforeStartTime, "Message received before connection start");
        }
    }
}
