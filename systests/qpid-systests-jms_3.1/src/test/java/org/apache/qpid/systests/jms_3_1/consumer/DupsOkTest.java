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

package org.apache.qpid.systests.jms_3_1.consumer;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@JmsSystemTest
@Tag("queue")
class DupsOkTest
{
    @Test
    void synchronousReceive(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            final int numberOfMessages = 3;
            connection.start();
            jms.messages().send(connection, queue, numberOfMessages);

            final var session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            for (int i = 0; i < numberOfMessages; i++) {
                Message received = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(received, "Expected message (%d) not received".formatted(i));
                assertEquals(i, received.getIntProperty(INDEX), "Unexpected message received");
            }

            assertNull(consumer.receive(Timeouts.noMessagesMillis()), "Received too many messages");

        }
    }

    @Test
    void asynchronousReceive(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            final int numberOfMessages = 3;
            connection.start();
            jms.messages().send(connection, queue, numberOfMessages);

            final var session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            AtomicReference<Throwable> exception = new AtomicReference<>();
            CountDownLatch completionLatch = new CountDownLatch(numberOfMessages);
            AtomicInteger expectedIndex = new AtomicInteger();

            consumer.setMessageListener(message -> {
                try {
                    Object index = message.getObjectProperty(INDEX);
                    assertEquals(expectedIndex.getAndIncrement(), message.getIntProperty(INDEX), "Unexpected message received");
                } catch (Throwable e) {
                    exception.set(e);
                } finally {
                    completionLatch.countDown();
                }
            });

            boolean completed = completionLatch.await(Timeouts.receiveMillis() * numberOfMessages, TimeUnit.MILLISECONDS);
            assertTrue(completed, "Message listener did not receive all messages within expected");
            assertNull(exception.get(), "Message listener encountered unexpected exception");
        }
    }
}
