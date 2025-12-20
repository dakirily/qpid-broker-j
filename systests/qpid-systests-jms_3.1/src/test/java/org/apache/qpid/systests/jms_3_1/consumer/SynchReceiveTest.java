/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
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
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@JmsSystemTest
@Tag("queue")
class SynchReceiveTest
{
    @Test
    void receiveWithTimeout(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            final int numberOfMessages = 3;
            connection.start();
            jms.messages().send(connection, queue, numberOfMessages);

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            for (int num = 0; num < numberOfMessages; num++)
            {
                assertNotNull(consumer.receive(Timeouts.receiveMillis()), "Expected message (%d) not received".formatted(num));
            }

            assertNull(consumer.receive(Timeouts.noMessagesMillis()), "Received too many messages");
        }
    }

    @Test
    void receiveNoWait(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            jms.messages().send(connection, queue, 1);

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            final Instant timeout = Instant.now().plusMillis(Timeouts.receiveMillis());
            Message msg;
            do {
                msg = consumer.receiveNoWait();
            }
            while (msg == null && Instant.now().isBefore(timeout));

            assertNotNull(msg, "Expected message not received within timeout");
            assertNull(consumer.receiveNoWait(), "Received too many messages");
        }
    }

    @Test
    void twoConsumersInterleaved(final JmsSupport jms) throws Exception
    {
        Queue queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().prefetch(0).create())
        {
            final int numberOfReceiveLoops = 3;
            final int numberOfMessages = numberOfReceiveLoops * 2;

            connection.start();
            jms.messages().send(connection, queue, numberOfMessages);

            Session consumerSession1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer1 = consumerSession1.createConsumer(queue);

            Session consumerSession2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer2 = consumerSession2.createConsumer(queue);

            for (int i = 0; i < numberOfReceiveLoops; i++)
            {
                final Message receive1 = consumer1.receive(Timeouts.receiveMillis());
                assertNotNull(receive1, "Expected message not received from consumer1 within timeout");

                final Message receive2 = consumer2.receive(Timeouts.receiveMillis());
                assertNotNull(receive2, "Expected message not received from consumer1 within timeout");
            }
        }
    }
}
