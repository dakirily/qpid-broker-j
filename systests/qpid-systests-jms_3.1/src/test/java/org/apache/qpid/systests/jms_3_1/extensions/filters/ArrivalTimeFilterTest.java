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

package org.apache.qpid.systests.jms_3_1.extensions.filters;

import static org.apache.qpid.systests.JmsAwait.pause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;

import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("filters")
@Tag("queue")
@Tag("timing")
class ArrivalTimeFilterTest
{
    @Test
    void queueDefaultFilterArrivalTime0(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().replayPeriod(0).bind().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue))
        {
            producer.send(session.createTextMessage("A"));

            final var consumer = session.createConsumer(queue);
            connection.start();

            producer.send(session.createTextMessage("B"));

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "Message should be received");
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage, "Unexpected message type");
            assertEquals("B", textMessage.getText(), "Unexpected message");
        }
    }

    @Test
    void queueDefaultFilterArrivalTime1000(final JmsSupport jms) throws Exception
    {
        final long period = Timeouts.receiveMillis() / 1000;
        final var queue = jms.builder().queue().replayPeriod(period).bind().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));

            pause(Duration.ofMillis(Timeouts.receiveMillis() / 4));

            final var consumer = session.createConsumer(queue);

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "Message A should be received");
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage, "Unexpected message type");
            assertEquals("A", textMessage.getText(), "Unexpected message");

            producer.send(session.createTextMessage("B"));

            final var secondMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(secondMessage, "Message B should be received");
            final var secondTextMessage = assertInstanceOf(TextMessage.class, secondMessage, "Unexpected message type");
            assertEquals("B", secondTextMessage.getText(), "Unexpected message");
        }
    }
}
