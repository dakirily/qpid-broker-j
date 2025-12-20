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

package org.apache.qpid.systests.jms_3_1.message;

import static org.apache.qpid.systests.JmsAwait.jms;
import static org.apache.qpid.systests.JmsAwait.pause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import jakarta.jms.TopicConnection;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

import java.time.Duration;

@JmsSystemTest
@Tag("message")
@Tag("queue")
@Tag("timing")
class TimeToLiveTest
{
    @Test
    void passiveTTL(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final long ttl = Math.max(2000L, Timeouts.receiveMillis());
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var producer = session.createProducer(queue))
        {
            producer.setTimeToLive(ttl);
            producer.send(session.createTextMessage("A"));
            producer.setTimeToLive(0);
            producer.send(session.createTextMessage("B"));
            session.commit();

            pause(Duration.ofMillis(ttl + ttl / 5));

            final var consumer = session.createConsumer(queue);
            connection.start();
            final var message = consumer.receive(Timeouts.receiveMillis());

            final var textMessage = assertInstanceOf(TextMessage.class, message, "TextMessage should be received");
            assertEquals("B", textMessage.getText(), "Unexpected message received");
        }
    }

    @Test
    void activeTTL(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final long ttl = Math.max(2000L, Timeouts.receiveMillis());
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var producer = session.createProducer(queue))
        {
            producer.setTimeToLive(ttl);
            producer.send(session.createTextMessage("A"));
            producer.setTimeToLive(0);
            producer.send(session.createTextMessage("B"));
            session.commit();

            final var consumer = session.createConsumer(queue);
            connection.start();
            var message = consumer.receive(Timeouts.receiveMillis());

            final var textMessage = assertInstanceOf(TextMessage.class, message, "TextMessage should be received");
            assertEquals("A", textMessage.getText(), "Unexpected message received");

            jms(Duration.ofMillis(ttl).multipliedBy(2))
                    .pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() ->
                    {
                        session.rollback();
                        final var messageAfterTtl = consumer.receive(Timeouts.receiveMillis());
                        final var textMessageAfterTtl = assertInstanceOf(TextMessage.class, messageAfterTtl, "TextMessage should be received after waiting for TTL");
                        assertEquals("B", textMessageAfterTtl.getText(),
                                "Unexpected message received after waiting for TTL");
                    });
        }
    }

    @Test
    void passiveTTLWithDurableSubscription(final JmsSupport jms) throws Exception
    {
        final long ttl = Math.max(2000L, Timeouts.receiveMillis());
        final var subscriptionName = jms.testMethodName() + "_sub";
        final var topic = jms.builder().topic().create();

        try (final var connection = jms.builder().connection().create(TopicConnection.class);
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var durableSubscriber = session.createDurableSubscriber(topic, subscriptionName);
             final var producer = session.createProducer(topic))
        {
            producer.setTimeToLive(ttl);
            producer.send(session.createTextMessage("A"));
            producer.setTimeToLive(0);
            producer.send(session.createTextMessage("B"));
            session.commit();

            connection.start();
            var message = durableSubscriber.receive(Timeouts.receiveMillis());

            final var textMessage = assertInstanceOf(TextMessage.class, message, "TextMessage should be received");
            assertEquals("A", textMessage.getText(), "Unexpected message received");

            jms(Duration.ofMillis(ttl).multipliedBy(2))
                    .pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() ->
                    {
                        session.rollback();
                        final var messageAfterTtl = durableSubscriber.receive(Timeouts.receiveMillis());
                        final var textMessageAfterTtl = assertInstanceOf(TextMessage.class, messageAfterTtl, "TextMessage should be received after waiting for TTL");
                        assertEquals("B", textMessageAfterTtl.getText(),
                                "Unexpected message received after waiting for TTL");
                    });
        }
    }

    @Test
    void activeTTLWithDurableSubscription(final JmsSupport jms) throws Exception
    {
        final var subscriptionName = jms.testMethodName() + "_sub";
        final var topic = jms.builder().topic().create();
        final long ttl = Math.max(2000L, Timeouts.receiveMillis());
        try (final var connection = jms.builder().connection().create(TopicConnection.class);
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var durableSubscriber = session.createDurableSubscriber(topic, subscriptionName);
             final var producer = session.createProducer(topic))
        {
            producer.setTimeToLive(ttl);
            producer.send(session.createTextMessage("A"));
            producer.setTimeToLive(0);
            producer.send(session.createTextMessage("B"));
            session.commit();

            jms(Duration.ofMillis(ttl).multipliedBy(2))
                    .pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() ->
                    {
                        connection.start();
                        final var message = durableSubscriber.receive(Timeouts.receiveMillis());

                        final var textMessage = assertInstanceOf(TextMessage.class, message, "TextMessage should be received");
                        assertEquals("B", textMessage.getText(), "Unexpected message received");
                    });
        }
    }
}
