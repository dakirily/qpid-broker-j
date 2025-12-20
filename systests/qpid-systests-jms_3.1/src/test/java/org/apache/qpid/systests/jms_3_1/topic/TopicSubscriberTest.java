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

package org.apache.qpid.systests.jms_3_1.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.jms.TopicConnection;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("topic")
class TopicSubscriberTest
{
    @Test
    void messageDeliveredToAllSubscribers(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();
        try (final var connection = jms.builder().connection().create(TopicConnection.class);
             final var session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createPublisher(topic);
             final var subscriber1 = session.createSubscriber(topic))
        {
            assertEquals(topic.getTopicName(), subscriber1.getTopic().getTopicName(), "Unexpected subscriber1 topic");
            final var subscriber2 = session.createSubscriber(topic);
            assertEquals(topic.getTopicName(), subscriber2.getTopic().getTopicName(), "Unexpected subscriber2 topic");

            connection.start();
            final var messageText = "Test Message";
            producer.send(session.createTextMessage(messageText));

            final var subscriber1Message = subscriber1.receive(Timeouts.receiveMillis());
            final var subscriber2Message = subscriber2.receive(Timeouts.receiveMillis());

            final var textMessage1 = assertInstanceOf(TextMessage.class, subscriber1Message, "TextMessage should be received  by subscriber1");
            assertEquals(messageText, textMessage1.getText());
            final var textMessage2 = assertInstanceOf(TextMessage.class, subscriber2Message, "TextMessage should be received  by subscriber2");
            assertEquals(messageText, textMessage2.getText());
        }
    }

    @Test
    void publishedMessageIsLostWhenSubscriberDisconnected(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(topic);
             final var subscriber = session.createConsumer(topic))
        {
            connection.start();
            producer.send(session.createTextMessage("A"));

            final var message1 = subscriber.receive(Timeouts.receiveMillis());
            final var textMessage1 = assertInstanceOf(TextMessage.class, message1, "TextMessage should be received");
            assertEquals("A", textMessage1.getText(), "Unexpected message received");

            subscriber.close();

            producer.send(session.createTextMessage("B"));
            final var subscriber2 = session.createConsumer(topic);
            producer.send(session.createTextMessage("C"));

            final var message2 = subscriber2.receive(Timeouts.receiveMillis());
            final var textMessage2 = assertInstanceOf(TextMessage.class, message2, "TextMessage should be received");
            assertEquals("C", textMessage2.getText(), "Unexpected message received");
        }
    }

    @Test
    void selector(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var subscriber = session.createConsumer(topic, "id='B'");
             final var producer = session.createProducer(topic))
        {
            final var message1 = session.createMessage();
            message1.setStringProperty("id", "A");
            producer.send(message1);
            final var message2 = session.createMessage();
            message2.setStringProperty("id", "B");
            producer.send(message2);

            connection.start();
            final var receivedMessage = subscriber.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "Message not received");
            assertEquals("B", receivedMessage.getStringProperty("id"), "Unexpected message received");
        }
    }

    @Test
    void noLocal(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();
        try (final var connection = jms.builder().connection().create();
             final var connection2 = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var subscriber = session.createConsumer(topic, null, true);
             final var session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer1 = session.createProducer(topic);
             final var producer2 = session2.createProducer(topic))
        {
            producer1.send(session.createTextMessage("A"));
            producer2.send(session2.createTextMessage("B"));

            connection.start();
            final var receivedMessage = subscriber.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage, "TextMessage should be received");
            assertEquals("B", textMessage.getText(), "Unexpected message received");
        }
    }
}
