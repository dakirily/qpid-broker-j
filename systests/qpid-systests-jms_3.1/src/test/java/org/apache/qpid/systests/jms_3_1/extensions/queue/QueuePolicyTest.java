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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Map;

import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("policy")
@Tag("queue")
class QueuePolicyTest
{
    @Test
    void rejectPolicyMessageDepth(final JmsSupport jms) throws Exception
    {
        final var destination = jms.builder().queue()
                .maximumQueueDepthMessages(5)
                .overflowPolicy(OverflowPolicy.REJECT)
                .create();
        try (final var connection = jms.builder().connection().syncPublish(true).create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var producer = session.createProducer(destination))
        {
            for (int i = 0; i < 5; i++)
            {
                producer.send(session.createMessage());
                session.commit();
            }

            assertThrows(JMSException.class, () ->
            {
                producer.send(session.createMessage());
                session.commit();
            }, "The client did not receive an exception after exceeding the queue limit");
        }

        try (final var secondConnection = jms.builder().connection().create())
        {
            secondConnection.start();

            final var secondSession = secondConnection.createSession(true, Session.SESSION_TRANSACTED);
            final var consumer = secondSession.createConsumer(destination);
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "Message  is not received");
            secondSession.commit();

            final var secondProducer = secondSession.createProducer(destination);
            secondProducer.send(secondSession.createMessage());
            secondSession.commit();
        }
    }

    @Test
    void ringPolicy(final JmsSupport jms) throws Exception
    {
        final var destination = jms.builder().queue()
                .maximumQueueDepthMessages(2)
                .overflowPolicy(OverflowPolicy.RING)
                .create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(destination))
        {
            producer.send(session.createTextMessage("Test1"));
            producer.send(session.createTextMessage("Test2"));
            producer.send(session.createTextMessage("Test3"));

            final var consumer = session.createConsumer(destination);
            connection.start();

            var receivedMessage = (TextMessage) consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "The consumer should receive the receivedMessage with body='Test2'");
            assertEquals("Test2", receivedMessage.getText(), "Unexpected first message");

            receivedMessage = (TextMessage) consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "The consumer should receive the receivedMessage with body='Test3'");
            assertEquals("Test3", receivedMessage.getText(), "Unexpected second message");
        }
    }

    @Test
    void roundTripWithFlowToDisk(final JmsSupport jms) throws Exception
    {
        assumeTrue(jms.brokerAdmin().supportsRestart(), "Test requires persistent store");

        final var queue = jms.builder().queue()
                .maximumQueueDepthBytes(0L)
                .overflowPolicy(OverflowPolicy.FLOW_TO_DISK)
                .create();

        final Map<String, Object> statistics = jms.virtualhost().statistics("bytesEvacuatedFromMemory");
        final Long originalBytesEvacuatedFromMemory = (Long) statistics.get("bytesEvacuatedFromMemory");

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED))
        {
            final var message = session.createTextMessage("testMessage");
            final var producer = session.createProducer(queue);
            producer.send(message);
            session.commit();

            // make sure we are flowing to disk
            final Map<String, Object> statistics2 = jms.virtualhost().statistics("bytesEvacuatedFromMemory");
            final Long bytesEvacuatedFromMemory = (Long) statistics2.get("bytesEvacuatedFromMemory");
            assertTrue(bytesEvacuatedFromMemory > originalBytesEvacuatedFromMemory,
                    "Message was not evacuated from memory");

            final var consumer = session.createConsumer(queue);
            connection.start();
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "Did not receive message");
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage, "Unexpected message type");
            assertEquals(message.getText(), textMessage.getText(), "Unexpected message content");
        }
    }
}
