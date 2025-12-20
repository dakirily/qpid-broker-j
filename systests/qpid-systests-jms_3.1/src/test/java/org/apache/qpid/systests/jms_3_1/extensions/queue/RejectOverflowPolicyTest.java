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
 */

package org.apache.qpid.systests.jms_3_1.extensions.queue;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.naming.NamingException;

import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("policy")
@Tag("queue")
class RejectOverflowPolicyTest extends OverflowPolicyTestBase
{
    @Test
    void testMaximumQueueDepthBytesExceeded(final JmsSupport jms) throws Exception
    {
        final long messageSize = evaluateMessageSize(jms);
        final long maximumQueueDepthBytes = messageSize + messageSize / 2;
        final var queue = jms.builder().queue()
            .maximumQueueDepthBytes(maximumQueueDepthBytes)
            .maximumQueueDepthMessages(-1)
            .overflowPolicy(OverflowPolicy.REJECT)
            .resumeCapacity(-1)
            .create();
        verifyOverflowPolicyRejectingSecondMessage(jms, queue);
    }

    @Test
    void testMaximumQueueDepthMessagesExceeded(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue()
                .maximumQueueDepthBytes(-1)
                .maximumQueueDepthMessages(1)
                .overflowPolicy(OverflowPolicy.REJECT)
                .resumeCapacity(-1)
                .create();
        verifyOverflowPolicyRejectingSecondMessage(jms, queue);
    }

    private void verifyOverflowPolicyRejectingSecondMessage(final JmsSupport jms, final Queue queue) throws NamingException, JMSException
    {
        try (final var producerConnection = jms.builder().connection().syncPublish(true).create();
             final var producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var firstMessage = nextMessage(0, producerSession);
            final var secondMessage = nextMessage(1, producerSession);

            final var producer = producerSession.createProducer(queue);
            producer.send(firstMessage);

            assertThrows(JMSException.class,
                    () -> producer.send(secondMessage),
                    "Message send should fail due to reject policy");

            final var producerSession2 = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var producer2 = producerSession2.createProducer(queue);

            try (final var consumerConnection = jms.builder().connection().create();
                 var consumerSession = consumerConnection.createSession(true, Session.SESSION_TRANSACTED);
                 var consumer = consumerSession.createConsumer(queue))
            {
                consumerConnection.start();

                final var message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Message is not received");
                assertEquals(0, message.getIntProperty(INDEX));

                consumerSession.commit();

                producer2.send(secondMessage);

                final var message2 = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message2, "Message is not received");
                assertEquals(1, message2.getIntProperty(INDEX));

                consumerSession.commit();
            }
        }
    }
}
