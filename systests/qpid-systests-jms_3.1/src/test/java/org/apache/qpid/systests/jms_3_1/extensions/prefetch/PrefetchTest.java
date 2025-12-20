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

package org.apache.qpid.systests.jms_3_1.extensions.prefetch;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import javax.naming.NamingException;

import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("policy")
@Tag("queue")
class PrefetchTest
{
    @Test
    void prefetch(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection1 = jms.builder().connection().prefetch(3).create())
        {
            connection1.start();

            final var session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer1 = session1.createConsumer(queue);

            jms.messages().send(connection1, queue, 6);

            final var receivedMessage = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "First message was not received");
            assertEquals(0, receivedMessage.getIntProperty(INDEX), "Received message has unexpected index");

            forceSync(session1);

            observeNextAvailableMessage(jms, queue, 4);
        }
    }

    /**
     * send two messages to the queue, consume and acknowledge one message on connection 1
     * create a second connection and attempt to consume the second message - this will only be possible
     * if the first connection has no prefetch
     */
    @Test
    void prefetchDisabled(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection1 = jms.builder().connection().prefetch(0).create())
        {
            connection1.start();

            final var session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer1 = session1.createConsumer(queue);

            jms.messages().send(connection1, queue, 2);

            final var receivedMessage = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "First message was not received");
            assertEquals(0, receivedMessage.getIntProperty(INDEX), "Message property was not as expected");

            observeNextAvailableMessage(jms, queue, 1);
        }
    }

    @Test
    void consumeBeyondPrefetch(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection1 = jms.builder().connection().prefetch(1).create())
        {
            connection1.start();

            final var session1 = connection1.createSession(true, Session.SESSION_TRANSACTED);
            final var consumer1 = session1.createConsumer(queue);

            jms.messages().send(connection1, queue, 5);

            var message = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(message);
            assertEquals(0, message.getIntProperty(INDEX));

            message = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(message);
            assertEquals(1, message.getIntProperty(INDEX));
            message = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(message);
            assertEquals(2, message.getIntProperty(INDEX));

            forceSync(session1);

            // In pre 0-10, in a transaction session the client does not ack the message until the commit occurs
            // so the message observed by another connection will have the index 3 rather than 4.
            try (final var connection2 = jms.builder().connection().create();
                 final var session2 = connection2.createSession(true, Session.SESSION_TRANSACTED);
                 final var consumer2 = session2.createConsumer(queue))
            {
                connection2.start();

                message = consumer2.receive(Timeouts.receiveMillis());
                assertNotNull(message);
                assertEquals( 4, message.getIntProperty(INDEX), "Received message has unexpected index");

                session2.rollback();
            }
        }
    }

    private void observeNextAvailableMessage(final JmsSupport jms, final Queue queue, final int expectedIndex) throws JMSException, NamingException
    {
        try (final var connection2 = jms.builder().connection().create())
        {
            connection2.start();
            final var session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer2 = session2.createConsumer(queue);

            final var receivedMessage2 = consumer2.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage2, "Observer connection did not receive message");
            assertEquals(expectedIndex, receivedMessage2.getIntProperty(INDEX),
                    "Message received by the observer connection has unexpected index");
        }
    }

    private void forceSync(final Session session1) throws Exception
    {
        session1.createTemporaryQueue().delete();
    }
}
