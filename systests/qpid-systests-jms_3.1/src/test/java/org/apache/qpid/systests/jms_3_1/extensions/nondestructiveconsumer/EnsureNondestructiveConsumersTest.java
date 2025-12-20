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

package org.apache.qpid.systests.jms_3_1.extensions.nondestructiveconsumer;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("policy")
@Tag("queue")
class EnsureNondestructiveConsumersTest
{
    @Test
    void ensureNondestructiveConsumers(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().ensureNondestructiveConsumers(true).create();
        final int numberOfMessages = 5;
        try (final var connection = jms.builder().connection().syncPublish(true).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();

            jms.messages().send(session, queue, numberOfMessages);

            final var consumer1 = session.createConsumer(queue);

            for (int i = 0; i < numberOfMessages; i++)
            {
                final var receivedMsg = consumer1.receive(Timeouts.receiveMillis());
                assertNotNull(receivedMsg, "Message " + i + " not received");
                assertEquals(i, receivedMsg.getIntProperty(INDEX), "Unexpected message");
            }

            assertNull(consumer1.receive(Timeouts.noMessagesMillis()), "Unexpected message arrived");

            final var consumer2 = session.createConsumer(queue);

            for (int i = 0; i < numberOfMessages; i++)
            {
                final var receivedMsg = consumer2.receive(Timeouts.receiveMillis());
                assertNotNull(receivedMsg, "Message " + i + " not received");
                assertEquals(i, receivedMsg.getIntProperty(INDEX), "Unexpected message");
            }

            assertNull(consumer2.receive(Timeouts.noMessagesMillis()), "Unexpected message arrived");

            final var producer = session.createProducer(queue);
            producer.send(jms.messages().createNext(session, 6));

            assertNotNull(consumer1.receive(Timeouts.receiveMillis()), "Message not received on first consumer");
            assertNotNull(consumer2.receive(Timeouts.receiveMillis()), "Message not received on second consumer");

            assertNull(consumer1.receive(Timeouts.noMessagesMillis()), "Unexpected message arrived");
            assertNull(consumer2.receive(Timeouts.noMessagesMillis()), "Unexpected message arrived");
        }
    }
}
