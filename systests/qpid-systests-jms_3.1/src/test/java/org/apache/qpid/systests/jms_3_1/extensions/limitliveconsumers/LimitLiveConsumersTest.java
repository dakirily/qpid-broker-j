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

package org.apache.qpid.systests.jms_3_1.extensions.limitliveconsumers;

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
class LimitLiveConsumersTest
{
    @Test
    void limitLiveConsumers(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().maximumLiveConsumers(1).create();
        final int numberOfMessages = 5;

        try (final var connection = jms.builder().connection().syncPublish(true).create();
             final var session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();

            final var consumer1 = session1.createConsumer(queue);
            final var session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer2 = session2.createConsumer(queue);

            jms.messages().send(session1, queue, numberOfMessages);

            for (int i = 0; i < 3; i++)
            {
                final var receivedMsg = consumer1.receive(Timeouts.receiveMillis());
                assertNotNull(receivedMsg, "Message " + i + " not received");
                assertEquals(i, receivedMsg.getIntProperty(INDEX), "Unexpected message");
            }

            assertNull(consumer2.receive(Timeouts.noMessagesMillis()), "Unexpected message arrived");

            consumer1.close();
            session1.close();

            for (int i = 3; i < numberOfMessages; i++)
            {
                final var receivedMsg = consumer2.receive(Timeouts.receiveMillis());
                assertNotNull(receivedMsg, "Message " + i + " not received");
                assertEquals(i, receivedMsg.getIntProperty(INDEX), "Unexpected message");
            }

            assertNull(consumer2.receive(Timeouts.noMessagesMillis()), "Unexpected message arrived");

            final var session3 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer3 = session3.createConsumer(queue);
            final var producer = session3.createProducer(queue);

            producer.send(jms.messages().createNext(session3, 6));
            producer.send(jms.messages().createNext(session3, 7));

            assertNotNull(consumer2.receive(Timeouts.receiveMillis()), "Message not received on second consumer");
            assertNull(consumer3.receive(Timeouts.noMessagesMillis()),
                    "Message unexpectedly received on third consumer");

            final var session4 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer4 = session4.createConsumer(queue);

            assertNull(consumer4.receive(Timeouts.noMessagesMillis()),
                    "Message unexpectedly received on fourth consumer");
            consumer3.close();
            session3.close();

            assertNull(consumer4.receive(Timeouts.noMessagesMillis()),
                    "Message unexpectedly received on fourth consumer");
            consumer2.close();
            session2.close();

            assertNotNull(consumer4.receive(Timeouts.receiveMillis()), "Message not received on fourth consumer");

            assertNull(consumer4.receive(Timeouts.noMessagesMillis()), "Unexpected message arrived");
        }
    }
}
