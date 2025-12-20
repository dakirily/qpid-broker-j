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

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on message order for a single session.
 *
 * The tests in this class are aligned with:
 * - 6.2.9  "Message order"
 * - 6.2.9.1 "Order of message receipt"
 */
@JmsSystemTest
@Tag("queue")
class MessageOrderTest
{
    /**
     * Verifies that messages sent in a single session to the same destination
     * are received in the order they were sent (6.2.9.1).
     */
    @Test
    void messagesSentBySingleSessionAreReceivedInOrder(final JmsSupport jms) throws Exception
    {
        final int count = 5;
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            jms.messages().send(session, queue, count);

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            for (int i = 0; i < count; i++)
            {
                final Message message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Expected message was not received");
                assertEquals(i, message.getIntProperty(INDEX), "Unexpected message order");
            }
        }
    }
}
