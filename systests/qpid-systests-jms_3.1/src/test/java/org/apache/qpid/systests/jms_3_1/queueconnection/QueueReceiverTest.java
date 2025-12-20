/*
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

package org.apache.qpid.systests.jms_3_1.queueconnection;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.jms.QueueConnection;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("legacy")
@Tag("queue")
class QueueReceiverTest
{
    @Test
    void createReceiver(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var queueConnection = jms.builder().connection().create(QueueConnection.class))
        {
            queueConnection.start();
            jms.messages().send(queueConnection, queue, 3);

            final var session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            final var receiver = session.createReceiver(queue, "%s=2".formatted(INDEX));
            assertEquals(queue.getQueueName(), receiver.getQueue().getQueueName(),
                    "Queue names should match from QueueReceiver");

            final var received = receiver.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message is not received");
            assertEquals(2, received.getIntProperty(INDEX), "Unexpected message is received");
        }
    }
}
