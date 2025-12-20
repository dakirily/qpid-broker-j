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
@Tag("filters")
@Tag("queue")
class DefaultFiltersTest
{
    @Test
    void defaultFilterIsApplied(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var queue = jms.builder().queue().jmsSelector("foo = 1").create();
            final var prod = session.createProducer(queue);
            var message = session.createMessage();
            message.setIntProperty("foo", 0);
            prod.send(message);

            final var cons = session.createConsumer(queue);

            assertNull(cons.receive(Timeouts.noMessagesMillis()), "Message with foo=0 should not be received");

            message = session.createMessage();
            message.setIntProperty("foo", 1);
            prod.send(message);

            final var receivedMsg = cons.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMsg, "Message with foo=1 should be received");
            assertEquals(1, receivedMsg.getIntProperty("foo"), "Property foo not as expected");
        }
    }

    @Test
    void defaultFilterIsOverridden(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var queue = jms.builder().queue().jmsSelector("foo = 1").create();
            final var prod = session.createProducer(queue);
            var message = session.createMessage();
            message.setIntProperty("foo", 0);
            prod.send(message);

            final var cons = session.createConsumer(queue, "foo = 0");

            final var receivedMsg = cons.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMsg, "Message with foo=0 should be received");
            assertEquals(0, receivedMsg.getIntProperty("foo"), "Property foo not as expected");

            message = session.createMessage();
            message.setIntProperty("foo", 1);
            prod.send( message);

            assertNull(cons.receive(Timeouts.noMessagesMillis()), "Message with foo=1 should not be received");
        }
    }
}
