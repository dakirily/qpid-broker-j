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

package org.apache.qpid.systests.jms_3_1.context;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@JmsSystemTest
class JMSContextCreateContextTest
{
    @Test
    @Tag("connection")
    void createdChildContextUsesSameClientId(final JmsSupport jms) throws Exception
    {
        final var clientId = jms.fullTestName();
        final ConnectionFactory cf = jms.builder().connection().clientId(clientId).connectionFactory();

        try (final JMSContext firstContext = cf.createContext(JMSContext.AUTO_ACKNOWLEDGE))
        {
            assertNotNull(firstContext, "first JMSContext must be created");
            assertEquals(clientId, firstContext.getClientID(), "unexpected clientID");

            try (final JMSContext secondContext = firstContext.createContext(JMSContext.AUTO_ACKNOWLEDGE))
            {
                assertThrows(JMSRuntimeException.class, () -> secondContext.setClientID(clientId),
                        "should not be able to set clientID on existing context with clientID already set");

                assertEquals(clientId, secondContext.getClientID(), "unexpected clientID");
            }
        }
    }

    @Test
    @Tag("queue")
    @Tag("transactions")
    void createdChildContextUsesDifferentSession(final JmsSupport jms) throws Exception
    {
        final ConnectionFactory cf = jms.builder().connection().connectionFactory();

        try (final JMSContext parentContext = cf.createContext(JMSContext.SESSION_TRANSACTED);
             final JMSContext childContext = parentContext.createContext(JMSContext.SESSION_TRANSACTED);
             final JMSContext consumerContext = parentContext.createContext(JMSContext.AUTO_ACKNOWLEDGE))
        {
            jms.builder().queue().create();

            final var queue = parentContext.createQueue(jms.testMethodName());
            final var producer1 = parentContext.createProducer();
            final var producer2 = childContext.createProducer();
            final var consumer = consumerContext.createConsumer(queue);

            producer1.send(queue, "from-parent");
            producer2.send(queue, "from-child");

            childContext.commit();

            final var messageFromChild = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(messageFromChild, "message from child is null");
            assertEquals("from-child", messageFromChild.getBody(String.class));

            final var messageFromParent1 = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(messageFromParent1, "message from parent should be null before parent commit");

            parentContext.commit();

            final var messageFromParent2 = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(messageFromParent2, "message from parent is null");
            assertEquals("from-parent", messageFromParent2.getBody(String.class));
        }
    }
}
