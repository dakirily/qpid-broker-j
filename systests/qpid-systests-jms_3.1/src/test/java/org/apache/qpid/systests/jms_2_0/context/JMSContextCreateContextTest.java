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

package org.apache.qpid.systests.jms_2_0.context;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;
import org.apache.qpid.systests.JmsTestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JMSContextCreateContextTest extends JmsTestBase
{
    @Test
    void createdChildContextUsesSameClientId() throws Exception
    {
        final var clientId = getFullTestName();
        final ConnectionFactory cf = getConnectionBuilder().setClientId(clientId).buildConnectionFactory();

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
    void createdChildContextUsesDifferentSession() throws Exception
    {
        final ConnectionFactory cf = getConnectionBuilder().buildConnectionFactory();

        try (final JMSContext parentContext = cf.createContext(JMSContext.SESSION_TRANSACTED);
             final JMSContext childContext = parentContext.createContext(JMSContext.SESSION_TRANSACTED);
             final JMSContext consumerContext = parentContext.createContext(JMSContext.AUTO_ACKNOWLEDGE))
        {
            createQueue(getTestName());

            final var queue = parentContext.createQueue(getTestName());
            final var producer1 = parentContext.createProducer();
            final var producer2 = childContext.createProducer();
            final var consumer = consumerContext.createConsumer(queue);

            producer1.send(queue, "from-parent");
            producer2.send(queue, "from-child");

            childContext.commit();

            final var messageFromChild = consumer.receive(getReceiveTimeout());
            assertNotNull(messageFromChild, "message from child is null");
            assertEquals("from-child", messageFromChild.getBody(String.class));

            final var messageFromParent1 = consumer.receive(getReceiveTimeout());
            assertNull(messageFromParent1, "message from parent should be null before parent commit");

            parentContext.commit();

            final var messageFromParent2 = consumer.receive(getReceiveTimeout());
            assertNotNull(messageFromParent2, "message from parent is null");
            assertEquals("from-parent", messageFromParent2.getBody(String.class));
        }
    }
}
