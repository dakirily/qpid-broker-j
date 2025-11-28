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
package org.apache.qpid.systests.jms_1_1.extensions.filters;

import org.apache.qpid.systests.JmsTestBase;
import org.junit.jupiter.api.Test;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DefaultFiltersTest extends JmsTestBase
{
    @Test
    public void defaultFilterIsApplied() throws Exception
    {
        String queueName = getTestName();
        Connection connection = getConnection();
        try
        {
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            createQueueWithDefaultFilter(queueName, "foo = 1");
            Queue queue = createQueue(queueName);

            final MessageProducer prod = session.createProducer(queue);
            Message message = session.createMessage();
            message.setIntProperty("foo", 0);
            prod.send(message);

            MessageConsumer cons = session.createConsumer(queue);

            assertNull(cons.receive(getReceiveTimeout()), "Message with foo=0 should not be received");

            message = session.createMessage();
            message.setIntProperty("foo", 1);
            prod.send(message);

            Message receivedMsg = cons.receive(getReceiveTimeout());
            assertNotNull(receivedMsg, "Message with foo=1 should be received");
            assertEquals(1, receivedMsg.getIntProperty("foo"), "Property foo not as expected");
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void defaultFilterIsOverridden() throws Exception
    {
        String queueName = getTestName();
        Connection connection = getConnection();
        try
        {
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            createQueueWithDefaultFilter(queueName, "foo = 1");
            Queue queue = createQueue(queueName);

            final MessageProducer prod = session.createProducer(queue);
            Message message = session.createMessage();
            message.setIntProperty("foo", 0);
            prod.send(message);

            MessageConsumer cons = session.createConsumer(queue, "foo = 0");

            Message receivedMsg = cons.receive(getReceiveTimeout());
            assertNotNull(receivedMsg, "Message with foo=0 should be received");
            assertEquals(0, receivedMsg.getIntProperty("foo"), "Property foo not as expected");

            message = session.createMessage();
            message.setIntProperty("foo", 1);
            prod.send( message);

            assertNull(cons.receive(getReceiveTimeout()), "Message with foo=1 should not be received");
        }
        finally
        {
            connection.close();
        }
    }

    private void createQueueWithDefaultFilter(String queueName, String selector) throws Exception
    {
        selector = selector.replace("\\", "\\\\");
        selector = selector.replace("\"", "\\\"");

        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue",
                                        Collections.singletonMap("defaultFilters", "{ \"x-filter-jms-selector\" : { \"x-filter-jms-selector\" : [ \"" + selector + "\" ] } }"));
    }

}
