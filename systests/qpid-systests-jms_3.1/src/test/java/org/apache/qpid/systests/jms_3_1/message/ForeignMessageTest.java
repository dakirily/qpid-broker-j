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

package org.apache.qpid.systests.jms_3_1.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.lang.reflect.Proxy;
import java.util.UUID;

import jakarta.jms.Message;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("message")
@Tag("queue")
class ForeignMessageTest
{
    @Test
    void sendForeignMessage(final JmsSupport jms) throws Exception
    {
        final var replyTo = jms.builder().queue().nameSuffix("_replyTo").create();
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var jmsType = "TestJmsType";
            final var correlationId = "testCorrelationId";
            final var message = session.createObjectMessage();
            final var foreignMessage = ObjectMessage.class.cast(Proxy.newProxyInstance(
                    ObjectMessage.class.getClassLoader(), new Class<?>[] { ObjectMessage.class },
                    (proxy, method, args) -> method.invoke(message, args)));

            foreignMessage.setJMSCorrelationID(correlationId);
            foreignMessage.setJMSType(jmsType);
            foreignMessage.setJMSReplyTo(replyTo);
            final var payload = UUID.randomUUID();
            foreignMessage.setObject(payload);

            final var consumer = session.createConsumer(queue);
            final var producer = session.createProducer(queue);
            producer.send(foreignMessage);

            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage was not received ");
            assertEquals(foreignMessage.getJMSCorrelationID(), receivedMessage.getJMSCorrelationID(),
                    "JMSCorrelationID mismatch");
            assertEquals(foreignMessage.getJMSType(), receivedMessage.getJMSType(), "JMSType mismatch");
            assertEquals(foreignMessage.getJMSReplyTo(), receivedMessage.getJMSReplyTo(),
                    "JMSReply To mismatch");
            assertEquals(foreignMessage.getJMSMessageID(), receivedMessage.getJMSMessageID(),
                    "JMSMessageID mismatch");
            assertEquals(Message.DEFAULT_PRIORITY, receivedMessage.getJMSPriority(),
                    "JMS Default priority should be default");
            assertEquals(payload, ((ObjectMessage) receivedMessage).getObject(),
                    "Message payload not as expected");
        }
    }
}
