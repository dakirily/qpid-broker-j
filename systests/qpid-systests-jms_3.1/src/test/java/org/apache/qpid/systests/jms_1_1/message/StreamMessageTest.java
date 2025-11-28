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
package org.apache.qpid.systests.jms_1_1.message;

import org.apache.qpid.systests.JmsTestBase;
import org.junit.jupiter.api.Test;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class StreamMessageTest extends JmsTestBase
{
    @Test
    public void testStreamMessageEOF() throws Exception
    {
        Queue queue = createQueue(getTestName());
        try (Connection consumerConnection = getConnection())
        {
            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);

            try (Connection producerConnection = getConnection())
            {
                Session producerSession = producerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                MessageProducer producer = producerSession.createProducer(queue);
                StreamMessage msg = producerSession.createStreamMessage();
                msg.writeByte((byte) 42);
                producer.send(msg);

                consumerConnection.start();

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertInstanceOf(StreamMessage.class, receivedMessage);
                StreamMessage streamMessage = (StreamMessage) receivedMessage;
                streamMessage.readByte();
                try {
                    streamMessage.readByte();
                    fail("Expected exception not thrown");
                } catch (Exception e) {
                    assertTrue(e instanceof MessageEOFException, "Expected MessageEOFException: " + e);
                }

                try {
                    streamMessage.writeByte((byte) 42);
                    fail("Expected exception not thrown");
                } catch (MessageNotWriteableException e) {
                    // pass
                }
            }
        }
    }

    @Test
    public void testModifyReceivedMessageContent() throws Exception
    {
        Queue queue = createQueue(getTestName());
        final CountDownLatch awaitMessages = new CountDownLatch(1);
        final AtomicReference<Throwable> listenerCaughtException = new AtomicReference<>();

        try (Connection consumerConnection = getConnection())
        {
            Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.close();

            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumer.setMessageListener(message -> {
                final StreamMessage sm = (StreamMessage) message;
                try {
                    sm.clearBody();
                    // it is legal to extend a stream message's content
                    sm.writeString("dfgjshfslfjshflsjfdlsjfhdsljkfhdsljkfhsd");
                } catch (Throwable t) {
                    listenerCaughtException.set(t);
                } finally {
                    awaitMessages.countDown();
                }
            });

            try (Connection producerConnection = getConnection())
            {
                Session producerSession = producerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                MessageProducer producer = producerSession.createProducer(queue);

                StreamMessage message = producerSession.createStreamMessage();
                message.writeInt(42);
                producer.send(message);

                consumerConnection.start();
                assertTrue(awaitMessages.await(getReceiveTimeout(), TimeUnit.SECONDS),
                        "Message did not arrive with consumer within a reasonable time");
                assertNull(listenerCaughtException.get(),
                        "No exception should be caught by listener : " + listenerCaughtException.get());
            }
        }
    }
}
