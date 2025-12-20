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

package org.apache.qpid.systests.jms_3_1.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("message")
@Tag("queue")
class JMSReplyToTest
{
    @Test
    void requestResponseUsingJmsReplyTo(final JmsSupport jms) throws Exception
    {
        final var requestQueue = jms.builder().queue().nameSuffix(".request").create();
        final var replyToQueue = jms.builder().queue().nameSuffix(".reply").create();

        try (final var connection = jms.builder().connection().create())
        {
            final AtomicReference<Throwable> exceptionHolder = createAsynchronousConsumer(connection, requestQueue);
            connection.start();

            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var replyConsumer = session.createConsumer(replyToQueue);
            final var requestMessage = session.createTextMessage("Request");
            requestMessage.setJMSReplyTo(replyToQueue);

            final var producer = session.createProducer(requestQueue);
            producer.send(requestMessage);

            final var responseMessage = replyConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(responseMessage, "Response message not received");
            assertEquals(responseMessage.getJMSCorrelationID(), requestMessage.getJMSMessageID(),
                    "Correlation id of the response should match message id of the request");
            assertNull(exceptionHolder.get(), "Unexpected exception in responder");
        }
    }

    @Test
    void requestResponseUsingTemporaryJmsReplyTo(final JmsSupport jms) throws Exception
    {
        final var requestQueue = jms.builder().queue().nameSuffix(".request").create();
        try (final var connection = jms.builder().connection().create())
        {
            final AtomicReference<Throwable> exceptionHolder = createAsynchronousConsumer(connection, requestQueue);
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var replyToQueue = session.createTemporaryQueue();
            final var replyConsumer = session.createConsumer(replyToQueue);
            final var requestMessage = session.createTextMessage("Request");
            requestMessage.setJMSReplyTo(replyToQueue);

            final var producer = session.createProducer(requestQueue);
            producer.send(requestMessage);

            final var responseMessage = replyConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(responseMessage, "Response message not received");
            assertEquals(responseMessage.getJMSCorrelationID(), requestMessage.getJMSMessageID(),
                    "Correlation id of the response should match message id of the request");
            assertNull(exceptionHolder.get(), "Unexpected exception in responder");
        }
    }

    private AtomicReference<Throwable> createAsynchronousConsumer(final Connection connection,
                                                                  final Queue requestQueue) throws JMSException
    {
        final var responderSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final AtomicReference<Throwable> caughtException = new AtomicReference<>();
        final var requestConsumer = responderSession.createConsumer(requestQueue);
        requestConsumer.setMessageListener(message ->
        {
            try
            {
                final var replyTo = message.getJMSReplyTo();
                final var responseProducer = responderSession.createProducer(replyTo);
                final var responseMessage = responderSession.createMessage();
                responseMessage.setJMSCorrelationID(message.getJMSMessageID());
                responseProducer.send(responseMessage);
            }
            catch (Throwable t)
            {
                caughtException.set(t);
            }
        });
        return caughtException;
    }
}
