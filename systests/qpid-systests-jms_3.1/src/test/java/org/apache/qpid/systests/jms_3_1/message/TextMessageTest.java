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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("message")
@Tag("queue")
class TextMessageTest
{
    @Test
    void sendAndReceiveEmpty(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue))
        {
            final var message = session.createTextMessage(null);
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());

            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage, "TextMessage should be received");
            assertNull(textMessage.getText(), "Unexpected body");
        }
    }

    @Test
    void sendAndReceiveAsciiBody(final JmsSupport jms) throws Exception
    {
        sendAndReceiveBody(jms, "body");
    }

    @Test
    void sendAndReceiveNonAsciiUTF8Body(final JmsSupport jms) throws Exception
    {
        sendAndReceiveBody(jms, "YEN\u00A5EURO\u20AC");
    }

    private void sendAndReceiveBody(final JmsSupport jms, final String text) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue))
        {
            final var message = session.createTextMessage(text);
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage, "TextMessage should be received");

            assertEquals(textMessage.getText(), text, "Unexpected body");
        }
    }

    /**
     * Verifies that {@link TextMessage} bodies can be obtained via {@link Message#getBody(Class)}
     * and that the provider reports assignability correctly.
     *
     * The test also verifies that requesting an incompatible body type results in
     * {@link MessageFormatException}, as required by the API contract.
     *
     * Spec references:
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jakarta-messaging-message-body">3.11 "Jakarta Messaging message body"</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#a315-new-method-to-extract-the-body-directly-from-a-message-jms_spec-101">A.3.15 "New method to extract the body directly from a Message"</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#getBody(java.lang.Class)">Message.getBody(Class)</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#isBodyAssignableTo(java.lang.Class)">Message.isBodyAssignableTo(Class)</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">10.3 "Standard exceptions"</a>
     */
    @Test
    void textMessageGetBody(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue);
             final var consumer = session.createConsumer(queue))
        {
            final var expectedBody = "hello";
            producer.send(session.createTextMessage(expectedBody));

            connection.start();
            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");
            assertInstanceOf(TextMessage.class, received, "TextMessage should be received");

            assertTrue(received.isBodyAssignableTo(String.class),
                    "String should be a compatible body type");
            assertEquals(expectedBody, received.getBody(String.class), "Unexpected body");

            assertFalse(received.isBodyAssignableTo(byte[].class),
                    "byte[] should be an incompatible body type");
            assertThrows(MessageFormatException.class, () -> received.getBody(byte[].class),
                    "Incompatible body type should cause MessageFormatException");
        }
    }

}
