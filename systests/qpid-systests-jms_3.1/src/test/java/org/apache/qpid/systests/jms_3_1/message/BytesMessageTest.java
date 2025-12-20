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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import jakarta.jms.BytesMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("message")
@Tag("queue")
class BytesMessageTest
{
    @Test
    void sendAndReceiveEmpty(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue))
        {
            final var message = session.createBytesMessage();
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(BytesMessage.class, receivedMessage, "BytesMessage should be received");
            assertEquals(0, ((BytesMessage) receivedMessage).getBodyLength(), "Unexpected body length");
        }
    }

    @Test
    void sendAndReceiveBody(final JmsSupport jms) throws Exception
    {
        final byte[] text = "euler".getBytes(StandardCharsets.US_ASCII);
        final double value = 2.71828;
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue))
        {
            final var message = session.createBytesMessage();
            message.writeBytes(text);
            message.writeDouble(value);
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(BytesMessage.class, receivedMessage, "BytesMessage should be received");

            final byte[] receivedBytes = new byte[text.length];

            final var receivedBytesMessage = (BytesMessage) receivedMessage;
            receivedBytesMessage.readBytes(receivedBytes);

            assertArrayEquals(receivedBytes, text, "Unexpected bytes");
            assertEquals(value, receivedBytesMessage.readDouble(), 0, "Unexpected double");
        }
    }

    /**
     * Verifies that a failed read of a {@code BytesMessage} does not advance the read pointer.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#conversions-provided-by-streammessage-and-mapmessage">3.11.3</a>
     * requires that if a read method of {@link BytesMessage} throws {@link MessageFormatException}
     * or {@link NumberFormatException}, the current position of the read pointer must not be
     * incremented.
     * <br>
     * This test sends a BytesMessage containing deliberately invalid UTF data (encoded as the
     * two-byte length followed by invalid bytes). It then calls {@link BytesMessage#readUTF()},
     * which must throw {@link MessageFormatException}. After the exception, the test reads the
     * raw bytes using other read methods and verifies that the read pointer was not advanced by
     * the failed {@code readUTF()} call.
     */
    @Test
    @Disabled("QPIDJMS-617: BytesMessage increments the read pointer position on MessageFormatException")
    void messageFormatExceptionDoesNotAdvanceReadPointer(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue);
             final var consumer = session.createConsumer(queue))
        {
            // Construct invalid modified-UTF data for readUTF():
            //  - the first two bytes are the unsigned short length
            //  - the following bytes are an invalid sequence (0xC0 0x00)
            final var message = session.createBytesMessage();
            message.writeShort((short) 2);
            message.writeByte((byte) 0xC0);
            message.writeByte((byte) 0x00);
            producer.send(message);

            connection.start();
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "BytesMessage should be received");
            assertInstanceOf(BytesMessage.class, receivedMessage, "BytesMessage should be received");

            final var bytesMessage = (BytesMessage) receivedMessage;
            assertThrows(MessageFormatException.class,
                    bytesMessage::readUTF,
                    "Expected MessageFormatException when reading invalid UTF from BytesMessage");

            // The read pointer must not have moved; the raw bytes should still be readable.
            assertEquals(2, bytesMessage.readUnsignedShort(), "Unexpected UTF length prefix");
            assertEquals((byte) 0xC0, bytesMessage.readByte(), "Unexpected first UTF data byte");
            assertEquals((byte) 0x00, bytesMessage.readByte(), "Unexpected second UTF data byte");

            assertThrows(MessageEOFException.class,
                    bytesMessage::readByte,
                    "Expected end-of-message after reading the remaining bytes");
        }
    }

    /**
     * Verifies that {@link BytesMessage} bodies can be obtained as a {@code byte[]} via
     * {@link Message#getBody(Class)} and that the returned byte array matches the payload sent.
     * <br>
     * Spec references:
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jakarta-messaging-message-body">3.11 "Jakarta Messaging message body"</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#a315-new-method-to-extract-the-body-directly-from-a-message-jms_spec-101">A.3.15 "New method to extract the body directly from a Message"</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#getBody(java.lang.Class)">Message.getBody(Class)</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#isBodyAssignableTo(java.lang.Class)">Message.isBodyAssignableTo(Class)</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">10.3 "Standard exceptions"</a>
     */
    @Test
    void bytesMessageGetBody(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue);
             final var consumer = session.createConsumer(queue))
        {
            final byte[] expectedBody = new byte[] { 1, 2, 3, 4, 5 };
            final var message = session.createBytesMessage();
            message.writeBytes(expectedBody);
            producer.send(message);

            connection.start();
            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");
            assertInstanceOf(BytesMessage.class, received, "BytesMessage should be received");

            assertTrue(received.isBodyAssignableTo(byte[].class),
                    "byte[] should be a compatible body type");
            assertArrayEquals(expectedBody, received.getBody(byte[].class), "Unexpected body");

            assertFalse(received.isBodyAssignableTo(String.class),
                    "String should be an incompatible body type");
            assertThrows(MessageFormatException.class, () -> received.getBody(String.class),
                    "Incompatible body type should cause MessageFormatException");
        }
    }
}
