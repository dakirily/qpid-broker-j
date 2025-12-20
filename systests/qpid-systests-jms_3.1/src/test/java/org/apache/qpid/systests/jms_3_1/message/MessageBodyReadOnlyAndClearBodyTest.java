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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.BytesMessage;
import jakarta.jms.MapMessage;
import jakarta.jms.MessageNotReadableException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * Tests that cover message-body writability rules for received messages and {@code Message.clearBody()} behaviour.
 * <p>
 * These tests validate the semantics defined in the Jakarta Messaging 3.1 specification:
 * <ul>
 *     <li><a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#changing-the-value-of-a-received-message">
 *         Changing the value of a received message</a></li>
 *     <li><a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jakarta-messaging-message-body">
 *         Jakarta Messaging message body</a></li>
 *     <li><a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#clearing-a-message-body">
 *         Clearing a message body</a></li>
 *     <li><a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#read-only-message-body">
 *         Read-only message body</a></li>
 *     <li><a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">
 *         Standard exceptions</a></li>
 * </ul>
 */
@JmsSystemTest
@Tag("message")
@Tag("queue")
class MessageBodyReadOnlyAndClearBodyTest
{
    /**
     * Verifies that the body of a received {@link TextMessage} is read-only and becomes writable only after
     * calling {@code clearBody()}, which also resets the body to its empty initial value.
     *
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#read-only-message-body">Read-only message body</a>
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#clearing-a-message-body">Clearing a message body</a>
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">Standard exceptions</a>
     */
    @Test
    void receivedTextMessageBodyIsReadOnlyUntilClearBody(final JmsSupport jms) throws Exception
    {
        final var sourceQueue = jms.builder().queue().create();
        final var targetQueue = jms.builder().queue("targetQueue").create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(sourceQueue);
            final var message = session.createTextMessage("original");
            producer.send(message);

            final var consumer = session.createConsumer(sourceQueue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage, "TextMessage should be received");

            assertEquals("original", textMessage.getText(), "Unexpected received text");

            assertThrows(MessageNotWriteableException.class,
                    () -> textMessage.setText("should fail"),
                    "A received message body must be read-only");

            // clearBody() resets the body to its empty initial value and makes it writable again.
            textMessage.clearBody();
            assertNull(textMessage.getText(), "TextMessage body should be empty after clearBody()");
            assertDoesNotThrow(() -> textMessage.setText("modified"), "Message body should be writable after clearBody()");

            // Resend the modified received message and validate the new body value on the consumer side.
            final var resendProducer = session.createProducer(targetQueue);
            resendProducer.send(textMessage);

            final var targetConsumer = session.createConsumer(targetQueue);
            final var resentMessage = targetConsumer.receive(Timeouts.receiveMillis());
            final var textMessageResent = assertInstanceOf(TextMessage.class, resentMessage, "TextMessage should be received");
            assertEquals("modified", textMessageResent.getText(), "Unexpected resent text");
        }
    }

    /**
     * Verifies that the body of a received {@link MapMessage} is read-only and becomes writable only after
     * calling {@code clearBody()}, which also resets the body to its empty initial value.
     *
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#read-only-message-body">Read-only message body</a>
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#clearing-a-message-body">Clearing a message body</a>
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">Standard exceptions</a>
     */
    @Test
    void receivedMapMessageBodyIsReadOnlyUntilClearBody(final JmsSupport jms) throws Exception
    {
        final var sourceQueue = jms.builder().queue().create();
        final var targetQueue = jms.builder().queue("targetQueue").create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(sourceQueue);
            final var message = session.createMapMessage();
            message.setString("key", "original");
            producer.send(message);

            final var consumer = session.createConsumer(sourceQueue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(MapMessage.class, receivedMessage, "MapMessage should be received");

            final var mapMessage = (MapMessage) receivedMessage;
            assertEquals("original", mapMessage.getString("key"), "Unexpected received map value");

            assertThrows(MessageNotWriteableException.class,
                    () -> mapMessage.setString("key", "should fail"),
                    "A received message body must be read-only");

            // clearBody() resets the map to its empty initial value and makes it writable again.
            mapMessage.clearBody();
            assertFalse(mapMessage.getMapNames().hasMoreElements(),
                    "MapMessage body should be empty after clearBody()");
            assertDoesNotThrow(() -> mapMessage.setString("key", "modified"),
                    "Message body should be writable after clearBody()");

            // Resend the modified received message and validate the new body value on the consumer side.
            final var resendProducer = session.createProducer(targetQueue);
            resendProducer.send(mapMessage);

            final var targetConsumer = session.createConsumer(targetQueue);
            final var resentMessage = targetConsumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(MapMessage.class, resentMessage, "MapMessage should be received");
            assertEquals("modified", ((MapMessage) resentMessage).getString("key"), "Unexpected resent map value");
        }
    }

    /**
     * Verifies that the body of a received {@link BytesMessage} is read-only, and that {@code clearBody()} resets
     * the body to its empty initial value and returns it to the same state as a newly created {@link BytesMessage}
     * (i.e. write-only).
     *
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#read-only-message-body">Read-only message body</a>
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#clearing-a-message-body">Clearing a message body</a>
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">Standard exceptions</a>
     */
    @Test
    void receivedBytesMessageBodyIsReadOnlyUntilClearBody(final JmsSupport jms) throws Exception
    {
        final var sourceQueue = jms.builder().queue().create();
        final var targetQueue = jms.builder().queue("targetQueue").create();

        final byte[] original = new byte[] { 1, 2, 3, 4 };
        final byte[] modified = new byte[] { 9, 8, 7 };

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(sourceQueue);
            final var message = session.createBytesMessage();
            message.writeBytes(original);
            producer.send(message);

            final var consumer = session.createConsumer(sourceQueue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(BytesMessage.class, receivedMessage, "BytesMessage should be received");

            final var bytesMessage = (BytesMessage) receivedMessage;
            assertEquals(original.length, bytesMessage.getBodyLength(), "Unexpected received body length");

            assertThrows(MessageNotWriteableException.class,
                    () -> bytesMessage.writeByte((byte) 42),
                    "A received message body must be read-only");

            // clearBody() resets the body to its empty initial value and returns the BytesMessage to write-only state.
            bytesMessage.clearBody();
            assertThrows(MessageNotReadableException.class,
                    bytesMessage::readByte,
                    "A cleared BytesMessage should be in write-only state");

            bytesMessage.writeBytes(modified);

            // Resend the modified received message and validate the new body value on the consumer side.
            final var resendProducer = session.createProducer(targetQueue);
            resendProducer.send(bytesMessage);

            final var targetConsumer = session.createConsumer(targetQueue);
            final var resentMessage = targetConsumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(BytesMessage.class, resentMessage, "BytesMessage should be received");

            final var resentBytesMessage = (BytesMessage) resentMessage;
            assertEquals(modified.length, resentBytesMessage.getBodyLength(), "Unexpected resent body length");

            final byte[] buffer = new byte[modified.length];
            final int read = resentBytesMessage.readBytes(buffer);
            assertEquals(modified.length, read, "Unexpected number of bytes read");
            assertArrayEquals(modified, buffer, "Unexpected resent bytes content");
        }
    }

    /**
     * Verifies that if a consumer modifies the body of a received message after calling {@code clearBody()}, then
     * a subsequent redelivery of that message must deliver the original, unmodified message body.
     *
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#changing-the-value-of-a-received-message">Changing the value of a received message</a>
     * @see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#clearing-a-message-body">Clearing a message body</a>
     */
    @Test
    void redeliveryMustReturnOriginalMessageBody(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            producer.send(session.createTextMessage("original"));

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var firstDelivery = consumer.receive(Timeouts.receiveMillis());
            final var received = assertInstanceOf(TextMessage.class, firstDelivery, "TextMessage should be received");

            assertEquals("original", received.getText(), "Unexpected received text");

            // Modify the received message (legal after clearBody).
            received.clearBody();
            received.setText("modified");

            // Force redelivery of the unacknowledged message.
            session.recover();

            final var redelivery = consumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, redelivery, "TextMessage should be received");
            assertEquals("original", textMessage.getText(),
                    "Redelivered message must be the original, unmodified message");

            // Acknowledge to clean up the queue.
            redelivery.acknowledge();
        }
    }
}
