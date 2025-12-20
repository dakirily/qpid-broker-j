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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Message;
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("message")
@Tag("queue")
class StreamMessageTest
{
    @Test
    void streamMessageEOF(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var consumerConnection = jms.builder().connection().create();
             final var consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
             final var consumer = consumerSession.createConsumer(queue))
        {
            try (final var producerConnection = jms.builder().connection().create();
                 final var producerSession = producerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                 final var producer = producerSession.createProducer(queue))
            {
                final var msg = producerSession.createStreamMessage();
                msg.writeByte((byte) 42);
                producer.send(msg);

                consumerConnection.start();

                final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
                assertInstanceOf(StreamMessage.class, receivedMessage);
                final var streamMessage = (StreamMessage) receivedMessage;
                streamMessage.readByte();

                assertThrows(MessageEOFException.class,
                        streamMessage::readByte,
                        "Expected exception not thrown");

                assertThrows(MessageNotWriteableException.class,
                        () -> streamMessage.writeByte((byte) 42),
                        "Expected exception not thrown");
            }
        }
    }

    @Test
    void modifyReceivedMessageContent(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final var awaitMessages = new CountDownLatch(1);
        final AtomicReference<Throwable> listenerCaughtException = new AtomicReference<>();

        try (final var consumerConnection = jms.builder().connection().create();
             final var session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            session.close();

            final var consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            final var consumer = consumerSession.createConsumer(queue);
            consumer.setMessageListener(message ->
            {
                final StreamMessage sm = (StreamMessage) message;
                try
                {
                    sm.clearBody();
                    // it is legal to extend a stream message's content
                    sm.writeString("dfgjshfslfjshflsjfdlsjfhdsljkfhdsljkfhsd");
                }
                catch (Throwable t)
                {
                    listenerCaughtException.set(t);
                }
                finally
                {
                    awaitMessages.countDown();
                }
            });

            try (final var producerConnection = jms.builder().connection().create();
                 final var producerSession = producerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                 final var producer = producerSession.createProducer(queue))
            {
                final var message = producerSession.createStreamMessage();
                message.writeInt(42);
                producer.send(message);

                consumerConnection.start();
                assertTrue(awaitMessages.await(Timeouts.receiveMillis(), TimeUnit.SECONDS),
                        "Message did not arrive with consumer within a reasonable time");
                assertNull(listenerCaughtException.get(),
                        "No exception should be caught by listener : " + listenerCaughtException.get());
            }
        }
    }

    /**
     * Verifies that a failed String-to-numeric conversion throws {@link NumberFormatException}
     * and does not advance the read pointer.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#conversions-provided-by-streammessage-and-mapmessage">3.11.3</a>
     * requires String-to-numeric conversions to throw {@link NumberFormatException} when the
     * String value is not accepted as a valid numeric representation. It also requires that,
     * if a read method throws {@link MessageFormatException} or {@link NumberFormatException},
     * the current read pointer position must not be incremented.
     * <br>
     * This test writes a String followed by an int, attempts to read the String as an int,
     * and then verifies that the String can still be read correctly, followed by the int.
     */
    @Test
    void numberFormatExceptionDoesNotAdvanceReadPointer(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue);
             final var consumer = session.createConsumer(queue))
        {
            final var message = session.createStreamMessage();
            message.writeString("notANumber");
            message.writeInt(7);
            producer.send(message);

            connection.start();
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "StreamMessage should be received");
            assertInstanceOf(StreamMessage.class, receivedMessage, "StreamMessage should be received");

            final var streamMessage = (StreamMessage) receivedMessage;
            assertThrows(NumberFormatException.class,
                    streamMessage::readInt,
                    "Expected NumberFormatException when converting invalid String to int");

            // The read pointer must not have moved; the String should still be readable.
            assertEquals("notANumber", streamMessage.readString(), "Unexpected first stream element");
            assertEquals(7, streamMessage.readInt(), "Unexpected second stream element");
        }
    }

    /**
     * Verifies that a type mismatch throws {@link MessageFormatException} and does not advance
     * the read pointer.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#conversions-provided-by-streammessage-and-mapmessage">3.11.3</a>
     * defines the supported type conversions for StreamMessage and requires unsupported conversions
     * to throw {@link MessageFormatException}. It also requires that, if a read method throws
     * {@link MessageFormatException} or {@link NumberFormatException}, the current read pointer
     * position must not be incremented.
     * <br>
     * This test writes a boolean followed by an int, attempts to read the boolean as an int,
     * and then verifies that the boolean can still be read correctly, followed by the int.
     */
    @Test
    void messageFormatExceptionDoesNotAdvanceReadPointer(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue);
             final var consumer = session.createConsumer(queue))
        {
            final var message = session.createStreamMessage();
            message.writeBoolean(true);
            message.writeInt(7);
            producer.send(message);

            connection.start();
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "StreamMessage should be received");
            assertInstanceOf(StreamMessage.class, receivedMessage, "StreamMessage should be received");

            final var streamMessage = (StreamMessage) receivedMessage;
            assertThrows(MessageFormatException.class,
                    streamMessage::readInt,
                    "Expected MessageFormatException when reading boolean as int");

            // The read pointer must not have moved; the boolean should still be readable.
            assertTrue(streamMessage.readBoolean(), "Unexpected first stream element");
            assertEquals(7, streamMessage.readInt(), "Unexpected second stream element");
        }
    }

    /**
     * Verifies that {@link StreamMessage} does not support {@link Message#getBody(Class)}.
     * <br>
     * StreamMessage provides a cursor-based body API ({@code readXXX}) and cannot be exposed
     * as a single typed body object. The API contract therefore requires
     * {@link MessageFormatException} to be thrown if {@code getBody(...)} is called.
     * <br>
     * Spec references:
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jakarta-messaging-message-body">3.11 "Jakarta Messaging message body"</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#getBody(java.lang.Class)">Message.getBody(Class)</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#isBodyAssignableTo(java.lang.Class)">Message.isBodyAssignableTo(Class)</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">10.3 "Standard exceptions"</a>
     */
    @Test
    void streamMessageGetBodyIsNotSupported(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue);
             final var consumer = session.createConsumer(queue))
        {
            final var message = session.createStreamMessage();
            message.writeString("a");
            message.writeInt(1);
            producer.send(message);

            connection.start();
            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");
            assertInstanceOf(StreamMessage.class, received, "StreamMessage should be received");

            assertFalse(received.isBodyAssignableTo(String.class),
                    "StreamMessage should not report any supported body type");
            assertThrows(MessageFormatException.class, () -> received.getBody(String.class),
                    "StreamMessage should not allow getBody(...) access");
        }
    }
}