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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * System tests covering support for {@link Message#setJMSCorrelationIDAsBytes(byte[])} and
 * {@link Message#getJMSCorrelationIDAsBytes()}.
 *
 * Jakarta Messaging allows {@code JMSCorrelationID} to carry a provider-native {@code byte[]}
 * value in order to interoperate with non-Jakarta Messaging clients, but also explicitly states
 * that support for {@code byte[]} correlation IDs is optional and non-portable.
 *
 * The tests in this class are aligned with:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-header-fields">3.4 "Message header fields"</a>
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jmscorrelationid">3.4.5 "JMSCorrelationID"</a>
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#how-message-header-values-are-set">3.4.11 "How message header values are set"</a>
 *
 * Additionally, the tests validate API-level requirements documented on
 * <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message">jakarta.jms.Message</a>,
 * including the copying semantics of {@link Message#setJMSCorrelationIDAsBytes(byte[])}.
 */
@JmsSystemTest
@Tag("message")
@Tag("queue")
class JMSCorrelationIDAsBytesTest
{
    /**
     * Verifies that {@link Message#setJMSCorrelationIDAsBytes(byte[])} copies the provided array.
     *
     * The Jakarta Messaging API specifies that the input array is copied before the method
     * returns, so that subsequent changes to the original array do not alter the message header.
     * See {@link Message#setJMSCorrelationIDAsBytes(byte[])} in the API documentation:
     * <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#setJMSCorrelationIDAsBytes(byte%5B%5D)">setJMSCorrelationIDAsBytes</a>.
     *
     * This test is also consistent with the definition of {@code JMSCorrelationID} in section
     * <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jmscorrelationid">3.4.5</a>,
     * which allows {@code JMSCorrelationID} to contain a provider-native {@code byte[]} value.
     */
    @Test
    void correlationIdAsBytesIsCopiedOnSet(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var message = session.createMessage();

            final byte[] correlationId = new byte[] { 1, 2, 3, 4, 5 };
            final boolean supported = trySetCorrelationIdAsBytes(message, correlationId);
            assumeTrue(supported, "Provider does not support byte[] JMSCorrelationID");

            // Modify the original array. The message header must remain unchanged.
            correlationId[0] = 9;

            final var readBack = message.getJMSCorrelationIDAsBytes();
            assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, readBack,
                    "The correlation ID bytes should be copied, not referenced");
        }
    }

    /**
     * Verifies that a {@code byte[]} {@code JMSCorrelationID} set by the client is preserved
     * across send and receive.
     *
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#how-message-header-values-are-set">3.4.11</a>
     * lists {@code JMSCorrelationID} as a header field set by the client application, and section
     * <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-header-fields">3.4</a>
     * states that a message's complete header is transmitted to Jakarta Messaging clients that
     * receive the message.
     *
     * The representation as a provider-native {@code byte[]} is described in
     * section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jmscorrelationid">3.4.5</a>.
     */
    @Test
    void correlationIdAsBytesIsTransmittedToConsumer(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue);
             final var consumer = session.createConsumer(queue))
        {
            final byte[] expectedCorrelationId = new byte[] { 10, 11, 12, 13, 14, 15 };

            final var message = session.createTextMessage("payload");
            final boolean supported = trySetCorrelationIdAsBytes(message, expectedCorrelationId);
            assumeTrue(supported, "Provider does not support byte[] JMSCorrelationID");

            producer.send(message);

            connection.start();
            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");
            assertInstanceOf(TextMessage.class, received, "TextMessage should be received");

            assertArrayEquals(expectedCorrelationId, received.getJMSCorrelationIDAsBytes(),
                    "JMSCorrelationID (byte[]) should be preserved across send/receive");
        }
    }

    private boolean trySetCorrelationIdAsBytes(final Message message, final byte[] correlationId) throws Exception
    {
        try
        {
            message.setJMSCorrelationIDAsBytes(correlationId);
            // Some providers may also throw UnsupportedOperationException from the corresponding get method.
            message.getJMSCorrelationIDAsBytes();
            return true;
        }
        catch (UnsupportedOperationException e)
        {
            // Section 3.4.5 allows providers without native correlation IDs to omit support for byte[].
            return false;
        }
    }
}
