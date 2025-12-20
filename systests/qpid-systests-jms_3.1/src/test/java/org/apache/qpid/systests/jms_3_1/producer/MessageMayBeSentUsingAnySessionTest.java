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

package org.apache.qpid.systests.jms_3_1.producer;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * System tests verifying that a {@link Message} created using one {@link Session}
 * may be sent using a {@link jakarta.jms.MessageProducer} created from a different {@link Session}.
 * <p>
 * The Jakarta Messaging specification explicitly states that message objects are not bound
 * to the session used to create them:
 * <ul>
 *     <li>
 *         <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#optimized-message-implementations">
 *             6.2.4 "Optimized message implementations"
 *         </a>
 *     </li>
 *     <li>
 *         <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#provider-implementations-of-jakarta-messaging-message-interfaces">
 *             3.12 "Provider implementations of Jakarta Messaging message interfaces"
 *         </a>
 *     </li>
 *     <li>
 *         <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#clarification-message-may-be-sent-using-any-session-jms_spec-52">
 *             A.3.17 "Clarification: message may be sent using any session (JMS_SPEC-52)"
 *         </a>
 *     </li>
 * </ul>
 */
@JmsSystemTest
@Tag("message")
@Tag("queue")
class MessageMayBeSentUsingAnySessionTest
{
    private static final String PROPERTY_NAME = "testProperty";
    private static final String PROPERTY_VALUE = "testValue";

    /**
     * Verifies that a {@link jakarta.jms.TextMessage} created using one {@link Session}
     * can be sent by a {@link jakarta.jms.MessageProducer} created from a different {@link Session}.
     * <p>
     * This is required by:
     * <ul>
     *     <li>
     *         <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#optimized-message-implementations">
     *             6.2.4 "Optimized message implementations"
     *         </a>
     *     </li>
     *     <li>
     *         <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#producers">
     *             7.1 "Producers"
     *         </a>
     *     </li>
     *     <li>
     *         <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#synchronous-send">
     *             7.2 "Synchronous send"
     *         </a>
     *     </li>
     * </ul>
     */
    @Test
    void textMessageCreatedByOneSessionMayBeSentUsingAnotherSession(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (var connection = jms.builder().connection().create();
             var messageSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var sendingSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = sendingSession.createProducer(queue);
             var consumer = sendingSession.createConsumer(queue))
        {
            final String expectedBody = "Hello";

            final var message = messageSession.createTextMessage(expectedBody);
            message.setStringProperty(PROPERTY_NAME, PROPERTY_VALUE);

            // The message must not be tied to the session that created it.
            messageSession.close();

            producer.send(message);

            connection.start();
            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message not received");
            final var textMessage = assertInstanceOf(TextMessage.class, received, "Unexpected message type");
            assertEquals(expectedBody, textMessage.getText());
            assertEquals(PROPERTY_VALUE, received.getStringProperty(PROPERTY_NAME));
        }
    }

    /**
     * Verifies that a {@link MapMessage} created using one {@link Session}
     * can be sent by a {@link jakarta.jms.MessageProducer} created from a different {@link Session}.
     * <p>
     * In addition to the requirement that messages may be sent using any session
     * (see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#optimized-message-implementations">6.2.4</a>),
     * this test exercises the requirement that a provider must be able to handle messages
     * regardless of how the message object is implemented (see
     * <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#provider-implementations-of-jakarta-messaging-message-interfaces">3.12</a>).
     * <p>
     * The message is sent using a {@link jakarta.jms.MessageProducer} (see
     * <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#producers">7.1</a>).
     */
    @Test
    void mapMessageCreatedByOneSessionMayBeSentUsingAnotherSession(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (var connection = jms.builder().connection().create();
             var messageSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var sendingSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = sendingSession.createProducer(queue);
             var consumer = sendingSession.createConsumer(queue))
        {
            final var mapMessage = messageSession.createMapMessage();
            mapMessage.setString("key", "value");
            mapMessage.setInt("intKey", 123);
            mapMessage.setStringProperty(PROPERTY_NAME, PROPERTY_VALUE);

            // The message must not be tied to the session that created it.
            messageSession.close();

            producer.send(mapMessage);

            connection.start();
            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message not received");
            final var receivedMapMessage = (MapMessage) received;
            assertEquals("value", receivedMapMessage.getString("key"));
            assertEquals(123, receivedMapMessage.getInt("intKey"));
            assertEquals(PROPERTY_VALUE, receivedMapMessage.getStringProperty(PROPERTY_NAME));
        }
    }
}
