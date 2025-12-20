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

package org.apache.qpid.systests.jms_3_1.context;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.Queue;

import static jakarta.jms.JMSContext.AUTO_ACKNOWLEDGE;
import static jakarta.jms.JMSContext.CLIENT_ACKNOWLEDGE;
import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies {@link JMSContext} acknowledgement and recovery semantics in {@link JMSContext#CLIENT_ACKNOWLEDGE} mode.
 *
 * <p>The simplified API provides {@link JMSContext#acknowledge()} and {@link JMSContext#recover()} methods which apply
 * to the underlying session owned by the {@link JMSContext}.</p>
 *
 * <p>Specification:</p>
 * <ul>
 *   <li>
 *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#simplified-api-interfaces">
 *       <strong>Simplified API interfaces</strong>
 *     </a>
 *   </li>
 *   <li>
 *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-acknowledgment">
 *       <strong>Message acknowledgment</strong>
 *     </a>
 *   </li>
 *   <li>
 *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-acknowledgment-1">
 *       <strong>Message acknowledgment (Sessions)</strong>
 *     </a>
 *   </li>
 * </ul>
 */
@JmsSystemTest
@Tag("queue")
class JMSContextClientAcknowledgeAndRecoverTest
{
    /**
     * Verifies that {@link JMSContext#acknowledge()} acknowledges all messages which have been delivered by the
     * underlying session <em>up to the point that acknowledge is called</em>. Any messages delivered after that call
     * remain unacknowledged until a subsequent acknowledge.
     *
     * <p>The test:</p>
     * <ul>
     *   <li>sends two messages to a queue;</li>
     *   <li>receives the first message and calls {@link JMSContext#acknowledge()};</li>
     *   <li>receives the second message but does not acknowledge it and closes the context;</li>
     *   <li>verifies that only the second message is redelivered to a new consumer.</li>
     * </ul>
     *
     * <p>Specification:</p>
     * <ul>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-acknowledgment-1">
     *       <strong>Message acknowledgment (CLIENT_ACKNOWLEDGE)</strong>
     *     </a>
     *   </li>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#closing-a-session">
     *       <strong>Closing a session</strong>
     *     </a>
     *     (closing a client-acknowledged session does not force an acknowledge)
     *   </li>
     * </ul>
     */
    @Test
    void acknowledgeAcknowledgesAllMessagesDeliveredSoFar(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();
        final ConnectionFactory cf = jms.builder().connection().connectionFactory();

        // Send two identifiable messages.
        try (final JMSContext sendingContext = cf.createContext(AUTO_ACKNOWLEDGE))
        {
            final JMSProducer producer = sendingContext.createProducer();

            final Message message0 = sendingContext.createTextMessage("m0");
            message0.setIntProperty(INDEX, 0);
            producer.send(queue, message0);

            final Message message1 = sendingContext.createTextMessage("m1");
            message1.setIntProperty(INDEX, 1);
            producer.send(queue, message1);
        }

        final String secondDeliveryId;
        try (final JMSContext clientAckContext = cf.createContext(CLIENT_ACKNOWLEDGE))
        {
            final JMSConsumer consumer = clientAckContext.createConsumer(queue);

            final Message firstDelivery = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(firstDelivery, "Message was not received");
            assertEquals(0, firstDelivery.getIntProperty(INDEX), "Unexpected first message");
            assertEquals(1, firstDelivery.getIntProperty("JMSXDeliveryCount"));

            // Acknowledge all messages delivered so far (i.e. firstDelivery).
            clientAckContext.acknowledge();

            final Message secondDelivery = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(secondDelivery, "Second message was not received");
            assertEquals(1, secondDelivery.getIntProperty(INDEX), "Unexpected second message");
            assertEquals(1, secondDelivery.getIntProperty("JMSXDeliveryCount"));

            secondDeliveryId = secondDelivery.getJMSMessageID();
        }

        // The second message should be redelivered, the first should not.
        try (final JMSContext verificationContext = cf.createContext(AUTO_ACKNOWLEDGE))
        {
            final JMSConsumer consumer = verificationContext.createConsumer(queue);

            final Message redelivered = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered, "Expected an unacknowledged message to be redelivered");
            assertEquals(1, redelivered.getIntProperty(INDEX), "Expected only the second message to be redelivered");

            // Redelivery should keep the same JMSMessageID.
            assertEquals(secondDeliveryId, redelivered.getJMSMessageID(), "Redelivered message must have the same JMSMessageID");

            // Redelivery due to an unacknowledged delivery should increment JMSXDeliveryCount and set JMSRedelivered.
            assertTrue(redelivered.getIntProperty("JMSXDeliveryCount") >= 2, "JMSXDeliveryCount should be incremented on redelivery");
            assertTrue(redelivered.getJMSRedelivered(), "JMSRedelivered should be true for a redelivered message");

            assertNull(consumer.receive(Timeouts.noMessagesMillis()), "No more messages expected");
        }
    }

    /**
     * Verifies that {@link JMSContext#recover()} restarts message delivery with the first unacknowledged message and
     * that the message is redelivered with {@code JMSRedelivered=true} and an incremented {@code JMSXDeliveryCount}.
     *
     * <p>Specification:</p>
     * <ul>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-acknowledgment-1">
     *       <strong>Message acknowledgment (recover)</strong>
     *     </a>
     *   </li>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jmsredelivered">
     *       <strong>JMSRedelivered</strong>
     *     </a>
     *   </li>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jmsxdeliverycount">
     *       <strong>JMSXDeliveryCount</strong>
     *     </a>
     *   </li>
     * </ul>
     */
    @Test
    void recoverRedeliversFirstUnacknowledgedMessage(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();
        final ConnectionFactory cf = jms.builder().connection().connectionFactory();

        // Send a single message.
        try (final JMSContext sendingContext = cf.createContext(AUTO_ACKNOWLEDGE))
        {
            sendingContext.createProducer().send(queue, "payload");
        }

        try (final JMSContext clientAckContext = cf.createContext(CLIENT_ACKNOWLEDGE))
        {
            final JMSConsumer consumer = clientAckContext.createConsumer(queue);

            final Message firstDelivery = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(firstDelivery, "Message was not received before recover");

            final String messageId = firstDelivery.getJMSMessageID();

            assertEquals(1, firstDelivery.getIntProperty("JMSXDeliveryCount"));
            assertFalse(firstDelivery.getJMSRedelivered(), "First delivery must not be marked as redelivered");

            // Restart delivery from the first unacknowledged message.
            clientAckContext.recover();

            final Message redelivered = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered, "Message was not redelivered after recover");

            assertEquals(messageId, redelivered.getJMSMessageID(), "Redelivered message must have the same JMSMessageID");
            assertTrue(redelivered.getJMSRedelivered(), "JMSRedelivered should be true after recover");
            assertEquals(2, redelivered.getIntProperty("JMSXDeliveryCount"), "JMSXDeliveryCount should be incremented after recover");

            // Acknowledge to ensure the message is removed from the queue.
            clientAckContext.acknowledge();

            assertNull(consumer.receive(Timeouts.noMessagesMillis()), "No further deliveries expected after acknowledgement");
        }
    }
}
