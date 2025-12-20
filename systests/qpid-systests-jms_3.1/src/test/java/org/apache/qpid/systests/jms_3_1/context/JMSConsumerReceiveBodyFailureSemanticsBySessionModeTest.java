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

import static jakarta.jms.JMSContext.AUTO_ACKNOWLEDGE;
import static jakarta.jms.JMSContext.CLIENT_ACKNOWLEDGE;
import static jakarta.jms.JMSContext.DUPS_OK_ACKNOWLEDGE;
import static jakarta.jms.JMSContext.SESSION_TRANSACTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.Queue;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * System tests covering the failure semantics of {@link JMSConsumer#receiveBody(Class, long)} when it throws
 * {@link MessageFormatRuntimeException}.
 *
 * <p>Section
 * <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#receiving-message-bodies-synchronously">8.6
 * "Receiving message bodies synchronously"</a> defines that the effect of a failed receiveBody call depends on the
 * acknowledgement/transaction mode of the underlying session:</p>
 *
 * <ul>
 *   <li>
 *     In {@link JMSContext#AUTO_ACKNOWLEDGE} and {@link JMSContext#DUPS_OK_ACKNOWLEDGE} modes, the provider must behave
 *     <em>as if the unsuccessful call had not occurred</em>, and the message must be delivered again (before any
 *     subsequent messages) without being considered a redelivery.
 *   </li>
 *   <li>
 *     In {@link JMSContext#CLIENT_ACKNOWLEDGE} mode, the provider must behave <em>as if the call had succeeded</em> and
 *     the message must not be delivered again unless {@link JMSContext#recover()} is called.
 *   </li>
 *   <li>
 *     In {@link JMSContext#SESSION_TRANSACTED} mode, the provider must behave <em>as if the call had succeeded</em> and
 *     the local transaction remains uncommitted until {@link JMSContext#commit()} or {@link JMSContext#rollback()}.
 *   </li>
 * </ul>
 *
 * <p>When a message is redelivered due to {@link JMSContext#recover()} or {@link JMSContext#rollback()}, the provider
 * must set {@code JMSRedelivered=true} and increment {@code JMSXDeliveryCount}.</p>
 *
 * <p>Specification:</p>
 * <ul>
 *   <li>
 *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#receiving-message-bodies-synchronously">
 *       <strong>Receiving message bodies synchronously</strong>
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
 *   <li>
 *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#transactions">
 *       <strong>Transactions</strong>
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
@JmsSystemTest
@Tag("queue")
@Tag("message")
@Tag("simplified-api")
@Tag("acknowledge")
@Tag("transactions")
class JMSConsumerReceiveBodyFailureSemanticsBySessionModeTest
{
    /**
     * Verifies the failure semantics for {@link JMSContext#AUTO_ACKNOWLEDGE} and {@link JMSContext#DUPS_OK_ACKNOWLEDGE}
     * when {@link JMSConsumer#receiveBody(Class, long)} throws {@link MessageFormatRuntimeException}.
     *
     * <p>The provider must behave as if the unsuccessful call had not occurred. The failed message must be delivered
     * again before any subsequent messages and must <em>not</em> be marked as redelivered (no {@code JMSRedelivered}
     * and no increment of {@code JMSXDeliveryCount}).</p>
     *
     * <p>Specification:</p>
     * <ul>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#receiving-message-bodies-synchronously">
     *       <strong>Receiving message bodies synchronously</strong>
     *     </a>
     *   </li>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-acknowledgment-1">
     *       <strong>Message acknowledgment (AUTO_ACKNOWLEDGE, DUPS_OK_ACKNOWLEDGE)</strong>
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
    @ParameterizedTest
    @ValueSource(ints = { AUTO_ACKNOWLEDGE, DUPS_OK_ACKNOWLEDGE })
    void receiveBodyFailureBehavesAsIfCallHadNotOccurredInAutoAckAndDupsOk(final int sessionMode,
                                                                           final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();
        final ConnectionFactory cf = jms.builder().connection().connectionFactory();

        // Send two messages so the test can observe the required ordering after the failure.
        try (final JMSContext sendingContext = cf.createContext(AUTO_ACKNOWLEDGE))
        {
            sendingContext.createProducer().send(queue, "m0");
            sendingContext.createProducer().send(queue, "m1");
        }

        try (final JMSContext context = cf.createContext(sessionMode))
        {
            final JMSConsumer consumer = context.createConsumer(queue);

            // First delivery attempt: ask for an incompatible body type and expect a MessageFormatRuntimeException.
            assertThrows(MessageFormatRuntimeException.class,
                    () -> consumer.receiveBody(byte[].class, Timeouts.receiveMillis()),
                    "Expected MessageFormatRuntimeException on incompatible receiveBody type");

            // The provider must behave as if the unsuccessful call had not occurred.
            // The failed message must be delivered again and must not be marked as a redelivery.
            final Message first = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(first, "Expected the failed message to be delivered again");
            assertEquals("m0", first.getBody(String.class), "Unexpected message body after failed receiveBody call");
            assertFalse(first.getJMSRedelivered(), "Message must not be marked as redelivered in AUTO/DUPS_OK modes");
            assertEquals(1, first.getIntProperty("JMSXDeliveryCount"),
                    "JMSXDeliveryCount must not be incremented by a failed receiveBody call in AUTO/DUPS_OK modes");

            // The failed message must be delivered again before any subsequent messages.
            final Message second = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(second, "Expected the next message to be delivered after the failed one is successfully received");
            assertEquals("m1", second.getBody(String.class), "Unexpected ordering after failed receiveBody call");
        }
    }

    /**
     * Verifies the failure semantics for {@link JMSContext#CLIENT_ACKNOWLEDGE} when
     * {@link JMSConsumer#receiveBody(Class, long)} throws {@link MessageFormatRuntimeException}.
     *
     * <p>The provider must behave as if the call had succeeded: the message must not be delivered again unless
     * {@link JMSContext#recover()} is called. If {@code recover()} is called, the message must be redelivered with
     * {@code JMSRedelivered=true} and an incremented {@code JMSXDeliveryCount}.</p>
     *
     * <p>Specification:</p>
     * <ul>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#receiving-message-bodies-synchronously">
     *       <strong>Receiving message bodies synchronously</strong>
     *     </a>
     *   </li>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-acknowledgment-1">
     *       <strong>Message acknowledgment (CLIENT_ACKNOWLEDGE, recover)</strong>
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
    void receiveBodyFailureBehavesAsIfCallSucceededInClientAcknowledge(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();
        final ConnectionFactory cf = jms.builder().connection().connectionFactory();

        try (final JMSContext sendingContext = cf.createContext(AUTO_ACKNOWLEDGE))
        {
            sendingContext.createProducer().send(queue, "payload");
        }

        try (final JMSContext clientAckContext = cf.createContext(CLIENT_ACKNOWLEDGE))
        {
            final JMSConsumer consumer = clientAckContext.createConsumer(queue);

            // First delivery attempt: ask for an incompatible body type and expect a MessageFormatRuntimeException.
            assertThrows(MessageFormatRuntimeException.class,
                    () -> consumer.receiveBody(byte[].class, Timeouts.receiveMillis()),
                    "Expected MessageFormatRuntimeException on incompatible receiveBody type");

            // The provider must behave as if the call had succeeded and must not deliver the message again
            // unless recover() is called.
            assertNull(consumer.receive(Timeouts.noMessagesMillis()),
                    "Message must not be delivered again in CLIENT_ACKNOWLEDGE unless recover() is called");

            clientAckContext.recover();

            final Message redelivered = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered, "Expected message to be redelivered after recover()");
            assertEquals("payload", redelivered.getBody(String.class), "Unexpected body for redelivered message");

            assertTrue(redelivered.getJMSRedelivered(), "JMSRedelivered must be true after recover()");
            assertEquals(2, redelivered.getIntProperty("JMSXDeliveryCount"),
                    "JMSXDeliveryCount must be incremented after recover()");

            // Acknowledge to ensure the redelivered message is removed from the queue.
            clientAckContext.acknowledge();
            assertNull(consumer.receive(Timeouts.noMessagesMillis()), "No further deliveries expected after acknowledge()");
        }
    }

    /**
     * Verifies the failure semantics for {@link JMSContext#SESSION_TRANSACTED} when
     * {@link JMSConsumer#receiveBody(Class, long)} throws {@link MessageFormatRuntimeException}.
     *
     * <p>The provider must behave as if the call had succeeded, leaving the local transaction uncommitted. The message
     * must not be delivered again unless the transaction is rolled back. If the transaction is rolled back, the message
     * must be redelivered with {@code JMSRedelivered=true} and an incremented {@code JMSXDeliveryCount}.</p>
     *
     * <p>Specification:</p>
     * <ul>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#receiving-message-bodies-synchronously">
     *       <strong>Receiving message bodies synchronously</strong>
     *     </a>
     *   </li>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#transactions">
     *       <strong>Transactions</strong>
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
    void receiveBodyFailureBehavesAsIfCallSucceededInTransactedContext(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();
        final ConnectionFactory cf = jms.builder().connection().connectionFactory();

        try (final JMSContext sendingContext = cf.createContext(AUTO_ACKNOWLEDGE))
        {
            sendingContext.createProducer().send(queue, "payload");
        }

        try (final JMSContext txContext = cf.createContext(SESSION_TRANSACTED))
        {
            final JMSConsumer consumer = txContext.createConsumer(queue);

            // First delivery attempt: ask for an incompatible body type and expect a MessageFormatRuntimeException.
            assertThrows(MessageFormatRuntimeException.class,
                    () -> consumer.receiveBody(byte[].class, Timeouts.receiveMillis()),
                    "Expected MessageFormatRuntimeException on incompatible receiveBody type");

            // The provider must behave as if the call had succeeded and the message is part of the uncommitted tx.
            assertNull(consumer.receive(Timeouts.noMessagesMillis()),
                    "Message must not be delivered again in a transacted context until rollback() is called");

            txContext.rollback();

            final Message redelivered = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered, "Expected message to be redelivered after rollback()");
            assertEquals("payload", redelivered.getBody(String.class), "Unexpected body for redelivered message");

            assertTrue(redelivered.getJMSRedelivered(), "JMSRedelivered must be true after rollback()");
            assertEquals(2, redelivered.getIntProperty("JMSXDeliveryCount"),
                    "JMSXDeliveryCount must be incremented after rollback()");

            // Commit to acknowledge and remove the redelivered message.
            txContext.commit();
            assertNull(consumer.receive(Timeouts.noMessagesMillis()), "No further deliveries expected after commit()");
        }
    }
}
