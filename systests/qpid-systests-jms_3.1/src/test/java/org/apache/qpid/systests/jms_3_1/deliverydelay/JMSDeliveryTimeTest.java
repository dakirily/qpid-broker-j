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

package org.apache.qpid.systests.jms_3_1.deliverydelay;

import static org.apache.qpid.systests.JmsAwait.jms;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import jakarta.jms.Message;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.apache.qpid.tests.utils.VirtualhostContextVariable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on JMSDeliveryTime semantics for the classic API.
 * <br>
 * The tests in this class are aligned with:
 * - 3.4.13 "JMSDeliveryTime"
 * - 7.9   "Message delivery delay"
 * - 6.2.7 "Transactions"
 */
@JmsSystemTest
@Tag("queue")
@Tag("message")
@Tag("timing")
class JMSDeliveryTimeTest
{
    private static final int DELIVERY_DELAY = 2000;

    /**
     * Verifies that:
     * - JMSDeliveryTime is set on the sent message when delivery delay is used;
     * - the delivery time is preserved on receive.
     * <br>
     * See sections 3.4.13 and 7.9 for header calculation and delivery delay semantics.
     */
    @Test
    @VirtualhostContextVariable(name = "virtualhost.housekeepingCheckPeriod", value = "100")
    void deliveryTimeHeaderIsSetOnSendAndPreservedOnReceive(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = jms.builder().queue().holdOnPublishEnabled(true).create();
            final var producer = session.createProducer(queue);
            producer.setDeliveryDelay(DELIVERY_DELAY);

            final var message = session.createMessage();
            final long sendTime = System.currentTimeMillis();
            producer.send(message);
            final long sendReturnTime = System.currentTimeMillis();

            final long deliveryTime = message.getJMSDeliveryTime();
            assertTrue(deliveryTime >= sendTime + DELIVERY_DELAY,
                    "JMSDeliveryTime should be at or after send time + delivery delay");
            assertTrue(deliveryTime <= sendReturnTime + DELIVERY_DELAY + 1000,
                    "JMSDeliveryTime should be close to send time + delivery delay");

            final var consumer = session.createConsumer(queue);
            connection.start();
            final long receiveTimeout = Math.max(Timeouts.receiveMillis(), DELIVERY_DELAY * 2L);
            final Message received = consumer.receive(receiveTimeout);
            assertNotNull(received, "Message should be delivered after delay");
            assertEquals(deliveryTime, received.getJMSDeliveryTime(),
                    "Received JMSDeliveryTime should match the value set on send");
        }
    }

    /**
     * Verifies that delivery time is based on the send time, not the commit time,
     * for transacted sessions. This is required by section 7.9.
     */
    @Test
    void transactedSendUsesSendTimeForDeliveryTime(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED))
        {
            final var queue = jms.builder().queue().holdOnPublishEnabled(true).create();
            final var producer = session.createProducer(queue);
            producer.setDeliveryDelay(DELIVERY_DELAY);

            final var message = session.createMessage();
            final long sendTime = System.currentTimeMillis();
            producer.send(message);

            final long earliestCommitTime = System.currentTimeMillis() + DELIVERY_DELAY + 500L;
            jms(Duration.ofMillis(DELIVERY_DELAY + 1500L))
                    .until(() -> System.currentTimeMillis() >= earliestCommitTime);

            final long commitTime = System.currentTimeMillis();
            session.commit();

            final long deliveryTime = message.getJMSDeliveryTime();
            assertTrue(deliveryTime >= sendTime + DELIVERY_DELAY,
                    "JMSDeliveryTime should be based on send time + delivery delay");
            assertTrue(deliveryTime <= commitTime,
                    "Delivery time should already have elapsed before commit");

            final var consumer = session.createConsumer(queue);
            connection.start();
            final long receiveTimeout = Math.max(Timeouts.receiveMillis(), DELIVERY_DELAY * 2L);
            final var received = consumer.receive(receiveTimeout);
            assertNotNull(received, "Message should be available immediately after commit");
            assertEquals(deliveryTime, received.getJMSDeliveryTime(),
                    "Received JMSDeliveryTime should match the value set on send");
        }
    }
}
