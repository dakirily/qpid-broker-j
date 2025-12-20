/*
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

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.apache.qpid.tests.utils.VirtualhostContextVariable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.qpid.systests.EntityTypes.FANOUT_EXCHANGE;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@JmsSystemTest
@VirtualhostContextVariable(name = "virtualhost.housekeepingCheckPeriod", value = "100")
@Tag("queue")
@Tag("routing")
@Tag("timing")
class DeliveryDelayTest
{
    private static final int DELIVERY_DELAY = 3000;

    @Test
    void deliveryDelay(final JmsSupport jms) throws Exception
    {
        try (final var context = jms.builder().connection().connectionFactory().createContext())
        {
            final var queue = jms.builder().queue().holdOnPublishEnabled(true).create();
            final AtomicLong messageReceiptTime = new AtomicLong();
            final CountDownLatch receivedLatch = new CountDownLatch(1);
            context.createConsumer(queue).setMessageListener(message ->
            {
                messageReceiptTime.set(System.currentTimeMillis());
                receivedLatch.countDown();
            });

            final var producer = context.createProducer().setDeliveryDelay(DELIVERY_DELAY);

            final long messageSentTime = System.currentTimeMillis();
            producer.send(queue, "delayed message");

            final boolean messageArrived = receivedLatch.await(DELIVERY_DELAY * 3, TimeUnit.MILLISECONDS);
            assertTrue(messageArrived, "Delayed message did not arrive within expected period");
            final long actualDelay = messageReceiptTime.get() - messageSentTime;
            assertTrue(actualDelay >= DELIVERY_DELAY,
                    "Message was not delayed by sufficient time (%d). Actual delay (%d)".formatted(DELIVERY_DELAY, actualDelay));
        }
    }

    /**
     * The target queue, which is addressed directly by the client, does not have
     * holdsOnPublish turned on.  The Broker must reject the message.
     */
    @Test
    void deliveryDelayNotSupportedByQueue_MessageRejected(final JmsSupport jms) throws Exception
    {
        try (final var context = jms.builder().connection().connectionFactory().createContext())
        {
            final var queue = jms.builder().queue().holdOnPublishEnabled(false).create();
            final var producer = context.createProducer().setDeliveryDelay(DELIVERY_DELAY);

            final var e = assertThrows(JMSRuntimeException.class,
                () -> producer.send(queue, "message"),
                "Exception not thrown");

            assertTrue(e.getMessage().contains("amqp:precondition-failed"),
                    "Unexpected exception message: " + e.getMessage());
        }
    }

    /**
     * The client sends a messagge to a fanout exchange instance which is bound to a queue with
     * holdsOnPublish turned off. The Broker must reject the message.
     */
    @Test
    void deliveryDelayNotSupportedByQueueViaExchange_MessageRejected(final JmsSupport jms) throws Exception
    {
        try (final var context = jms.builder().connection().connectionFactory().createContext())
        {
            String testExchangeName = "test_exch";

            final var consumeDest = jms.builder().queue().holdOnPublishEnabled(false).create();
            final var publishDest = createExchange(jms, context, testExchangeName);
            bindQueueToExchange(jms, testExchangeName, consumeDest.getQueueName());

            final var consumer = context.createConsumer(consumeDest);
            final var producer = context.createProducer();

            producer.send(publishDest, "message without delivery delay");

            final var message = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(message, "Message published without delivery delay not received");

            producer.setDeliveryDelay(DELIVERY_DELAY);

            final var e = assertThrows(JMSRuntimeException.class,
                    () -> producer.send(publishDest, "message with delivery delay"),
                    "Exception not thrown");

            assertTrue(e.getMessage().contains("amqp:precondition-failed"),
                    "Unexpected exception message: " + e.getMessage());
        }
    }

    private Destination createExchange(final JmsSupport jms, final JMSContext context, String exchangeName) throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.Exchange.UNROUTABLE_MESSAGE_BEHAVIOUR, "REJECT");
        jms.management().create(exchangeName, FANOUT_EXCHANGE, attributes);
        return context.createQueue(exchangeName);
    }

    private void bindQueueToExchange(final JmsSupport jms,
                                     final String exchangeName,
                                     final String queueName) throws Exception
    {
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", queueName);
        arguments.put("bindingKey", queueName);
        jms.management().perform(exchangeName, "bind", FANOUT_EXCHANGE, arguments);
    }
}