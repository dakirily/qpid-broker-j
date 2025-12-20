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

package org.apache.qpid.systests.jms_3_1.extensions.maxdelivery;

import static org.apache.qpid.systests.EntityTypes.QUEUE;
import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("policy")
@Tag("queue")
class MaxDeliveryTest
{
    private static final int MAX_DELIVERY_ATTEMPTS = 2;
    private static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";

    @Test
    void maximumDelivery(final JmsSupport jms) throws Exception
    {
        final var dlq = jms.builder().queue().nameSuffix("_DLQ").create();
        final var queue = jms.builder().queue()
                .alternateBinding(dlq.getQueueName())
                .maximumDeliveryAttempts(MAX_DELIVERY_ATTEMPTS)
                .create();

        final int numberOfMessages = 5;
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var consumer = session.createConsumer(queue))
        {
            jms.messages().send(connection, queue, numberOfMessages);

            connection.start();

            int expectedMessageIndex = 0;
            int deliveryAttempt = 0;
            int deliveryCounter = 0;
            do
            {
                final var message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Message '%d' was not received in delivery attempt %d".formatted(expectedMessageIndex, deliveryAttempt));
                int index = message.getIntProperty(INDEX);
                assertEquals(expectedMessageIndex, index, "Unexpected message index (delivery attempt %d)".formatted(deliveryAttempt));

                deliveryCounter++;

                // dlq all even messages
                if (index % 2 == 0)
                {
                    session.rollback();
                    if (deliveryAttempt < MAX_DELIVERY_ATTEMPTS - 1)
                    {
                        deliveryAttempt++;
                    }
                    else
                    {
                        deliveryAttempt = 0;
                        expectedMessageIndex++;
                    }
                }
                else
                {
                    session.commit();
                    deliveryAttempt = 0;
                    expectedMessageIndex++;
                }
            }
            while (expectedMessageIndex != numberOfMessages);

            int numberOfEvenMessages = numberOfMessages / 2 + 1;
            assertEquals(numberOfEvenMessages * MAX_DELIVERY_ATTEMPTS + (numberOfMessages - numberOfEvenMessages),
                    deliveryCounter,
                    "Unexpected total delivery counter");

            verifyDeadLetterQueueMessages(jms, connection, dlq.getQueueName(), numberOfEvenMessages);
        }
    }

    @Test
    void maximumDeliveryWithinMessageListener(final JmsSupport jms) throws Exception
    {
        final var dlq = jms.builder().queue().nameSuffix("_DLQ").create();
        final var queue = jms.builder().queue()
                .alternateBinding(dlq.getQueueName())
                .maximumDeliveryAttempts(MAX_DELIVERY_ATTEMPTS)
                .create();

        final int numberOfMessages = 5;
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var consumer = session.createConsumer(queue))
        {
            jms.messages().send(connection, queue, numberOfMessages);

            connection.start();

            final int numberOfEvenMessages = numberOfMessages / 2 + 1;
            final int expectedNumberOfDeliveries =
                    numberOfEvenMessages * MAX_DELIVERY_ATTEMPTS + (numberOfMessages - numberOfEvenMessages);
            final CountDownLatch deliveryLatch = new CountDownLatch(expectedNumberOfDeliveries);
            final AtomicReference<Throwable> messageListenerThrowable = new AtomicReference<>();
            final AtomicInteger deliveryAttempt = new AtomicInteger();
            final AtomicInteger expectedMessageIndex = new AtomicInteger();
            consumer.setMessageListener(message ->
            {
                try
                {
                    final int index = message.getIntProperty(INDEX);
                    assertEquals(expectedMessageIndex.get(), index,
                            "Unexpected message index (delivery attempt %d)".formatted(deliveryAttempt.get()));

                    // dlq all even messages
                    if (index % 2 == 0)
                    {
                        session.rollback();
                        if (deliveryAttempt.get() < MAX_DELIVERY_ATTEMPTS - 1)
                        {
                            deliveryAttempt.incrementAndGet();
                        }
                        else
                        {
                            deliveryAttempt.set(0);
                            expectedMessageIndex.incrementAndGet();
                        }
                    }
                    else
                    {
                        session.commit();
                        deliveryAttempt.set(0);
                        expectedMessageIndex.incrementAndGet();
                    }
                }
                catch (Throwable t)
                {
                    messageListenerThrowable.set(t);
                }
                finally
                {
                    deliveryLatch.countDown();
                }
            });

            assertTrue(deliveryLatch.await(expectedNumberOfDeliveries * Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "Messages were not received in timely manner");
            assertNull(messageListenerThrowable.get(), "Unexpected throwable in MessageListener");

            verifyDeadLetterQueueMessages(jms, connection, dlq.getQueueName(), numberOfEvenMessages);
        }
    }

    @Test
    void browsingDoesNotIncrementDeliveryCount(final JmsSupport jms) throws Exception
    {
        assumeTrue(jms.brokerAdmin().isManagementSupported());
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();)
        {
            connection.start();
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);

            jms.messages().send(connection, queue, 1);

            final Map<String, Object> messageInfoBefore = getMessageInfo(jms, queue.getQueueName(), 0);
            assertThat("Unexpected delivery count before browse", messageInfoBefore.get("deliveryCount"), is(equalTo(0)));

            browseQueueAndValidationDeliveryHeaders(session, queue);

            final Map<String, Object> messageInfoAfter = getMessageInfo(jms, queue.getQueueName(), 0);
            assertThat("Unexpected delivery count after first browse", messageInfoAfter.get("deliveryCount"), is(equalTo(0)));

            browseQueueAndValidationDeliveryHeaders(session, queue);
        }
    }

    private void verifyDeadLetterQueueMessages(final JmsSupport jms,
                                               final Connection connection,
                                               final String dlqName,
                                               final int numberOfEvenMessages) throws Exception
    {
        assertEquals(numberOfEvenMessages, jms.virtualhost().totalDepthOfQueuesMessages(), "Unexpected number of total messages");

        final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(dlqName);
        MessageConsumer consumer = session.createConsumer(queue);

        for (int i = 0; i < numberOfEvenMessages; i++)
        {
            final var message = consumer.receive(Timeouts.receiveMillis());
            assertEquals(i * 2, message.getIntProperty(INDEX), "Unexpected DLQ message index");
        }
    }

    private Map<String, Object> getMessageInfo(final JmsSupport jms, String queueName, final int index) throws Exception
    {
        List<Map<String, Object>> messages = jms.management()
                .perform(queueName, "getMessageInfo", QUEUE, Collections.emptyMap());
        assertThat("Too few messsages on the queue", messages.size(), is(greaterThan(index)));
        return messages.get(index);
    }

    private void browseQueueAndValidationDeliveryHeaders(final Session session, final Queue queue) throws Exception
    {
        try (final QueueBrowser browser = session.createBrowser(queue))
        {
            final List<Message> messages = (List<Message>) new ArrayList(Collections.list(browser.getEnumeration()));
            assertThat("Unexpected number of messages seen by browser", messages.size(), is(equalTo(1)));
            final Message browsedMessage = messages.get(0);
            assertThat(browsedMessage.getJMSRedelivered(), is(equalTo(false)));

            if (browsedMessage.propertyExists(JMSX_DELIVERY_COUNT))
            {
                assertThat(browsedMessage.getIntProperty(JMSX_DELIVERY_COUNT), is(equalTo(1)));
            }
        }
    }
}
