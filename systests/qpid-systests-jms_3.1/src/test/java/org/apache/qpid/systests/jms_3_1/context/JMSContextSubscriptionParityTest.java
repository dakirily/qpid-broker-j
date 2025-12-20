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

import static org.apache.qpid.systests.JmsAwait.jms;
import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.Topic;

import org.apache.qpid.systests.support.AsyncSupport;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * System tests verifying that the simplified API ({@link jakarta.jms.JMSContext} and
 * {@link jakarta.jms.JMSConsumer}) provides the same semantics for shared subscriptions as the
 * classic API ({@link jakarta.jms.Session}).
 *
 * In particular, the Jakarta Messaging specification explicitly defines shared subscriptions
 * (both non-durable and durable) and states that they can be created using either the classic API
 * ({@code Session#createSharedConsumer}, {@code Session#createSharedDurableConsumer}) or the
 * simplified API ({@code JMSContext#createSharedConsumer}, {@code JMSContext#createSharedDurableConsumer}).
 *
 * The tests in this class are aligned with:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#shared-non-durable-subscriptions">8.3.2 "Shared non-durable subscriptions"</a>
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#shared-durable-subscriptions">8.3.4 "Shared durable subscriptions"</a>
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#starting-message-delivery">8.4 "Starting message delivery"</a>
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#client-identifier">6.1.2 "Client identifier"</a>
 */
@JmsSystemTest
@Tag("topic")
class JMSContextSubscriptionParityTest
{
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * Verifies that a shared non-durable subscription created via {@link jakarta.jms.JMSContext}
     * distributes messages across multiple consumers and does not persist after the last consumer
     * on the subscription is closed.
     *
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#shared-non-durable-subscriptions">8.3.2</a>
     * states that:
     * - a shared non-durable subscription may have more than one consumer; and
     * - each message added to the subscription will be delivered to only one consumer.
     *
     * It also states that a shared non-durable subscription is deleted when the last consumer on
     * the subscription is closed. This implies that messages published while there are no active
     * consumers on the subscription are not accumulated for later delivery.
     */
    @Test
    void sharedNonDurableSubscriptionDistributesAndIsNonPersistent(final JmsSupport jms) throws Exception
    {
        final ConnectionFactory cf = jms.builder().connection().prefetch(0).clientId(null).connectionFactory();

        final String address = "amq.direct/" + jms.testMethodName();
        final String subscriptionName = jms.testMethodName() + "_shared";

        try (final JMSContext publisherContext = cf.createContext();
             final JMSContext consumerContext1 = cf.createContext();
             final JMSContext consumerContext2 = cf.createContext())
        {
            final Topic publishTopic = publisherContext.createTopic(address);
            final Topic consumerTopic1 = consumerContext1.createTopic(address);
            final Topic consumerTopic2 = consumerContext2.createTopic(address);

            final JMSProducer producer = publisherContext.createProducer();

            // Create two consumers on the same shared subscription.
            try (final JMSConsumer consumer1 = consumerContext1.createSharedConsumer(consumerTopic1, subscriptionName);
                 final JMSConsumer consumer2 = consumerContext2.createSharedConsumer(consumerTopic2, subscriptionName))
            {
                sendIndexedMessages(publisherContext, producer, publishTopic, 0, 2);

                final Set<Integer> received = receiveAndReturnIndexes(consumer1, consumer2, 2);
                assertEquals(Set.of(0, 1), received,
                        "Unexpected messages received from shared non-durable subscription");

                assertNull(consumer1.receive(Timeouts.noMessagesMillis()), "Unexpected extra message received by first shared consumer");
                assertNull(consumer2.receive(Timeouts.noMessagesMillis()), "Unexpected extra message received by second shared consumer");
            }

            // With no active consumers, the shared non-durable subscription is deleted.
            // Messages published while no consumer exists must not be accumulated for later delivery.
            sendIndexedMessages(publisherContext, producer, publishTopic, 2, 1);

            try (final JMSConsumer consumer = consumerContext1.createSharedConsumer(consumerTopic1, subscriptionName))
            {
                final Message unexpected = consumer.receive(Timeouts.noMessagesMillis());
                assertNull(unexpected,
                        "Shared non-durable subscription should not accumulate messages while no consumers are active");
            }
        }
    }

    /**
     * Verifies that a shared durable subscription created via {@link jakarta.jms.JMSContext}
     * both distributes work across multiple consumers and persists when there are no active
     * consumers.
     *
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#shared-durable-subscriptions">8.3.4</a>
     * states that a shared durable subscription:
     * - may have more than one consumer;
     * - delivers each message to only one consumer; and
     * - is persisted and continues to accumulate messages even when there are no active consumers,
     *   until it is deleted using {@code unsubscribe}.
     */
    @Test
    void sharedDurableSubscriptionPersistsAndDistributes(final JmsSupport jms) throws Exception
    {
        final ConnectionFactory cf = jms.builder().connection().prefetch(0).clientId(null).connectionFactory();

        final String address = "amq.direct/" + jms.testMethodName();
        final String subscriptionName = jms.testMethodName() + "_shared_durable";

        try (final JMSContext publisherContext = cf.createContext();
             final JMSContext consumerContext1 = cf.createContext();
             final JMSContext consumerContext2 = cf.createContext())
        {
            final Topic publishTopic = publisherContext.createTopic(address);
            final Topic consumerTopic1 = consumerContext1.createTopic(address);
            final Topic consumerTopic2 = consumerContext2.createTopic(address);

            final JMSProducer producer = publisherContext.createProducer();

            // Phase 1: create two consumers on the shared durable subscription and send two messages.
            try (final JMSConsumer consumer1 = consumerContext1.createSharedDurableConsumer(consumerTopic1, subscriptionName);
                 final JMSConsumer consumer2 = consumerContext2.createSharedDurableConsumer(consumerTopic2, subscriptionName))
            {
                sendIndexedMessages(publisherContext, producer, publishTopic, 0, 2);

                final Set<Integer> received = receiveAndReturnIndexes(consumer1, consumer2, 2);
                assertEquals(Set.of(0, 1), received, "Unexpected messages received from shared durable subscription");
            }

            // Phase 2: send messages while there are no active consumers.
            // A durable subscription must continue to accumulate messages.
            sendIndexedMessages(publisherContext, producer, publishTopic, 2, 2);

            // Phase 3: recreate consumers and verify the messages published while offline are delivered.
            try (final JMSConsumer consumer1 = consumerContext1.createSharedDurableConsumer(consumerTopic1, subscriptionName);
                 final JMSConsumer consumer2 = consumerContext2.createSharedDurableConsumer(consumerTopic2, subscriptionName))
            {
                final Set<Integer> received = receiveAndReturnIndexes(consumer1, consumer2, 2);
                assertEquals(Set.of(2, 3), received,
                        "Shared durable subscription did not retain messages published while no consumers were active");

                assertNull(consumer1.receive(Timeouts.noMessagesMillis()), "Unexpected extra message received by first durable shared consumer");
                assertNull(consumer2.receive(Timeouts.noMessagesMillis()), "Unexpected extra message received by second durable shared consumer");
            }

            // Cleanup: durable subscriptions persist until explicitly deleted.
            publisherContext.unsubscribe(subscriptionName);
        }
    }

    /**
     * Verifies that {@link jakarta.jms.JMSContext#unsubscribe(String)} deletes a shared durable
     * subscription when there are no active consumers.
     *
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#shared-durable-subscriptions">8.3.4</a>
     * states that a shared durable subscription continues to exist until it is deleted using the
     * {@code unsubscribe} method on {@code Session}, {@code TopicSession} or {@code JMSContext}.
     */
    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void unsubscribeDeletesSharedDurableSubscription(final JmsSupport jms,
                                                     final AsyncSupport async) throws Exception
    {
        final int numberOfQueuesBeforeTest = jms.virtualhost().queueCount();

        final ConnectionFactory cf = jms.builder().connection().prefetch(0).clientId(null).connectionFactory();

        final String address = "amq.direct/" + jms.testMethodName();
        final String subscriptionName = jms.testMethodName() + "_to_unsubscribe";

        // Create the subscription (and close the consumer) to ensure it becomes inactive but persists.
        try (final JMSContext context = cf.createContext())
        {
            final Topic topic = context.createTopic(address);
            try (final var ignored = context.createSharedDurableConsumer(topic, subscriptionName))
            {
                // Await is used because the broker-side resources may be created asynchronously.
                jms(TIMEOUT).untilAsserted(() -> assertEquals(numberOfQueuesBeforeTest + 1, jms.virtualhost().queueCount(),
                        "Unexpected number of queues after creating shared durable subscription"));
            }
        }

        if (jms.brokerAdmin().supportsRestart())
        {
            async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());
        }

        // Unsubscribe should delete the durable subscription (and its backing queue).
        try (final JMSContext context = cf.createContext())
        {
            context.unsubscribe(subscriptionName);

            jms(TIMEOUT).untilAsserted(() -> assertEquals(numberOfQueuesBeforeTest, jms.virtualhost().queueCount(),
                    "Shared durable subscription should be deleted by unsubscribe"));
        }
    }

    private void sendIndexedMessages(final JMSContext context,
                                     final JMSProducer producer,
                                     final Topic topic,
                                     final int startIndex,
                                     final int count) throws Exception
    {
        for (int i = 0; i < count; i++)
        {
            final var message = context.createMessage();
            message.setIntProperty(INDEX, startIndex + i);
            producer.send(topic, message);
        }
    }

    private Set<Integer> receiveAndReturnIndexes(final JMSConsumer consumer1,
                                                 final JMSConsumer consumer2,
                                                 final int expectedMessageCount) throws Exception
    {
        final Set<Integer> received = new HashSet<>();

        // Shared subscriptions do not guarantee fair distribution between consumers. This helper
        // therefore only asserts that the expected number of distinct messages is received across
        // the set of consumers.
        jms(TIMEOUT).untilAsserted(() ->
        {
            final Message m1 = consumer1.receive(Timeouts.noMessagesMillis());
            if (m1 != null)
            {
                received.add(m1.getIntProperty(INDEX));
            }

            final Message m2 = consumer2.receive(Timeouts.noMessagesMillis());
            if (m2 != null)
            {
                received.add(m2.getIntProperty(INDEX));
            }

            assertEquals(expectedMessageCount,
                    received.size(),
                    "Did not receive expected number of messages from the shared subscription within the timeout");
        });

        return received;
    }
}
