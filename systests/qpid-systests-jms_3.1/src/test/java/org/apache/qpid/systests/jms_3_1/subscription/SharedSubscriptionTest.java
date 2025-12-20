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
 */

package org.apache.qpid.systests.jms_3_1.subscription;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.JMSException;

import org.apache.qpid.systests.support.AsyncSupport;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@JmsSystemTest
@Tag("topic")
class SharedSubscriptionTest
{
    @Test
    void sharedNonDurableSubscription(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var publishingSession = connection.createSession();
             final var subscriber1Session = connection.createSession();
             final var subscriber2Session = connection.createSession())
        {
            final var topicName = jms.testMethodName();
            final var topic = publishingSession.createTopic("amq.direct/" + topicName);

            final var consumer1 = subscriber1Session.createSharedConsumer(topic, "subscription");
            final var consumer2 = subscriber2Session.createSharedConsumer(topic, "subscription");

            jms.messages().send(publishingSession, topic, 2);

            connection.start();

            final var message1 = consumer1.receive(Timeouts.receiveMillis());
            final var message2 = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(message1, "Message 1 was not received");
            assertNotNull(message2, "Message 2 was not received");

            assertEquals(0, message1.getIntProperty(INDEX), "Unexpected index for message 1");
            assertEquals(1, message2.getIntProperty(INDEX), "Unexpected index for message 2");

            final var message3 = consumer1.receive(Timeouts.noMessagesMillis());
            final var message4 = consumer2.receive(Timeouts.noMessagesMillis());

            assertNull(message3, "Unexpected message received by first shared consumer");
            assertNull(message4, "Unexpected message received by second shared consumer");
        }
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void sharedDurableSubscription(final JmsSupport jms,
                                   final AsyncSupport async) throws Exception
    {
        final var topicName = jms.testMethodName();
        try (final var connection = jms.builder().connection().prefetch(0).clientId("myClientId").create();
             final var publishingSession = connection.createSession();
             final var subscriber1Session = connection.createSession();
             final var subscriber2Session = connection.createSession())
        {
            final var topic = publishingSession.createTopic("amq.direct/" + topicName);

            final var consumer1 = subscriber1Session.createSharedDurableConsumer(topic, "subscription");
            final var consumer2 = subscriber2Session.createSharedDurableConsumer(topic, "subscription");

            jms.messages().send(publishingSession, topic, 4);

            connection.start();

            final var message1 = consumer1.receive(Timeouts.receiveMillis());
            final var message2 = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(message1, "Message 1 was not received");
            assertNotNull(message2, "Message 2 was not received");

            assertEquals(0, message1.getIntProperty(INDEX), "Unexpected index for message 1");
            assertEquals(1, message2.getIntProperty(INDEX), "Unexpected index for message 2");
        }

        if (jms.brokerAdmin().supportsRestart())
        {
            async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());
        }

        try (final var connection = jms.builder().connection().prefetch(0).clientId("myClientId").create();
             final var subscriber1Session = connection.createSession();
             final var subscriber2Session = connection.createSession())
        {
            final var topic = subscriber1Session.createTopic("amq.direct/" + topicName);
            final var consumer1 = subscriber1Session.createSharedDurableConsumer(topic, "subscription");
            final var consumer2 = subscriber2Session.createSharedDurableConsumer(topic, "subscription");

            connection.start();

            final var message3 = consumer1.receive(Timeouts.receiveMillis());
            final var message4 = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(message3, "Message 3 was not received");
            assertNotNull(message4, "Message 4 was not received");

            assertEquals(2, message3.getIntProperty(INDEX), "Unexpected index for message 3");
            assertEquals(3, message4.getIntProperty(INDEX), "Unexpected index for message 4");

            final var message5 = consumer1.receive(Timeouts.noMessagesMillis());
            final var message6 = consumer2.receive(Timeouts.noMessagesMillis());

            assertNull(message5, "Unexpected message received by first shared consumer");
            assertNull(message6, "Unexpected message received by second shared consumer");
        }
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void unsubscribe(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        sharedDurableSubscriptionUnsubscribeTest(jms, async, "myClientId");
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void unsubscribeForGlobalSharedDurableSubscription(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        sharedDurableSubscriptionUnsubscribeTest(jms, async, null);
    }

    private void sharedDurableSubscriptionUnsubscribeTest(final JmsSupport jms,
                                                          final AsyncSupport async,
                                                          final String clientId) throws Exception
    {
        final var subscriptionName = "testSharedSubscription";
        final int numberOfQueuesBeforeTest = jms.virtualhost().queueCount();
        final var topicName = jms.testMethodName();
        try (final var connection = jms.builder().connection().prefetch(0).clientId(clientId).create();
             final var session = connection.createSession())
        {
            connection.start();

            final var topic = session.createTopic("amq.direct/" + topicName);
            try (final var ignoredConsumer = session.createSharedDurableConsumer(topic, subscriptionName))
            {
                int numberOfQueuesBeforeUnsubscribe = jms.virtualhost().queueCount();
                assertEquals(numberOfQueuesBeforeTest + 1, numberOfQueuesBeforeUnsubscribe, "Unexpected number of Queues");
            }
        }

        if (jms.brokerAdmin().supportsRestart())
        {
            async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());
        }

        try (final var connection = jms.builder().connection().prefetch(0).clientId(clientId).create())
        {
            final var session = connection.createSession();
            session.unsubscribe(subscriptionName);

            int numberOfQueuesAfterUnsubscribe = jms.virtualhost().queueCount();
            assertEquals(numberOfQueuesBeforeTest, numberOfQueuesAfterUnsubscribe, "Queue should be deleted");
        }
    }

    @Test
    void durableSharedAndNonDurableSharedCanUseTheSameSubscriptionName(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var publishingSession = connection.createSession();
             final var subscriberSession = connection.createSession())
        {
            final var topicName = jms.testMethodName();
            final var topic = publishingSession.createTopic("amq.direct/" + topicName);
            final var consumer1 = subscriberSession.createSharedDurableConsumer(topic, "testSharedSubscription");
            final var consumer2 = subscriberSession.createSharedConsumer(topic, "testSharedSubscription");
            connection.start();

            jms.messages().send(publishingSession, topic, 1);

            final var message1 = consumer1.receive(Timeouts.receiveMillis());
            final var message2 = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(message1, "Message 1 was not received");
            assertNotNull(message2, "Message 2 was not received");

            assertEquals(0, message1.getIntProperty(INDEX), "Unexpected index for message 1");
            assertEquals(0, message2.getIntProperty(INDEX), "Unexpected index for message 2");
        }
    }

    @Test
    void globalAndNotGlobalCanUseTheSameSubscriptionName(final JmsSupport jms) throws Exception
    {
        try (final var connection =  jms.builder().connection().clientId("testClientId").create();
             final var connection2 = jms.builder().connection().clientId(null).create();
             final var publishingSession = connection.createSession();
             final var subscriber1Session = connection.createSession();
             final var subscriber2Session = connection2.createSession())
        {
            final var topicName = jms.testMethodName();
            final var topic = publishingSession.createTopic("amq.direct/" + topicName);
            final var consumer1 = subscriber1Session.createSharedConsumer(topic, "testSharedSubscription");
            final var consumer2 = subscriber2Session.createSharedConsumer(topic, "testSharedSubscription");

            connection.start();
            connection2.start();

            jms.messages().send(publishingSession, topic, 1);

            final var message1 = consumer1.receive(Timeouts.receiveMillis());
            final var message2 = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(message1, "Message 1 was not received");
            assertNotNull(message2, "Message 2 was not received");

            assertEquals(0, message1.getIntProperty(INDEX), "Unexpected index for message 1");
            assertEquals(0, message2.getIntProperty(INDEX), "Unexpected index for message 2");
        }
    }

    @Test
    void topicOrSelectorChange(final JmsSupport jms) throws Exception
    {
        try (final var connection =  jms.builder().connection().prefetch(0).clientId(null).create();
             final var connection2 = jms.builder().connection().prefetch(0).clientId(null).create();
             final var publishingSession = connection.createSession();
             final var subscriber1Session = connection.createSession();
             final var subscriber2Session = connection2.createSession())
        {
            final var topicName = jms.testMethodName();
            final var topic = publishingSession.createTopic("amq.direct/" + topicName);
            final var topic2 = publishingSession.createTopic("amq.direct/" + topicName + "2");

            try (var consumer1 = subscriber1Session.createSharedDurableConsumer(topic, "subscription", "index>1")) {

                jms.messages().send(publishingSession, topic, 4);

                connection.start();
                connection2.start();

                final var message1 = consumer1.receive(Timeouts.receiveMillis());
                assertNotNull(message1, "Message 1 was not received");
                assertEquals(2, message1.getIntProperty(INDEX), "Unexpected index for message 1");

                assertThrows(JMSException.class,
                        () -> subscriber2Session.createSharedDurableConsumer(topic, "subscription", "index>2"),
                        "Consumer should not be allowed to join shared subscription with different filter when there is an active subscriber");

                assertThrows(JMSException.class,
                        () -> subscriber2Session.createSharedDurableConsumer(topic2, "subscription", "index>1"),
                        "Consumer should not be allowed to join shared subscription with different topic when there is an active subscriber");
            }

            try (var consumer2 = subscriber2Session.createSharedDurableConsumer(topic, "subscription", "index>2"))
            {
                final var message2 = consumer2.receive(Timeouts.noMessagesMillis());
                assertNull(message2,
                           "No message should be received as re-subscribing with different topic or selector is equivalent to unsubscribe/subscribe");

                jms.messages().send(publishingSession, topic, 4);

                final var message3 = consumer2.receive(Timeouts.receiveMillis());
                assertNotNull(message3, "Should receive message 3");
                assertEquals(3, message3.getIntProperty(INDEX), "Unexpected index for message 3");
            }

            try (var consumer3 = subscriber2Session.createSharedDurableConsumer(topic2, "subscription", "index>2"))
            {
                final var message4 = consumer3.receive(Timeouts.noMessagesMillis());

                assertNull(message4,
                        "No message should be received as re-subscribing with different topic or selector is equivalent to unsubscribe/subscribe");

                jms.messages().send(publishingSession, topic2, 4);

                final var message5 = consumer3.receive(Timeouts.receiveMillis());
                assertEquals(3, message5.getIntProperty(INDEX), "Unexpected index for message 5");
            }
        }
    }
}
