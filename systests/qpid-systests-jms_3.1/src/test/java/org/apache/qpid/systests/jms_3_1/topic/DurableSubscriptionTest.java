/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.qpid.systests.jms_3_1.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;

import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicSession;

import org.apache.qpid.systests.support.AsyncSupport;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@JmsSystemTest
@Tag("topic")
class DurableSubscriptionTest
{
    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void publishedMessagesAreSavedAfterSubscriberClose(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final var topic = jms.builder().topic().create();
        final var subscriptionName = jms.testMethodName() + "_sub";
        final var clientId = "testClientId";

        try (final var connection = (TopicConnection) jms.builder().connection().clientId(clientId).create();
             final var producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = producerSession.createProducer(topic);
             final var durableSubscriberSession = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var durableSubscriber = durableSubscriberSession.createDurableSubscriber(topic, subscriptionName))
        {
            connection.start();

            producer.send(producerSession.createTextMessage("A"));

            var message = durableSubscriber.receive(Timeouts.receiveMillis());
            var textMessage = assertInstanceOf(TextMessage.class, message);
            assertEquals("A", textMessage.getText());

            durableSubscriberSession.commit();

            producer.send(producerSession.createTextMessage("B"));

            message = durableSubscriber.receive(Timeouts.receiveMillis());
            textMessage = assertInstanceOf(TextMessage.class, message);
            assertEquals("B", textMessage.getText());

            durableSubscriberSession.rollback();

            durableSubscriber.close();
            durableSubscriberSession.close();

            producer.send(producerSession.createTextMessage("C"));
        }

        if (jms.brokerAdmin().supportsRestart())
        {
            async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());
        }

        try (final var connection2 = (TopicConnection) jms.builder().connection().clientId(clientId).create())
        {
            connection2.start();
            final var durableSubscriberSession = connection2.createSession(true, Session.SESSION_TRANSACTED);
            final var durableSubscriber = durableSubscriberSession.createDurableSubscriber(topic, subscriptionName);

            final var expectedMessages = Arrays.asList("B", "C");
            for (final var expectedMessageText : expectedMessages)
            {
                final var message = durableSubscriber.receive(Timeouts.receiveMillis());
                final var textMessage = assertInstanceOf(TextMessage.class, message);
                assertEquals(expectedMessageText, textMessage.getText());

                durableSubscriberSession.commit();
            }

            durableSubscriber.close();
            durableSubscriberSession.unsubscribe(subscriptionName);
        }
    }

    @Test
    void unsubscribe(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();
        final var subscriptionName = jms.testMethodName() + "_sub";
        final var clientId = "clientId";
        final int numberOfQueuesBeforeTest = jms.virtualhost().queueCount();

        try (final var connection = jms.builder().connection().clientId(clientId).create();
             final var durableSubscriberSession = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var nonDurableSubscriberSession = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var subscriber = nonDurableSubscriberSession.createConsumer(topic);
             final var producer = producerSession.createProducer(topic);
             final var durableSubscriber = durableSubscriberSession.createDurableSubscriber(topic, subscriptionName))
        {
            connection.start();
            producer.send(nonDurableSubscriberSession.createTextMessage("A"));

            var message = subscriber.receive(Timeouts.receiveMillis());
            var textMessage = assertInstanceOf(TextMessage.class, message);
            assertEquals("A", textMessage.getText());

            message = durableSubscriber.receive(Timeouts.receiveMillis());
            textMessage = assertInstanceOf(TextMessage.class, message);
            assertEquals("A", textMessage.getText());

            nonDurableSubscriberSession.commit();
            durableSubscriberSession.commit();

            durableSubscriber.close();
            durableSubscriberSession.unsubscribe(subscriptionName);

            producer.send(nonDurableSubscriberSession.createTextMessage("B"));

            final var durableSubscriberSession2 = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var durableSubscriber2 = durableSubscriberSession2.createDurableSubscriber(topic, subscriptionName);

            producer.send(nonDurableSubscriberSession.createTextMessage("C"));

            message = subscriber.receive(Timeouts.receiveMillis());
            textMessage = assertInstanceOf(TextMessage.class, message);
            assertEquals("B", textMessage.getText());

            message = subscriber.receive(Timeouts.receiveMillis());
            textMessage = assertInstanceOf(TextMessage.class, message);
            assertEquals("C", textMessage.getText());

            message = durableSubscriber2.receive(Timeouts.receiveMillis());
            textMessage = assertInstanceOf(TextMessage.class, message);
            assertEquals("C", textMessage.getText());

            nonDurableSubscriberSession.commit();
            durableSubscriberSession2.commit();

            assertEquals(0, jms.virtualhost().totalDepthOfQueuesMessages(), "Message count should be 0");

            durableSubscriber2.close();
            durableSubscriberSession2.unsubscribe(subscriptionName);
        }

        final int numberOfQueuesAfterTest = jms.virtualhost().queueCount();
        assertEquals(numberOfQueuesBeforeTest, numberOfQueuesAfterTest, "Unexpected number of queues");
    }

    @Test
    void unsubscribeTwice(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();
        try (final var connection = jms.builder().connection().create())
        {
            final var subscriptionName = jms.testMethodName() + "_sub";

            final var subscriberSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            final var subscriber = subscriberSession.createDurableSubscriber(topic, subscriptionName);
            final var publisher = subscriberSession.createProducer(topic);

            connection.start();

            publisher.send(subscriberSession.createTextMessage("Test"));
            subscriberSession.commit();

            final var message = subscriber.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "TextMessage should be received");
            assertEquals("Test", textMessage.getText(), "Unexpected message");
            subscriberSession.commit();
            subscriber.close();
            subscriberSession.unsubscribe(subscriptionName);

            assertThrows(InvalidDestinationException.class,
                    () -> subscriberSession.unsubscribe(subscriptionName),
                    "expected InvalidDestinationException when unsubscribing from unknown subscription");
        }
    }

    /**
     * <ul>
     * <li>create and register a durable subscriber with no message selector
     * <li>try to create another durable with the same name, should fail
     * </ul>
     * <p>
     * QPID-2418
     */
    @Test
    void multipleSubscribersWithTheSameName(final JmsSupport jms) throws Exception
    {
        final var subscriptionName = jms.testMethodName() + "_sub";
        final var topic = jms.builder().topic(subscriptionName).create();
        try (final var conn = jms.builder().connection().create())
        {
            conn.start();
            final var session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // create and register a durable subscriber with no message selector
            session.createDurableSubscriber(topic, subscriptionName, null, false);

            // try to recreate the durable subscriber
            assertThrows(JMSException.class,
                    () -> session.createDurableSubscriber(topic, subscriptionName, null, false),
                    "Subscription should not have been created");
        }
    }

    @Test
    @Disabled("Not investigated - fails on AMQP 1.0")
    void durableSubscribeWithTemporaryTopic(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var ssn = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var topic = ssn.createTemporaryTopic();

            assertThrows(InvalidDestinationException.class,
                    () -> ssn.createDurableSubscriber(topic, "test"),
                    "expected InvalidDestinationException");

            assertThrows(InvalidDestinationException.class,
                    () -> ssn.createDurableSubscriber(topic, "test", null, false),
                    "expected InvalidDestinationException");
        }
    }

    @Test
    void localMessagesNotDelivered(final JmsSupport jms) throws Exception
    {
        final var noLocalSubscriptionName = jms.testMethodName() + "_no_local_sub";
        final var topic = jms.builder().topic().create();
        final var clientId = "testClientId";

        try (final var publishingConnection = jms.builder().connection().clientId("publishingConnection").create();
             final var session = publishingConnection.createSession(true, Session.SESSION_TRANSACTED);
             final var sessionProducer = session.createProducer(topic))
        {
            try (final var noLocalConnection = jms.builder().connection().clientId(clientId).create();
                 final var noLocalSession = noLocalConnection.createSession(true, Session.SESSION_TRANSACTED);
                 final var noLocalSessionProducer = noLocalSession.createProducer(topic);
                 final var noLocalSubscriber = noLocalSession.createDurableSubscriber(topic, noLocalSubscriptionName, null, true))
            {
                noLocalConnection.start();
                publishingConnection.start();

                noLocalSessionProducer.send(noLocalSession.createTextMessage("Message1"));
                noLocalSession.commit();
                sessionProducer.send(session.createTextMessage("Message2"));
                session.commit();

                final var durableSubscriberMessage = noLocalSubscriber.receive(Timeouts.receiveMillis());
                final var textMessage = assertInstanceOf(TextMessage.class, durableSubscriberMessage);
                assertEquals("Message2", textMessage.getText(),
                        "Unexpected local message received");
                noLocalSession.commit();
            }

            try (final var noLocalConnection2 = jms.builder().connection().clientId(clientId).create();
                 final var noLocalSession = noLocalConnection2.createSession(true, Session.SESSION_TRANSACTED))
            {
                noLocalConnection2.start();
                try (final var noLocalSubscriber = noLocalSession.createDurableSubscriber(topic, noLocalSubscriptionName, null, true))
                {
                    sessionProducer.send(session.createTextMessage("Message3"));
                    session.commit();

                    final var durableSubscriberMessage = noLocalSubscriber.receive(Timeouts.receiveMillis());
                    final var textMessage = assertInstanceOf(TextMessage.class, durableSubscriberMessage);
                    assertEquals("Message3", textMessage.getText(),
                            "Unexpected local message received");
                    noLocalSession.commit();
                }
                finally
                {
                    noLocalSession.unsubscribe(noLocalSubscriptionName);
                }
            }
        }
    }

    /**
     * Tests that messages are delivered normally to a subscriber on a separate connection despite
     * the use of durable subscriber with no-local on the first connection.
     */
    @Test
    void noLocalSubscriberAndSubscriberOnSeparateConnection(final JmsSupport jms) throws Exception
    {
        final var noLocalSubscriptionName = jms.testMethodName() + "_no_local_sub";
        final var subscriptionName = jms.testMethodName() + "_sub";
        final var topic = jms.builder().topic().create();
        final var clientId = "clientId";

        try (final var noLocalConnection = jms.builder().connection().clientId(clientId).create())
        {
            try (final var connection = jms.builder().connection().create();
                 final var noLocalSession = noLocalConnection.createSession(true, Session.SESSION_TRANSACTED);
                 final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
                 final var noLocalSessionProducer = noLocalSession.createProducer(topic);
                 final var sessionProducer = session.createProducer(topic))
            {
                try (final var noLocalSubscriber = noLocalSession.createDurableSubscriber(topic, noLocalSubscriptionName, null, true);
                     final var subscriber = session.createDurableSubscriber(topic, subscriptionName, null, false))
                {
                    noLocalConnection.start();
                    connection.start();

                    noLocalSessionProducer.send(noLocalSession.createTextMessage("Message1"));
                    noLocalSession.commit();
                    sessionProducer.send(session.createTextMessage("Message2"));
                    sessionProducer.send(session.createTextMessage("Message3"));
                    session.commit();

                    final var durableSubscriberMessage = noLocalSubscriber.receive(Timeouts.receiveMillis());
                    final var textMessageDurable = assertInstanceOf(TextMessage.class, durableSubscriberMessage);
                    assertEquals("Message2", textMessageDurable.getText(),
                            "Unexpected local message received");
                    noLocalSession.commit();

                    final var nonDurableSubscriberMessage = subscriber.receive(Timeouts.receiveMillis());
                    final var textMessageNonDurable = assertInstanceOf(TextMessage.class, nonDurableSubscriberMessage);
                    assertEquals("Message1", textMessageNonDurable.getText(),
                            "Unexpected message received");

                    session.commit();
                }
                finally
                {
                    noLocalSession.unsubscribe(noLocalSubscriptionName);
                    session.unsubscribe(subscriptionName);
                }
            }
        }
    }

    @Test
    void resubscribeWithChangedNoLocal(final JmsSupport jms) throws Exception
    {
        final var subscriptionName = jms.testMethodName() + "_sub";
        final var topic = jms.builder().topic().create();
        final var clientId = "testClientId";

        try (final var connection = jms.builder().connection().clientId(clientId).create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var durableSubscriber = session.createDurableSubscriber(topic, subscriptionName, null, false);
             final var producer = session.createProducer(topic))
        {
            producer.send(session.createTextMessage("A"));
            producer.send(session.createTextMessage("B"));
            session.commit();

            connection.start();

            final var receivedMessage = durableSubscriber.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage, "TextMessage should be received");
            assertEquals("A", textMessage.getText(), "Unexpected message received");

            session.commit();
        }

        try (final var connection = jms.builder().connection().clientId(clientId).create())
        {
            connection.start();

            final var session2 = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var noLocalSubscriber2 = session2.createDurableSubscriber(topic, subscriptionName, null, true);

            try (final var secondConnection = jms.builder().connection().clientId("secondConnection").create();
                 final var secondSession = secondConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 final var secondProducer = secondSession.createProducer(topic))
            {
                secondProducer.send(secondSession.createTextMessage("C"));
            }

            final var noLocalSubscriberMessage = noLocalSubscriber2.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, noLocalSubscriberMessage, "TextMessage should be received");
            assertEquals("C", textMessage.getText(),
                    "Unexpected message received");
        }
    }

    /**
     * create and register a durable subscriber with a message selector and then close it
     * crash the broker
     * create a publisher and send  5 right messages and 5 wrong messages
     * recreate the durable subscriber and check we receive the 5 expected messages
     */
    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void messageSelectorRecoveredOnBrokerRestart(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        assumeTrue(jms.brokerAdmin().supportsRestart());

        final var topic = jms.builder().topic().create();
        final var clientId = "testClientId";
        final var subscriptionName = jms.testMethodName() + "_sub";

        try (final var subscriberConnection = (TopicConnection) jms.builder().connection().clientId(clientId).create();
             final var session = subscriberConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
             final var ignoredSubscriber = session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false))
        {
            subscriberConnection.start();
        }

        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        try (final var connection = (TopicConnection) jms.builder().connection().clientId(clientId).create();
             final var session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
             final var publisher = session.createPublisher(topic))
        {
            for (int i = 0; i < 10; i++)
            {
                final var message = session.createMessage();
                message.setStringProperty("testprop", String.valueOf(i % 2 == 0));
                publisher.publish(message);
            }
        }

        try (final var subscriberConnection2 = (TopicConnection) jms.builder().connection().clientId(clientId).create();
             final var session = subscriberConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            try (final var subscriber = session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false))
            {
                subscriberConnection2.start();
                for (int i = 0; i < 5; i++)
                {
                    final var message = subscriber.receive(1000);
                    if (message == null)
                    {
                        fail("Message '%d' was received".formatted(i));
                    }
                    else
                    {
                        assertEquals("true", message.getStringProperty("testprop"),
                                "Received message %d with not matching selector".formatted(i));
                    }
                }
            }
            session.unsubscribe(subscriptionName);
        }
    }

    /**
     * create and register a durable subscriber without a message selector and then unsubscribe it
     * create and register a durable subscriber with a message selector and then close it
     * restart the broker
     * send matching and non-matching messages
     * recreate and register the durable subscriber with a message selector
     * verify only the matching messages are received
     */
    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void changeSubscriberToHaveSelector(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        assumeTrue(jms.brokerAdmin().supportsRestart());

        final var subscriptionName = jms.testMethodName() + "_sub";
        final var topic = jms.builder().topic().create();
        final var testClientId = "testClientId";

        try (final var subscriberConnection = (TopicConnection) jms.builder().connection().clientId(testClientId).create();
             final var session = subscriberConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
             final var subscriber = session.createDurableSubscriber(topic, subscriptionName);
             final var publisher = session.createPublisher(topic))
        {
            publisher.send(session.createTextMessage("Message1"));
            publisher.send(session.createTextMessage("Message2"));

            subscriberConnection.start();
            final var receivedMessage = subscriber.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message1", textMessage.getText(), "Unexpected message content");
        }

        // create and register a durable subscriber with a message selector and then close it
        try (final var subscriberConnection2 = (TopicConnection) jms.builder().connection().clientId(testClientId).create();
             final var session = subscriberConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
             final var subscriber = session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);
             final var publisher = session.createPublisher(topic))
        {
            var message = session.createTextMessage("Message3");
            message.setStringProperty("testprop", "false");
            publisher.send(message);
            message = session.createTextMessage("Message4");
            message.setStringProperty("testprop", "true");
            publisher.send(message);

            subscriberConnection2.start();

            final var receivedMessage = subscriber.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message4", textMessage.getText(), "Unexpected message content");
        }

        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        try (final var publisherConnection = jms.builder().connection().create(TopicConnection.class);
             final var session = publisherConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
             final var publisher = session.createPublisher(topic))
        {
            for (int i = 0; i < 10; i++)
            {
                final var message = session.createMessage();
                message.setStringProperty("testprop", String.valueOf(i % 2 == 0));
                publisher.publish(message);
            }
        }

        try (final var subscriberConnection3 = (TopicConnection) jms.builder().connection().clientId(testClientId).create();
             final var session = (TopicSession) subscriberConnection3.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
             try (final var subscriber = session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false))
             {
                 subscriberConnection3.start();

                 for (int i = 0; i < 5; i++)
                 {
                     final var message = subscriber.receive(2000);
                     if (message == null)
                     {
                         fail("Message '%d'  was not received".formatted(i));
                     }
                     else
                     {
                         assertEquals("true", message.getStringProperty("testprop"),
                                 "Received message %d with not matching selector".formatted(i));
                     }
                 }

             }
            session.unsubscribe(subscriptionName);
        }
    }

    /**
     * create and register a durable subscriber with a message selector and then unsubscribe it
     * create and register a durable subscriber without a message selector and then close it
     * restart the broker
     * send matching and non-matching messages
     * recreate and register the durable subscriber without a message selector
     * verify ALL sent messages are received
     */
    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void changeSubscriberToHaveNoSelector(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        assumeTrue(jms.brokerAdmin().supportsRestart());

        final var subscriptionName = jms.testMethodName() + "_sub";
        final var topic = jms.builder().topic().create();
        final var clientId = "testClientId";

        // create and register a durable subscriber with selector then unsubscribe it
        try (final var durConnection = (TopicConnection) jms.builder().connection().clientId(clientId).create();
             final var session = durConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
             final var subscriber = session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);
             final var publisher = session.createPublisher(topic))
        {
            var message = session.createTextMessage("Messag1");
            message.setStringProperty("testprop", "false");
            publisher.send(message);
            message = session.createTextMessage("Message2");
            message.setStringProperty("testprop", "true");
            publisher.send(message);

            message = session.createTextMessage("Message3");
            message.setStringProperty("testprop", "true");
            publisher.send(message);

            durConnection.start();

            final var receivedMessage = subscriber.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message2", textMessage.getText(), "Unexpected message content");
        }

        // create and register a durable subscriber without the message selector and then close it
        try (final var subscriberConnection2 = (TopicConnection) jms.builder().connection().clientId(clientId).create();
             final var session = subscriberConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
             final var ignoredSubscriber = session.createDurableSubscriber(topic, subscriptionName))
        {
            subscriberConnection2.start();
        }

        // send messages matching and not matching the original used selector
        try (final var publisherConnection = jms.builder().connection().create(TopicConnection.class);
             final var session = publisherConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
             final var publisher = session.createPublisher(topic))
        {
            for (int i = 1; i <= 10; i++)
            {
                final var message = session.createMessage();
                message.setStringProperty("testprop", String.valueOf(i % 2 == 0));
                publisher.publish(message);
            }
        }

        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        try (final var subscriberConnection3 = (TopicConnection) jms.builder().connection().clientId(clientId).create();
             final var session = (TopicSession) subscriberConnection3.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
             try (final var subscriber = session.createDurableSubscriber(topic, subscriptionName))
             {
                subscriberConnection3.start();

                for (int i = 1; i <= 10; i++)
                {
                    final var message = subscriber.receive(2000);
                    assertNotNull(message, "Message %d  was not received".formatted(i));
                }
            }
            session.unsubscribe(subscriptionName);
        }
    }

    @Test
    @ResourceLock(value = "BROKER", mode = ResourceAccessMode.READ_WRITE)
    void testResubscribeWithChangedSelector(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        assumeTrue(jms.brokerAdmin().supportsRestart());

        final var subscriptionName = jms.testMethodName() + "_sub";
        final var topic = jms.builder().topic().create();
        final var clientId = "testClientId";

        try (final var connection = (TopicConnection) jms.builder().connection().clientId(clientId).create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var producer = session.createProducer(topic);

            // Create durable subscriber that matches A
            final var subscriberA =
                    session.createDurableSubscriber(topic, subscriptionName, "Match = True", false);

            // Send 1 non-matching message and 1 matching message
            var message = session.createTextMessage("Message1");
            message.setBooleanProperty("Match", false);
            producer.send(message);
            message = session.createTextMessage("Message2");
            message.setBooleanProperty("Match", true);
            producer.send(message);

            var receivedMessage = subscriberA.receive(Timeouts.receiveMillis());
            var textMessage = assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message2", textMessage.getText(), "Unexpected message content");

            // Send another 1 matching message and 1 non-matching message
            message = session.createTextMessage("Message3");
            message.setBooleanProperty("Match", true);
            producer.send(message);
            message = session.createTextMessage("Message4");
            message.setBooleanProperty("Match", false);
            producer.send(message);

            // Disconnect subscriber without receiving the message to
            //leave it on the underlying queue
            subscriberA.close();

            // Reconnect with new selector that matches B
            final var subscriberB = session.createDurableSubscriber(topic, subscriptionName, "Match = False", false);

            // Check that new messages are received properly
            message = session.createTextMessage("Message5");
            message.setBooleanProperty("Match", true);
            producer.send(message);
            message = session.createTextMessage("Message6");
            message.setBooleanProperty("Match", false);
            producer.send(message);

            // changing the selector should have cleared the queue so we expect message 6 instead of message 4
            receivedMessage = subscriberB.receive(Timeouts.receiveMillis());
            textMessage = assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message6", textMessage.getText(),
                    "Unexpected message content");

            // publish a message to be consumed after restart
            message = session.createTextMessage("Message7");
            message.setBooleanProperty("Match", true);
            producer.send(message);
            message = session.createTextMessage("Message8");
            message.setBooleanProperty("Match", false);
            producer.send(message);
            session.close();
        }

        //now restart the server
        async.call("broker restart", Timeouts.brokerAdminRestart(), () -> jms.brokerAdmin().restart());

        // Reconnect to broker
        try (final var connection2 = (TopicConnection) jms.builder().connection().clientId(clientId).create())
        {
            connection2.start();
            final var session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Reconnect with new selector that matches B
            final var subscriberC =
                    session.createDurableSubscriber(topic, subscriptionName, "Match = False", false);

            //check the dur sub's underlying queue now has msg count 1
            final var receivedMessage = subscriberC.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message8", textMessage.getText(), "Unexpected message content");

            subscriberC.close();
            session.unsubscribe(subscriptionName);

            session.close();
        }
    }
}
