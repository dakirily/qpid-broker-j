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

package org.apache.qpid.systests.jms_1_1.topic;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.JmsTestBase;
import org.junit.jupiter.api.Test;

import jakarta.jms.Connection;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicPublisher;
import jakarta.jms.TopicSession;
import jakarta.jms.TopicSubscriber;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class DurableSubscriptionTest extends JmsTestBase
{
    @Test
    public void publishedMessagesAreSavedAfterSubscriberClose() throws Exception
    {
        Topic topic = createTopic(getTestName());
        String subscriptionName = getTestName() + "_sub";
        String clientId = "testClientId";

        try (TopicConnection connection = (TopicConnection) getConnectionBuilder().setClientId(clientId).build())
        {
            Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = producerSession.createProducer(topic);

            Session durableSubscriberSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            TopicSubscriber durableSubscriber =
                    durableSubscriberSession.createDurableSubscriber(topic, subscriptionName);

            connection.start();

            producer.send(producerSession.createTextMessage("A"));

            Message message = durableSubscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, message);
            assertEquals("A", ((TextMessage) message).getText());

            durableSubscriberSession.commit();

            producer.send(producerSession.createTextMessage("B"));

            message = durableSubscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, message);
            assertEquals("B", ((TextMessage) message).getText());

            durableSubscriberSession.rollback();

            durableSubscriber.close();
            durableSubscriberSession.close();

            producer.send(producerSession.createTextMessage("C"));
        }

        if (getBrokerAdmin().supportsRestart())
        {
            getBrokerAdmin().restart();
        }

        try (TopicConnection connection2 = (TopicConnection) getConnectionBuilder().setClientId(clientId).build())
        {
            connection2.start();
            final Session durableSubscriberSession = connection2.createSession(true, Session.SESSION_TRANSACTED);
            final TopicSubscriber durableSubscriber =
                    durableSubscriberSession.createDurableSubscriber(topic, subscriptionName);

            final List<String> expectedMessages = Arrays.asList("B", "C");
            for (String expectedMessageText : expectedMessages)
            {
                final Message message = durableSubscriber.receive(getReceiveTimeout());
                assertInstanceOf(TextMessage.class, message);
                assertEquals(expectedMessageText, ((TextMessage) message).getText());

                durableSubscriberSession.commit();
            }

            durableSubscriber.close();
            durableSubscriberSession.unsubscribe(subscriptionName);
        }
    }

    @Test
    public void testUnsubscribe() throws Exception
    {
        Topic topic = createTopic(getTestName());
        String subscriptionName = getTestName() + "_sub";
        String clientId = "clientId";
        int numberOfQueuesBeforeTest = getQueueCount();

        try (Connection connection = getConnectionBuilder().setClientId(clientId).build())
        {
            Session durableSubscriberSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            Session nonDurableSubscriberSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer subscriber = nonDurableSubscriberSession.createConsumer(topic);
            MessageProducer producer = producerSession.createProducer(topic);
            TopicSubscriber durableSubscriber =
                    durableSubscriberSession.createDurableSubscriber(topic, subscriptionName);

            connection.start();
            producer.send(nonDurableSubscriberSession.createTextMessage("A"));

            Message message = subscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, message);
            assertEquals("A", ((TextMessage) message).getText());

            message = durableSubscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, message);
            assertEquals("A", ((TextMessage) message).getText());

            nonDurableSubscriberSession.commit();
            durableSubscriberSession.commit();

            durableSubscriber.close();
            durableSubscriberSession.unsubscribe(subscriptionName);

            producer.send(nonDurableSubscriberSession.createTextMessage("B"));

            Session durableSubscriberSession2 = connection.createSession(true, Session.SESSION_TRANSACTED);
            TopicSubscriber durableSubscriber2 =
                    durableSubscriberSession2.createDurableSubscriber(topic, subscriptionName);

            producer.send(nonDurableSubscriberSession.createTextMessage("C"));

            message = subscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, message);
            assertEquals("B", ((TextMessage) message).getText());

            message = subscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, message);
            assertEquals("C", ((TextMessage) message).getText());

            message = durableSubscriber2.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, message);
            assertEquals("C", ((TextMessage) message).getText());

            nonDurableSubscriberSession.commit();
            durableSubscriberSession2.commit();

            assertEquals(0, getTotalDepthOfQueuesMessages(), "Message count should be 0");

            durableSubscriber2.close();
            durableSubscriberSession2.unsubscribe(subscriptionName);
        }

        int numberOfQueuesAfterTest = getQueueCount();
        assertEquals(numberOfQueuesBeforeTest, numberOfQueuesAfterTest, "Unexpected number of queues");
    }

    @Test
    public void unsubscribeTwice() throws Exception
    {
        Topic topic = createTopic(getTestName());
        try (Connection connection = getConnection())
        {
            String subscriptionName = getTestName() + "_sub";

            Session subscriberSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = subscriberSession.createDurableSubscriber(topic, subscriptionName);
            MessageProducer publisher = subscriberSession.createProducer(topic);

            connection.start();

            publisher.send(subscriberSession.createTextMessage("Test"));
            subscriberSession.commit();

            Message message = subscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, message, "TextMessage should be received");
            assertEquals("Test", ((TextMessage) message).getText(), "Unexpected message");
            subscriberSession.commit();
            subscriber.close();
            subscriberSession.unsubscribe(subscriptionName);

            try {
                subscriberSession.unsubscribe(subscriptionName);
                fail("expected InvalidDestinationException when unsubscribing from unknown subscription");
            } catch (InvalidDestinationException e) {
                // PASS
            } catch (Exception e) {
                fail("expected InvalidDestinationException when unsubscribing from unknown subscription, got: " + e);
            }
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
    public void multipleSubscribersWithTheSameName() throws Exception
    {
        String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(subscriptionName);
        try (Connection conn = getConnection())
        {
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // create and register a durable subscriber with no message selector
            session.createDurableSubscriber(topic, subscriptionName, null, false);

            // try to recreate the durable subscriber
            try {
                session.createDurableSubscriber(topic, subscriptionName, null, false);
                fail("Subscription should not have been created");
            } catch (JMSException e) {
                // pass
            }
        }
    }

    @Test
    public void testDurableSubscribeWithTemporaryTopic() throws Exception
    {
        assumeTrue(is(not(equalTo(Protocol.AMQP_1_0))).matches(getProtocol()), "Not investigated - fails on AMQP 1.0");
        try (Connection connection = getConnection())
        {
            connection.start();
            Session ssn = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = ssn.createTemporaryTopic();
            try
            {
                ssn.createDurableSubscriber(topic, "test");
                fail("expected InvalidDestinationException");
            } catch (InvalidDestinationException ex) {
                // this is expected
            }
            try
            {
                ssn.createDurableSubscriber(topic, "test", null, false);
                fail("expected InvalidDestinationException");
            } catch (InvalidDestinationException ex) {
                // this is expected
            }
        }
    }

    @Test
    public void noLocalMessagesNotDelivered() throws Exception
    {
        String noLocalSubscriptionName = getTestName() + "_no_local_sub";
        Topic topic = createTopic(getTestName());
        String clientId = "testClientId";

        try (Connection publishingConnection = getConnectionBuilder().setClientId("publishingConnection").build())
        {
            Session session = publishingConnection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer sessionProducer = session.createProducer(topic);

            try (Connection noLocalConnection = getConnectionBuilder().setClientId(clientId).build())
            {
                Session noLocalSession = noLocalConnection.createSession(true, Session.SESSION_TRANSACTED);
                MessageProducer noLocalSessionProducer = noLocalSession.createProducer(topic);

                TopicSubscriber noLocalSubscriber =
                        noLocalSession.createDurableSubscriber(topic, noLocalSubscriptionName, null, true);
                noLocalConnection.start();
                publishingConnection.start();

                noLocalSessionProducer.send(noLocalSession.createTextMessage("Message1"));
                noLocalSession.commit();
                sessionProducer.send(session.createTextMessage("Message2"));
                session.commit();

                Message durableSubscriberMessage = noLocalSubscriber.receive(getReceiveTimeout());
                assertInstanceOf(TextMessage.class, durableSubscriberMessage);
                assertEquals("Message2", ((TextMessage) durableSubscriberMessage).getText(),
                        "Unexpected local message received");
                noLocalSession.commit();
            }

            try (Connection noLocalConnection2 = getConnectionBuilder().setClientId(clientId).build())
            {
                Session noLocalSession = noLocalConnection2.createSession(true, Session.SESSION_TRANSACTED);
                noLocalConnection2.start();
                try (TopicSubscriber noLocalSubscriber = noLocalSession.createDurableSubscriber(topic, noLocalSubscriptionName, null, true))
                {
                    sessionProducer.send(session.createTextMessage("Message3"));
                    session.commit();

                    final Message durableSubscriberMessage = noLocalSubscriber.receive(getReceiveTimeout());
                    assertInstanceOf(TextMessage.class, durableSubscriberMessage);
                    assertEquals("Message3", ((TextMessage) durableSubscriberMessage).getText(),
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
    public void testNoLocalSubscriberAndSubscriberOnSeparateConnection() throws Exception
    {
        String noLocalSubscriptionName = getTestName() + "_no_local_sub";
        String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        final String clientId = "clientId";

        try (Connection noLocalConnection = getConnectionBuilder().setClientId(clientId).build())
        {
            try (Connection connection = getConnection())
            {
                Session noLocalSession = noLocalConnection.createSession(true, Session.SESSION_TRANSACTED);
                Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

                MessageProducer noLocalSessionProducer = noLocalSession.createProducer(topic);
                MessageProducer sessionProducer = session.createProducer(topic);

                try
                {
                    TopicSubscriber noLocalSubscriber =
                            noLocalSession.createDurableSubscriber(topic, noLocalSubscriptionName, null, true);
                    TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriptionName, null, false);
                    noLocalConnection.start();
                    connection.start();

                    noLocalSessionProducer.send(noLocalSession.createTextMessage("Message1"));
                    noLocalSession.commit();
                    sessionProducer.send(session.createTextMessage("Message2"));
                    sessionProducer.send(session.createTextMessage("Message3"));
                    session.commit();

                    Message durableSubscriberMessage = noLocalSubscriber.receive(getReceiveTimeout());
                    assertInstanceOf(TextMessage.class, durableSubscriberMessage);
                    assertEquals("Message2", ((TextMessage) durableSubscriberMessage).getText(),
                            "Unexpected local message received");
                    noLocalSession.commit();

                    Message nonDurableSubscriberMessage = subscriber.receive(getReceiveTimeout());
                    assertInstanceOf(TextMessage.class, nonDurableSubscriberMessage);
                    assertEquals("Message1", ((TextMessage) nonDurableSubscriberMessage).getText(),
                            "Unexpected message received");

                    session.commit();
                    noLocalSubscriber.close();
                    subscriber.close();
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
    public void testResubscribeWithChangedNoLocal() throws Exception
    {
        assumeTrue(is(equalTo(Protocol.AMQP_1_0)).matches(getProtocol()), "QPID-8068");
        String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        String clientId = "testClientId";
        Connection connection = getConnectionBuilder().setClientId(clientId).build();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            TopicSubscriber durableSubscriber =
                    session.createDurableSubscriber(topic, subscriptionName, null, false);

            MessageProducer producer = session.createProducer(topic);
            producer.send(session.createTextMessage("A"));
            producer.send(session.createTextMessage("B"));
            session.commit();

            connection.start();

            Message receivedMessage = durableSubscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, receivedMessage, "TextMessage should be received");
            assertEquals("A", ((TextMessage)receivedMessage).getText(), "Unexpected message received");

            session.commit();
        }
        finally
        {
            connection.close();
        }

        connection = getConnectionBuilder().setClientId(clientId).build();
        try
        {
            connection.start();

            Session session2 = connection.createSession(true, Session.SESSION_TRANSACTED);
            TopicSubscriber noLocalSubscriber2 = session2.createDurableSubscriber(topic, subscriptionName, null, true);

            try (Connection secondConnection = getConnectionBuilder().setClientId("secondConnection").build())
            {
                Session secondSession = secondConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer secondProducer = secondSession.createProducer(topic);
                secondProducer.send(secondSession.createTextMessage("C"));
            }

            Message noLocalSubscriberMessage = noLocalSubscriber2.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, noLocalSubscriberMessage, "TextMessage should be received");
            assertEquals("C", ((TextMessage)noLocalSubscriberMessage).getText(),
                    "Unexpected message received");
        }
        finally
        {
            connection.close();
        }
    }

    /**
     * create and register a durable subscriber with a message selector and then close it
     * crash the broker
     * create a publisher and send  5 right messages and 5 wrong messages
     * recreate the durable subscriber and check we receive the 5 expected messages
     */
    @Test
    public void testMessageSelectorRecoveredOnBrokerRestart() throws Exception
    {
        assumeTrue(getBrokerAdmin().supportsRestart());

        final Topic topic = createTopic(getTestName());

        String clientId = "testClientId";
        String subscriptionName = getTestName() + "_sub";
        try (TopicConnection subscriberConnection = (TopicConnection) getConnectionBuilder().setClientId(clientId).build())
        {
            TopicSession session = subscriberConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);
            subscriberConnection.start();
            subscriber.close();
            session.close();
        }

        getBrokerAdmin().restart();

        try (TopicConnection connection = (TopicConnection) getConnectionBuilder().setClientId(clientId).build())
        {
            TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicPublisher publisher = session.createPublisher(topic);
            for (int i = 0; i < 10; i++) {
                Message message = session.createMessage();
                message.setStringProperty("testprop", String.valueOf(i % 2 == 0));
                publisher.publish(message);
            }
            publisher.close();
            session.close();
        }

        try (TopicConnection subscriberConnection2 = (TopicConnection) getConnectionBuilder().setClientId(clientId).build())
        {
            TopicSession session = subscriberConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);
            subscriberConnection2.start();
            for (int i = 0; i < 5; i++)
            {
                Message message = subscriber.receive(1000);
                if (message == null)
                {
                    fail(String.format("Message '%d' was received", i));
                }
                else
                {
                    assertEquals("true", message.getStringProperty("testprop"),
                            String.format("Received message %d with not matching selector", i));
                }
            }
            subscriber.close();
            session.unsubscribe(subscriptionName);
        }
    }

    /**
     * create and register a durable subscriber without a message selector and then unsubscribe it
     * create and register a durable subscriber with a message selector and then close it
     * restart the broker
     * send matching and non matching messages
     * recreate and register the durable subscriber with a message selector
     * verify only the matching messages are received
     */
    @Test
    public void testChangeSubscriberToHaveSelector() throws Exception
    {
        assumeTrue(getBrokerAdmin().supportsRestart());

        final String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        String testClientId = "testClientId";

        try (TopicConnection subscriberConnection = (TopicConnection) getConnectionBuilder().setClientId(testClientId).build())
        {
            TopicSession session = subscriberConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriptionName);

            TopicPublisher publisher = session.createPublisher(topic);
            publisher.send(session.createTextMessage("Message1"));
            publisher.send(session.createTextMessage("Message2"));

            subscriberConnection.start();
            Message receivedMessage = subscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message1", ((TextMessage) receivedMessage).getText(),
                    "Unexpected message content");

            subscriber.close();
            session.close();
        }

        //create and register a durable subscriber with a message selector and then close it
        try (TopicConnection subscriberConnection2 = (TopicConnection) getConnectionBuilder().setClientId(testClientId).build())
        {
            TopicSession session = subscriberConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);

            TopicPublisher publisher = session.createPublisher(topic);
            TextMessage message = session.createTextMessage("Message3");
            message.setStringProperty("testprop", "false");
            publisher.send(message);
            message = session.createTextMessage("Message4");
            message.setStringProperty("testprop", "true");
            publisher.send(message);

            subscriberConnection2.start();

            Message receivedMessage = subscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message4", ((TextMessage) receivedMessage).getText(),
                    "Unexpected message content");

            subscriber.close();
            session.close();
        }

        getBrokerAdmin().restart();

        try (TopicConnection publisherConnection = getTopicConnection())
        {
            TopicSession session = publisherConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicPublisher publisher = session.createPublisher(topic);
            for (int i = 0; i < 10; i++) {
                Message message = session.createMessage();
                message.setStringProperty("testprop", String.valueOf(i % 2 == 0));
                publisher.publish(message);
            }
            publisher.close();
            session.close();
        }

        try (TopicConnection subscriberConnection3 = (TopicConnection) getConnectionBuilder().setClientId(testClientId).build())
        {
            TopicSession session = (TopicSession) subscriberConnection3.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);
            subscriberConnection3.start();

            for (int i = 0; i < 5; i++)
            {
                Message message = subscriber.receive(2000);
                if (message == null)
                {
                    fail(String.format("Message '%d'  was not received", i));
                }
                else
                {
                    assertEquals("true", message.getStringProperty("testprop"),
                            String.format("Received message %d with not matching selector", i));
                }
            }

            subscriber.close();
            session.unsubscribe(subscriptionName);
            session.close();
        }
    }

    /**
     * create and register a durable subscriber with a message selector and then unsubscribe it
     * create and register a durable subscriber without a message selector and then close it
     * restart the broker
     * send matching and non matching messages
     * recreate and register the durable subscriber without a message selector
     * verify ALL the sent messages are received
     */
    @Test
    public void testChangeSubscriberToHaveNoSelector() throws Exception
    {
        assumeTrue(getBrokerAdmin().supportsRestart());

        final String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        String clientId = "testClientId";

        //create and register a durable subscriber with selector then unsubscribe it
        try (TopicConnection durConnection = (TopicConnection) getConnectionBuilder().setClientId(clientId).build())
        {
            TopicSession session = durConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);

            TopicPublisher publisher = session.createPublisher(topic);
            TextMessage message = session.createTextMessage("Messag1");
            message.setStringProperty("testprop", "false");
            publisher.send(message);
            message = session.createTextMessage("Message2");
            message.setStringProperty("testprop", "true");
            publisher.send(message);

            message = session.createTextMessage("Message3");
            message.setStringProperty("testprop", "true");
            publisher.send(message);

            durConnection.start();

            Message receivedMessage = subscriber.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message2", ((TextMessage) receivedMessage).getText(),
                    "Unexpected message content");

            subscriber.close();
            session.close();
        }

        //create and register a durable subscriber without the message selector and then close it
        try (TopicConnection subscriberConnection2 = (TopicConnection) getConnectionBuilder().setClientId(clientId).build())
        {
            TopicSession session = subscriberConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriptionName);
            subscriberConnection2.start();
            subscriber.close();
            session.close();
        }

        //send messages matching and not matching the original used selector
        try (TopicConnection publisherConnection = getTopicConnection())
        {
            TopicSession session = publisherConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicPublisher publisher = session.createPublisher(topic);
            for (int i = 1; i <= 10; i++)
            {
                Message message = session.createMessage();
                message.setStringProperty("testprop", String.valueOf(i % 2 == 0));
                publisher.publish(message);
            }
            publisher.close();
            session.close();
        }

        getBrokerAdmin().restart();

        try (TopicConnection subscriberConnection3 = (TopicConnection) getConnectionBuilder().setClientId(clientId).build())
        {
            TopicSession session = (TopicSession) subscriberConnection3.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriptionName);
            subscriberConnection3.start();

            for (int i = 1; i <= 10; i++)
            {
                Message message = subscriber.receive(2000);
                if (message == null)
                {
                    fail(String.format("Message %d  was not received", i));
                }
            }

            subscriber.close();
            session.unsubscribe(subscriptionName);
            session.close();
        }
    }

    @Test
    public void testResubscribeWithChangedSelector() throws Exception
    {
        assumeTrue(getBrokerAdmin().supportsRestart());

        String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        String clientId = "testClientId";

        try (TopicConnection connection = (TopicConnection) getConnectionBuilder().setClientId(clientId).build())
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(topic);

            // Create durable subscriber that matches A
            TopicSubscriber subscriberA =
                    session.createDurableSubscriber(topic, subscriptionName, "Match = True", false);

            // Send 1 non-matching message and 1 matching message
            TextMessage message = session.createTextMessage("Message1");
            message.setBooleanProperty("Match", false);
            producer.send(message);
            message = session.createTextMessage("Message2");
            message.setBooleanProperty("Match", true);
            producer.send(message);

            Message receivedMessage = subscriberA.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message2", ((TextMessage) receivedMessage).getText(), "Unexpected message content");

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
            TopicSubscriber subscriberB = session.createDurableSubscriber(topic,
                    subscriptionName,
                    "Match = False", false);

            // Check that new messages are received properly
            message = session.createTextMessage("Message5");
            message.setBooleanProperty("Match", true);
            producer.send(message);
            message = session.createTextMessage("Message6");
            message.setBooleanProperty("Match", false);
            producer.send(message);

            // changing the selector should have cleared the queue so we expect message 6 instead of message 4
            receivedMessage = subscriberB.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message6", ((TextMessage) receivedMessage).getText(),
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
        getBrokerAdmin().restart();

        // Reconnect to broker
        try (TopicConnection connection2 = (TopicConnection) getConnectionBuilder().setClientId(clientId).build())
        {
            connection2.start();
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Reconnect with new selector that matches B
            TopicSubscriber subscriberC =
                    session.createDurableSubscriber(topic, subscriptionName, "Match = False", false);

            //check the dur sub's underlying queue now has msg count 1
            Message receivedMessage = subscriberC.receive(getReceiveTimeout());
            assertInstanceOf(TextMessage.class, receivedMessage);
            assertEquals("Message8", ((TextMessage) receivedMessage).getText(), "Unexpected message content");

            subscriberC.close();
            session.unsubscribe(subscriptionName);

            session.close();
        }
    }
}
