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

package org.apache.qpid.systests.jms_3_1.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Enumeration;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageFormatException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("message")
@Tag("queue")
class JMSHeadersAndPropertiesTest
{
    @Test
    void resentJMSMessageGetsReplacementJMSMessageID(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var producer = session.createProducer(queue);
            final var sentMessage = session.createMessage();
            producer.send(sentMessage);

            final var originalId = sentMessage.getJMSMessageID();
            assertNotNull(originalId, "JMSMessageID must be set after first publish");

            producer.send(sentMessage);

            final var firstResendID = sentMessage.getJMSMessageID();
            assertNotNull(firstResendID, "JMSMessageID must be set after first resend");
            assertNotSame(originalId, firstResendID, "JMSMessageID must be changed second publish");
        }
    }

    @Test
    void redelivered(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();;
        try (final var connection = jms.builder().connection().prefetch(1).create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var producer = session.createProducer(queue))
        {
            producer.send(session.createTextMessage("A"));
            producer.send(session.createTextMessage("B"));
            session.commit();

            final var consumer = session.createConsumer(queue);
            connection.start();

            var message = consumer.receive(Timeouts.receiveMillis());
            var textMessage = assertInstanceOf(TextMessage.class, message, "TextMessage should be received");
            assertFalse(message.getJMSRedelivered(), "Unexpected JMSRedelivered after first receive");
            assertEquals("A", textMessage.getText(), "Unexpected message content");

            session.rollback();

            message = consumer.receive(Timeouts.receiveMillis());
            textMessage = assertInstanceOf(TextMessage.class, message, "TextMessage should be received");
            assertTrue(message.getJMSRedelivered(), "Unexpected JMSRedelivered after second receive");
            assertEquals("A", textMessage.getText(), "Unexpected message content");

            message = consumer.receive(Timeouts.receiveMillis());
            textMessage = assertInstanceOf(TextMessage.class, message, "TextMessage should be received");
            assertFalse(message.getJMSRedelivered(), "Unexpected JMSRedelivered for second message");
            assertEquals("B", textMessage.getText(), "Unexpected message content");

            session.commit();
        }
    }

    @Test
    void headers(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final var replyTo = jms.builder().queue().nameSuffix("_replyTo").create();
        try (final var consumerConnection = jms.builder().connection().create();
             final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = consumerSession.createConsumer(queue))
        {
            final var correlationId = "testCorrelationId";
            final var jmsType = "testJmsType";

            final int priority = 1;
            final long timeToLive = 30 * 60 * 1000;

            try (final var producerConnection = jms.builder().connection().create();
                 final var producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 final var producer = producerSession.createProducer(queue))
            {
                final var message = producerSession.createMessage();
                message.setJMSCorrelationID(correlationId);
                message.setJMSType(jmsType);
                message.setJMSReplyTo(replyTo);

                long currentTime = System.currentTimeMillis();
                producer.send(message, DeliveryMode.NON_PERSISTENT, priority, timeToLive);

                consumerConnection.start();

                final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(receivedMessage);

                assertEquals(correlationId, receivedMessage.getJMSCorrelationID(), "JMSCorrelationID mismatch");
                assertEquals(message.getJMSType(), receivedMessage.getJMSType(), "JMSType mismatch");
                assertEquals(message.getJMSReplyTo(), receivedMessage.getJMSReplyTo(), "JMSReply To mismatch");
                assertTrue(receivedMessage.getJMSMessageID().startsWith("ID:"), "JMSMessageID does not start 'ID:'");
                assertEquals(priority, receivedMessage.getJMSPriority(), "JMSPriority mismatch");
                assertTrue(receivedMessage.getJMSExpiration() >= currentTime + timeToLive &&
                                receivedMessage.getJMSExpiration() <= System.currentTimeMillis() + timeToLive,
                        "Unexpected JMSExpiration: got '%d', but expected value equals or greater than '%d'"
                                .formatted(receivedMessage.getJMSExpiration(), currentTime + timeToLive));
            }
        }
    }

    @Test
    void groupIDAndGroupSeq(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            assumeTrue(isJMSXPropertySupported(connection, "JMSXGroupID"));
            assumeTrue(isJMSXPropertySupported(connection, "JMSXGroupSeq"));

            final var groupId = "testGroup";
            final int groupSequence = 3;
            final var queue = jms.builder().queue().create();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var producer = session.createProducer(queue);
            final var consumer = session.createConsumer(queue);
            final var message = session.createMessage();
            message.setStringProperty("JMSXGroupID", groupId);
            message.setIntProperty("JMSXGroupSeq", groupSequence);

            producer.send(message);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage);

            assertEquals(groupId, receivedMessage.getStringProperty("JMSXGroupID"),
                    "Unexpected JMSXGroupID");
            assertEquals(groupSequence, receivedMessage.getIntProperty("JMSXGroupSeq"),
                    "Unexpected JMSXGroupSeq");
        }
    }

    @Test
    void propertyValues(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue))
        {
            final var message = session.createMessage();
            message.setBooleanProperty("boolean", true);
            message.setByteProperty("byte", Byte.MAX_VALUE);
            message.setShortProperty("short", Short.MAX_VALUE);
            message.setIntProperty("int", Integer.MAX_VALUE);
            message.setFloatProperty("float", Float.MAX_VALUE);
            message.setDoubleProperty("double", Double.MAX_VALUE);

            producer.send(message);

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            final Message receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage);

            assertEquals(Boolean.TRUE, message.getBooleanProperty("boolean"),
                    "Unexpected boolean property value");
            assertEquals(Byte.MAX_VALUE, message.getByteProperty("byte"), "Unexpected byte property value");
            assertEquals(Short.MAX_VALUE, message.getShortProperty("short"),
                    "Unexpected short property value");
            assertEquals(Integer.MAX_VALUE, message.getIntProperty("int"), "Unexpected int property value");
            assertEquals(Float.MAX_VALUE, message.getFloatProperty("float"), 0f,
                    "Unexpected float property value");
            assertEquals(Double.MAX_VALUE, message.getDoubleProperty("double"), 0d,
                    "Unexpected double property value");
        }
    }

    @Test
    void unsupportedObjectPropertyValue(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue))
        {
            final var message = session.createMessage();

            assertThrows(MessageFormatException.class,
                    () -> message.setObjectProperty("invalidObject", new Exception()),
                    "Expected exception not thrown");

            final var validValue = "validValue";
            message.setObjectProperty("validObject", validValue);
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage);

            assertFalse(message.propertyExists("invalidObject"), "Unexpected property found");
            assertEquals(validValue, message.getObjectProperty("validObject"), "Unexpected property value");
        }
    }

    @Test
    void disableJMSMessageId(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue))
        {
            final var message = session.createMessage();
            producer.send(message);
            assertNotNull(message.getJMSMessageID(), "Produced message is expected to have a JMSMessageID");

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessageWithId = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessageWithId);
            assertNotNull(receivedMessageWithId.getJMSMessageID(),
                    "Received message is expected to have a JMSMessageID");
            assertEquals(message.getJMSMessageID(), receivedMessageWithId.getJMSMessageID(),
                    "Received message JMSMessageID should match the sent");

            producer.setDisableMessageID(true);
            producer.send(message);
            assertNull(message.getJMSMessageID(), "Produced message is expected to not have a JMSMessageID");

            final var receivedMessageWithoutId = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessageWithoutId);
            assertNull(receivedMessageWithoutId.getJMSMessageID(),
                    "Received message is not expected to have a JMSMessageID");
        }
    }

    private boolean isJMSXPropertySupported(final Connection connection, final String propertyName) throws JMSException
    {
        final Enumeration<?> props = connection.getMetaData().getJMSXPropertyNames();
        while (props.hasMoreElements())
        {
            final var name = (String) props.nextElement();
            if (name.equals(propertyName))
            {
                return true;
            }
        }
        return false;
    }
}
