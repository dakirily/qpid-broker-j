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

package org.apache.qpid.systests.jms_3_1.message;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("message")
@Tag("queue")
class ObjectMessageTest
{
    @Test
    void sendAndReceive(final JmsSupport jms) throws Exception
    {
        final var test = UUID.randomUUID();
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var testMessage = session.createObjectMessage(test);
            final var object = testMessage.getObject();

            assertNotNull(object, "Object was null");
            assertNotNull(testMessage.toString(), "toString returned null");

            final var producer = session.createProducer(queue);
            producer.send(testMessage);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");

            var result = ((ObjectMessage) receivedMessage).getObject();
            assertEquals(test, result, "First read: UUIDs were not equal");

            result = ((ObjectMessage) receivedMessage).getObject();
            assertEquals(test, result, "Second read: UUIDs were not equal");
        }
    }

    @Test
    void sendAndReceiveNull(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var testMessage = session.createObjectMessage(null);
            final var object = testMessage.getObject();

            assertNull(object, "Object was not null");
            assertNotNull(testMessage.toString(), "toString returned null");

            final var producer = session.createProducer(queue);
            producer.send(testMessage);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");

            var result = ((ObjectMessage) receivedMessage).getObject();
            assertNull(result, "First read: UUIDs were not equal");

            result = ((ObjectMessage) receivedMessage).getObject();
            assertNull(result, "Second read: UUIDs were not equal");
        }
    }

    @Test
    void sendEmptyObjectMessage(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var testMessage = session.createObjectMessage();
            final var producer = session.createProducer(queue);
            producer.send(testMessage);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");

            var result = ((ObjectMessage) receivedMessage).getObject();
            assertNull(result, "First read: unexpected object received");

            result = ((ObjectMessage) receivedMessage).getObject();
            assertNull(result, "Second read: unexpected object received");
        }
    }

    @Test
    void sendAndReceiveObject(final JmsSupport jms) throws Exception
    {
        final A a1 = new A(1, "A");
        final A a2 = new A(2, "a");
        final B b = new B(1, "B");
        final C<String, Object> c = new C<>();
        c.put("A1", a1);
        c.put("a2", a2);
        c.put("B", b);
        c.put("String", "String");

        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var testMessage = session.createObjectMessage(c);
            final var producer = session.createProducer(queue);
            producer.send(testMessage);
            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");

            var result = ((ObjectMessage) receivedMessage).getObject();
            assertInstanceOf(C.class, result, "Unexpected object received");

            final C<String, Object> received = (C) result;
            assertEquals(c.size(), received.size(), "Unexpected size");
            assertEquals(new HashSet<>(c.keySet()), new HashSet<>(received.keySet()), "Unexpected keys");

            for (String key : c.keySet())
            {
                assertEquals(c.get(key), received.get(key), "Unexpected value for " + key);
            }
        }
    }

    @Test
    void setObjectPropertyForString(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final var testStringProperty = "TestStringProperty";
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestStringProperty", testStringProperty);
            assertEquals(testStringProperty, msg.getObjectProperty("TestStringProperty"));

            final var producer = session.createProducer(queue);
            producer.send(msg);

            final var consumer = session.createConsumer(queue);
            connection.start();
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());

            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");
            assertEquals(testStringProperty, receivedMessage.getObjectProperty("TestStringProperty"),
                    "Unexpected received property");
        }
    }

    @Test
    void setObjectPropertyForBoolean(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestBooleanProperty", Boolean.TRUE);
            assertEquals(Boolean.TRUE, msg.getObjectProperty("TestBooleanProperty"));

            final var producer = session.createProducer(queue);
            producer.send(msg);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");
            assertEquals(Boolean.TRUE, receivedMessage.getObjectProperty("TestBooleanProperty"),
                    "Unexpected received property");
        }
    }

    @Test
    void setObjectPropertyForByte(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestByteProperty", Byte.MAX_VALUE);
            assertEquals(Byte.MAX_VALUE, msg.getObjectProperty("TestByteProperty"));

            final var producer = session.createProducer(queue);
            producer.send(msg);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");
            assertEquals(Byte.MAX_VALUE, receivedMessage.getObjectProperty("TestByteProperty"),
                    "Unexpected received property");
        }
    }

    @Test
    void setObjectPropertyForShort(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestShortProperty", Short.MAX_VALUE);
            assertEquals(Short.MAX_VALUE, msg.getObjectProperty("TestShortProperty"));

            final var producer = session.createProducer(queue);
            producer.send(msg);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");
            assertEquals(Short.MAX_VALUE, receivedMessage.getObjectProperty("TestShortProperty"),
                    "Unexpected received property");
        }
    }

    @Test
    void setObjectPropertyForInteger(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestIntegerProperty", Integer.MAX_VALUE);
            assertEquals(Integer.MAX_VALUE, msg.getObjectProperty("TestIntegerProperty"));
            final var producer = session.createProducer(queue);
            producer.send(msg);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");
            assertEquals(Integer.MAX_VALUE, receivedMessage.getObjectProperty("TestIntegerProperty"),
                    "Unexpected received property");
        }
    }

    @Test
    void setObjectPropertyForDouble(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestDoubleProperty", Double.MAX_VALUE);
            assertEquals(Double.MAX_VALUE, msg.getObjectProperty("TestDoubleProperty"));

            final var producer = session.createProducer(queue);
            producer.send(msg);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");
            assertEquals(Double.MAX_VALUE, receivedMessage.getObjectProperty("TestDoubleProperty"),
                    "Unexpected received property");
        }
    }

    @Test
    void setObjectPropertyForFloat(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var msg = session.createObjectMessage("test");
            msg.setObjectProperty("TestFloatProperty", Float.MAX_VALUE);
            assertEquals(Float.MAX_VALUE, msg.getObjectProperty("TestFloatProperty"));

            final var producer = session.createProducer(queue);
            producer.send(msg);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");
            assertEquals(Float.MAX_VALUE, receivedMessage.getObjectProperty("TestFloatProperty"),
                    "Unexpected received property");
        }
    }

    @Test
    void clearBodyAndProperties(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final A object = new A(1, "test");
            final var msg = session.createObjectMessage(object);
            msg.setStringProperty("testProperty", "testValue");

            final var producer = session.createProducer(queue);
            producer.send(msg);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(ObjectMessage.class, receivedMessage, "ObjectMessage should be received");

            final var objectMessage = (ObjectMessage) receivedMessage;
            final var received = objectMessage.getObject();
            assertInstanceOf(A.class, received, "Unexpected object type received");
            assertEquals(object, received, "Unexpected object received");
            assertEquals("testValue",   receivedMessage.getStringProperty("testProperty"),
                    "Unexpected property value");

            assertThrows(MessageNotWriteableException.class,
                    () -> objectMessage.setObject("Test text"),
                    "Message should not be writable");

            objectMessage.clearBody();

            assertDoesNotThrow(() -> objectMessage.setObject("Test text"), "Message should be writable");

            assertThrows(MessageNotWriteableException.class,
                    () -> objectMessage.setStringProperty("test", "test"),
                    "Message should not be writable");

            objectMessage.clearProperties();

            assertDoesNotThrow(() -> objectMessage.setStringProperty("test", "test"),
                    "Message should be writable");
        }
    }

    /**
     * Verifies that {@link ObjectMessage} bodies can be obtained via {@link Message#getBody(Class)}
     * when the expected type is known.
     *
     * Spec references:
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jakarta-messaging-message-body">3.11 "Jakarta Messaging message body"</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#a315-new-method-to-extract-the-body-directly-from-a-message-jms_spec-101">A.3.15 "New method to extract the body directly from a Message"</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#getBody(java.lang.Class)">Message.getBody(Class)</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#isBodyAssignableTo(java.lang.Class)">Message.isBodyAssignableTo(Class)</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">10.3 "Standard exceptions"</a>
     */
    @Test
    void objectMessageGetBody(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue);
             final var consumer = session.createConsumer(queue))
        {
            final var expectedBody = UUID.randomUUID();
            final var message = session.createObjectMessage(expectedBody);
            producer.send(message);

            connection.start();
            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");
            assertInstanceOf(ObjectMessage.class, received, "ObjectMessage should be received");

            assertTrue(received.isBodyAssignableTo(UUID.class),
                    "The concrete payload type should be assignable");
            assertEquals(expectedBody, received.getBody(UUID.class), "Unexpected body");

            assertFalse(received.isBodyAssignableTo(String.class),
                    "String should be an incompatible body type");
            assertThrows(MessageFormatException.class, () -> received.getBody(String.class),
                    "Incompatible body type should cause MessageFormatException");
        }
    }

    private static class A implements Serializable
    {
        private final String sValue;
        private final int iValue;

        A (int i, String s)
        {
            sValue = s;
            iValue = i;
        }

        @Override
        public int hashCode()
        {
            return iValue;
        }

        @Override
        public boolean equals(Object o)
        {
            return (o instanceof A) && equals((A) o);
        }

        protected boolean equals(A a)
        {
            return ((a.sValue == null) ? (sValue == null) : a.sValue.equals(sValue)) && (a.iValue == iValue);
        }
    }

    private static class B extends A
    {
        private final long time;

        B (int i, String s)
        {
            super(i, s);
            time = System.currentTimeMillis();
        }

        @Override
        protected boolean equals(A a)
        {
            return super.equals(a) && (a instanceof B) && (time == ((B) a).time);
        }
    }

    private static class C<X, Y> extends HashMap<X, Y> implements Serializable
    {
    }
}
