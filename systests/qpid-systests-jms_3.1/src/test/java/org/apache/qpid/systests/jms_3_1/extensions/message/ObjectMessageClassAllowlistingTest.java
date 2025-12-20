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

package org.apache.qpid.systests.jms_3_1.extensions.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("message")
@Tag("security")
class ObjectMessageClassAllowlistingTest
{
    private static final int TEST_VALUE = 37;

    @Test
    void objectMessage(final JmsSupport jms) throws Exception
    {
        final var destination = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().deserializationPolicyAllowList("*").create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer = session.createConsumer(destination);
            final var producer = session.createProducer(destination);

            sendTestObjectMessage(session, producer);
            
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "did not receive message within receive timeout");
            assertInstanceOf(ObjectMessage.class, receivedMessage, "message is of wrong type");
            final var receivedObjectMessage = (ObjectMessage) receivedMessage;
            final var payloadObject = receivedObjectMessage.getObject();
            assertInstanceOf(HashMap.class, payloadObject, "payload is of wrong type");
            
            final HashMap<String, Integer> payload = (HashMap<String, Integer>) payloadObject;
            assertEquals((Integer) TEST_VALUE, payload.get("value"), "payload has wrong value");
        }
    }

    @Test
    void notAllowListedByConnectionUrlObjectMessage(final JmsSupport jms) throws Exception
    {
        final var destination = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().deserializationPolicyAllowList("org.apache.qpid").create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer = session.createConsumer(destination);
            final var producer = session.createProducer(destination);

            sendTestObjectMessage(session, producer);
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "did not receive message within receive timeout");
            assertInstanceOf(ObjectMessage.class, receivedMessage, "message is of wrong type");
            ObjectMessage receivedObjectMessage = (ObjectMessage) receivedMessage;
            
            assertThrows(MessageFormatException.class, 
                    receivedObjectMessage::getObject,
                    "should not deserialize class");
        }
    }

    @Test
    void allowListedClassByConnectionUrlObjectMessage(final JmsSupport jms) throws Exception
    {
        final var destination = jms.builder().queue().create();
        try (final var connection = jms.builder().connection()
                .deserializationPolicyAllowList("java.util.HashMap,java.lang")
                .create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer = session.createConsumer(destination);
            final var producer = session.createProducer(destination);

            sendTestObjectMessage(session, producer);
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "did not receive message within receive timeout");
            assertInstanceOf(ObjectMessage.class, receivedMessage, "message is of wrong type");
            final var receivedObjectMessage = (ObjectMessage) receivedMessage;

            final HashMap<String, Integer> object = (HashMap<String, Integer>) receivedObjectMessage.getObject();
            assertEquals((Integer) TEST_VALUE, object.get("value"), "Unexpected value");
        }
    }

    @Test
    void denyListedClassByConnectionUrlObjectMessage(final JmsSupport jms) throws Exception
    {
        final var destination = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().deserializationPolicyAllowList("java")
                .deserializationPolicyDenyList("java.lang.Integer")
                .create())
        {
            connection.start();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var consumer = session.createConsumer(destination);
            final var producer = session.createProducer(destination);

            sendTestObjectMessage(session, producer);
            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "did not receive message within receive timeout");
            assertInstanceOf(ObjectMessage.class, receivedMessage, "message is of wrong type");
            final var receivedObjectMessage = (ObjectMessage) receivedMessage;

            assertThrows(JMSException.class,
                    receivedObjectMessage::getObject,
                    "Should not be allowed to deserialize black listed class");
        }
    }

    @Test
    void allowListedAnonymousClassByConnectionUrlObjectMessage(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection()
                .deserializationPolicyAllowList(ObjectMessageClassAllowlistingTest.class.getCanonicalName())
                .create())
        {
            doTestAllowListedEnclosedClassTest(jms, connection, createAnonymousObject(TEST_VALUE));
        }
    }

    @Test
    void denyListedAnonymousClassByConnectionUrlObjectMessage(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection()
                .deserializationPolicyAllowList(ObjectMessageClassAllowlistingTest.class.getPackage().getName())
                .deserializationPolicyDenyList(ObjectMessageClassAllowlistingTest.class.getCanonicalName())
                .create())
        {
            doTestDenyListedEnclosedClassTest(jms, connection, createAnonymousObject(TEST_VALUE));
        }
    }

    @Test
    void allowListedNestedClassByConnectionUrlObjectMessage(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection()
                .deserializationPolicyAllowList(NestedClass.class.getCanonicalName())
                .create())
        {
            doTestAllowListedEnclosedClassTest(jms, connection, new NestedClass(TEST_VALUE));
        }
    }

    @Test
    void denyListedNestedClassByConnectionUrlObjectMessage(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection()
                .deserializationPolicyAllowList(ObjectMessageClassAllowlistingTest.class.getCanonicalName())
                .deserializationPolicyDenyList(NestedClass.class.getCanonicalName())
                .create())
        {
            doTestDenyListedEnclosedClassTest(jms, connection, new NestedClass(TEST_VALUE));
        }
    }

    private void doTestAllowListedEnclosedClassTest(final JmsSupport jms, Connection connection, Serializable content) throws Exception
    {
        final var destination = jms.builder().queue().create();
        connection.start();

        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = session.createConsumer(destination);
             final var producer = session.createProducer(destination))
        {
            final ObjectMessage sendMessage = session.createObjectMessage();
            sendMessage.setObject(content);
            producer.send(sendMessage);

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "did not receive message within receive timeout");
            assertInstanceOf(ObjectMessage.class, receivedMessage, "message is of wrong type");
            final var receivedObject = ((ObjectMessage) receivedMessage).getObject();
            assertEquals(content.getClass(), receivedObject.getClass(), "Received object has unexpected class");
            assertEquals(content, receivedObject, "Received object has unexpected content");
        }
    }

    private void doTestDenyListedEnclosedClassTest(final JmsSupport jms, final Connection connection, final Serializable content) throws Exception
    {
        final var destination = jms.builder().queue().create();
        connection.start();

        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = session.createConsumer(destination);
             final var producer = session.createProducer(destination))
        {
            final var sendMessage = session.createObjectMessage();
            sendMessage.setObject(content);
            producer.send(sendMessage);

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(receivedMessage, "did not receive message within receive timeout");
            assertInstanceOf(ObjectMessage.class, receivedMessage, "message is of wrong type");

            assertThrows(MessageFormatException.class,
                    () -> ((ObjectMessage) receivedMessage).getObject(),
                    "Exception not thrown");
        }
    }

    private void sendTestObjectMessage(final Session s, final MessageProducer producer) throws JMSException
    {
        final HashMap<String, Integer> messageContent = new HashMap<>();
        messageContent.put("value", TEST_VALUE);
        final var objectMessage = s.createObjectMessage(messageContent);
        producer.send(objectMessage);
    }

    public static Serializable createAnonymousObject(final int field)
    {
        return new Serializable()
        {
            private final int _field = field;

            @Override
            public int hashCode()
            {
                return _field;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final Serializable that = (Serializable) o;

                return getFieldValueByReflection(that).equals(_field);
            }

            private Object getFieldValueByReflection(final Serializable that)
            {
                try
                {
                    final Field field = that.getClass().getDeclaredField("_field");
                    field.setAccessible(true);
                    return field.get(that);
                }
                catch (NoSuchFieldException | IllegalAccessException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static class NestedClass implements Serializable
    {
        private final int _field;

        public NestedClass(final int field)
        {
            _field = field;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final NestedClass that = (NestedClass) o;

            return _field == that._field;
        }

        @Override
        public int hashCode()
        {
            return _field;
        }
    }
}
