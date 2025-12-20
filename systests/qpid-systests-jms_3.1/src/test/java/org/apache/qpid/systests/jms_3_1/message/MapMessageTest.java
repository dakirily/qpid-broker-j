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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

import java.util.Map;

@JmsSystemTest
@Tag("message")
@Tag("queue")
class MapMessageTest
{
    private static final byte[] BYTES = { 99, 98, 97, 96, 95 };
    private static final String MESSAGE_ASCII = "Message";
    private static final String MESSAGE_NON_ASCII_UTF8 = "YEN\u00A5EURO\u20AC";
    private static final float SMALL_FLOAT = 100f;

    @Test
    void sendAndReceiveEmpty(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue))
        {
            final var message = session.createMapMessage();
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(MapMessage.class, receivedMessage, "BytesMessage should be received");
            assertFalse(((MapMessage) receivedMessage).getMapNames().hasMoreElements(),
                    "Unexpected map content");
        }
    }

    @Test
    void sendAndReceiveBody(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue))
        {
            final var message = session.createMapMessage();
            setMapValues(message);
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            assertInstanceOf(MapMessage.class, receivedMessage, "MapMessage should be received");
            assertTrue(((MapMessage) receivedMessage).getMapNames().hasMoreElements(), "Unexpected map content");

            final var receivedMapMessage = (MapMessage) receivedMessage;
            testMapValues(receivedMapMessage);
        }
    }

    private void setMapValues(final MapMessage message) throws JMSException
    {
        message.setBoolean("bool", true);
        message.setByte("byte",Byte.MAX_VALUE);
        message.setBytes("bytes", BYTES);
        message.setChar("char",'c');
        message.setDouble("double", Double.MAX_VALUE);
        message.setFloat("float", Float.MAX_VALUE);
        message.setFloat("smallfloat", SMALL_FLOAT);
        message.setInt("int",  Integer.MAX_VALUE);
        message.setLong("long",  Long.MAX_VALUE);
        message.setShort("short", Short.MAX_VALUE);
        message.setString("string-ascii", MESSAGE_ASCII);
        message.setString("string-utf8", MESSAGE_NON_ASCII_UTF8);

        // Test Setting Object Values
        message.setObject("object-bool", true);
        message.setObject("object-byte", Byte.MAX_VALUE);
        message.setObject("object-bytes", BYTES);
        message.setObject("object-char", 'c');
        message.setObject("object-double", Double.MAX_VALUE);
        message.setObject("object-float", Float.MAX_VALUE);
        message.setObject("object-int", Integer.MAX_VALUE);
        message.setObject("object-long", Long.MAX_VALUE);
        message.setObject("object-short", Short.MAX_VALUE);

        // Set a null String value
        message.setString("nullString", null);
        // Highlight protocol problem
        message.setString("emptyString", "");
    }

    private void testMapValues(final MapMessage m) throws JMSException
    {
        // Test get<Primitive>

        // Boolean
        assertTrue(m.getBoolean("bool"));
        assertEquals(Boolean.TRUE.toString(), m.getString("bool"));

        // Byte
        assertEquals(Byte.MAX_VALUE, m.getByte("byte"));
        assertEquals(String.valueOf(Byte.MAX_VALUE), m.getString("byte"));

        // Bytes
        assertArrayEquals(BYTES, m.getBytes("bytes"));

        // Char
        assertEquals('c', m.getChar("char"));

        // Double
        assertEquals(Double.MAX_VALUE, m.getDouble("double"), 0);
        assertEquals("" + Double.MAX_VALUE, m.getString("double"));

        // Float
        assertEquals(Float.MAX_VALUE, m.getFloat("float"), 0);
        assertEquals(SMALL_FLOAT, (float) m.getDouble("smallfloat"), 0);
        assertEquals("" + Float.MAX_VALUE, m.getString("float"));

        // Integer
        assertEquals(Integer.MAX_VALUE, m.getInt("int"));
        assertEquals("" + Integer.MAX_VALUE, m.getString("int"));

        // long
        assertEquals(Long.MAX_VALUE, m.getLong("long"));
        assertEquals("" + Long.MAX_VALUE, m.getString("long"));

        // Short
        assertEquals(Short.MAX_VALUE, m.getShort("short"));
        assertEquals("" + Short.MAX_VALUE, m.getString("short"));
        assertEquals((int) Short.MAX_VALUE, m.getInt("short"));

        // String
        assertEquals(MESSAGE_ASCII, m.getString("string-ascii"));
        assertEquals(MESSAGE_NON_ASCII_UTF8, m.getString("string-utf8"));

        // Test getObjects
        assertEquals(true, m.getObject("object-bool"));
        assertEquals(Byte.MAX_VALUE, m.getObject("object-byte"));
        assertArrayEquals(BYTES, (byte[]) m.getObject("object-bytes"));
        assertEquals('c', m.getObject("object-char"));
        assertEquals(Double.MAX_VALUE, m.getObject("object-double"));
        assertEquals(Float.MAX_VALUE, m.getObject("object-float"));
        assertEquals(Integer.MAX_VALUE, m.getObject("object-int"));
        assertEquals(Long.MAX_VALUE, m.getObject("object-long"));
        assertEquals(Short.MAX_VALUE, m.getObject("object-short"));

        // Check Special values
        assertNull(m.getString("nullString"));
        assertEquals("", m.getString("emptyString"));
    }

    /**
     * Verifies that {@link MapMessage} bodies can be obtained as a {@link Map} via
     * {@link Message#getBody(Class)} and that the map contains the expected key/value pairs.
     *
     * Spec references:
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jakarta-messaging-message-body">3.11 "Jakarta Messaging message body"</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#a315-new-method-to-extract-the-body-directly-from-a-message-jms_spec-101">A.3.15 "New method to extract the body directly from a Message"</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#getBody(java.lang.Class)">Message.getBody(Class)</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/message#isBodyAssignableTo(java.lang.Class)">Message.isBodyAssignableTo(Class)</a>
     * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">10.3 "Standard exceptions"</a>
     */
    @Test
    void mapMessageGetBody(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(queue);
             final var consumer = session.createConsumer(queue))
        {
            final var message = session.createMapMessage();
            message.setString("k", "v");
            message.setInt("n", 1);
            producer.send(message);

            connection.start();
            final var received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");
            assertInstanceOf(MapMessage.class, received, "MapMessage should be received");

            assertTrue(received.isBodyAssignableTo(Map.class),
                    "Map should be a compatible body type");

            @SuppressWarnings("unchecked")
            final var body = (Map<String, Object>) received.getBody(Map.class);
            assertEquals("v", body.get("k"), "Unexpected map entry");
            assertEquals(1, body.get("n"), "Unexpected map entry");

            assertFalse(received.isBodyAssignableTo(String.class),
                    "String should be an incompatible body type");
            assertThrows(MessageFormatException.class, () -> received.getBody(String.class),
                    "Incompatible body type should cause MessageFormatException");
        }
    }
}
