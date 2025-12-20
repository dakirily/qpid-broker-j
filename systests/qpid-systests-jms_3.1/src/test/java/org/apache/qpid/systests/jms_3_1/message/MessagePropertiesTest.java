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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on message property semantics.
 *
 * The tests in this class are aligned with:
 * - 3.5.1 "Property names"
 * - 3.5.3 "Using properties"
 * - 3.5.4 "Property value conversion"
 * - 3.5.6 "Property iteration"
 * - 3.5.8 "Non-existent properties"
 */
@JmsSystemTest
@Tag("message")
@Tag("queue")
class MessagePropertiesTest
{
    /**
     * Verifies that property names must obey the rules for selector identifiers
     * and that invalid property names are rejected (3.5.1).
     */
    @Test
    void propertyNamesMustBeSelectorIdentifiers(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var message = session.createMessage();
            assertThrows(IllegalArgumentException.class,
                    () -> message.setStringProperty("invalid-name", "value"),
                    "Property name should follow selector identifier rules");
        }
    }

    /**
     * Verifies that properties are read-only after a message is received
     * and attempting to modify them causes MessageNotWriteableException (3.5.3).
     */
    @Test
    void propertiesAreReadOnlyAfterReceive(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            final var message = session.createMessage();
            message.setStringProperty("name", "value");
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();
            final Message received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");

            assertThrows(MessageNotWriteableException.class,
                    () -> received.setStringProperty("name", "newValue"),
                    "Properties should be read-only on a received message");
        }
    }

    /**
     * Verifies supported property conversions and expected failures for unsupported conversions,
     * following the conversion table in 3.5.4.
     */
    @Test
    void propertyValueConversionsAndTypeErrors(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            final var message = session.createMessage();
            message.setIntProperty("intProp", 7);
            message.setStringProperty("stringNumber", "42");
            message.setStringProperty("badNumber", "notANumber");
            message.setStringProperty("nullString", null);
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();
            final Message received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");

            assertEquals(7L, received.getLongProperty("intProp"), "int to long conversion should succeed");
            assertEquals("7", received.getStringProperty("intProp"), "int to string conversion should succeed");
            assertThrows(MessageFormatException.class,
                    () -> received.getBooleanProperty("intProp"),
                    "int to boolean conversion should be rejected");

            assertEquals(42, received.getIntProperty("stringNumber"),
                    "String numeric conversion should succeed");
            assertThrows(NumberFormatException.class,
                    () -> received.getIntProperty("badNumber"),
                    "Invalid numeric String conversion should fail");
            assertThrows(NumberFormatException.class,
                    () -> received.getIntProperty("nullString"),
                    "Null String conversion to primitive should fail");
        }
    }

    /**
     * Verifies that property enumeration includes the properties set by the client
     * and allows iteration over names (3.5.6).
     */
    @Test
    void propertyEnumerationIncludesSetProperties(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            final var message = session.createMessage();
            message.setIntProperty("alpha", 1);
            message.setStringProperty("beta", "b");
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();
            final Message received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");

            final Enumeration<?> names = received.getPropertyNames();
            final Set<String> nameSet = new HashSet<>();
            while (names.hasMoreElements())
            {
                nameSet.add((String) names.nextElement());
            }

            assertTrue(nameSet.contains("alpha"), "Property 'alpha' should be listed");
            assertTrue(nameSet.contains("beta"), "Property 'beta' should be listed");
        }
    }

    /**
     * Verifies that reading a property that has not been set behaves as if the
     * property exists with a null value (3.5.8).
     */
    @Test
    void nonExistentPropertiesReturnNull(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            producer.send(session.createMessage());

            final var consumer = session.createConsumer(queue);
            connection.start();
            final Message received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be received");

            assertNull(received.getStringProperty("missing"), "Missing String property should be null");
            assertNull(received.getObjectProperty("missing"), "Missing Object property should be null");
            assertFalse(received.propertyExists("missing"), "Missing property should not exist");
        }
    }
}
