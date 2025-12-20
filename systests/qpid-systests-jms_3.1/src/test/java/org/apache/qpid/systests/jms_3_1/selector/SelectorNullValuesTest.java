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

package org.apache.qpid.systests.jms_3_1.selector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.jms.Message;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on selector null-value semantics.
 * <br>
 * The tests in this class are aligned with:
 * - 3.8.1.2 "Null values"
 * - 3.8.1.3 "Special notes"
 */
@JmsSystemTest
@Tag("filters")
@Tag("queue")
class SelectorNullValuesTest
{
    /**
     * Verifies that IS NULL matches messages where a property is not set and
     * IS NOT NULL matches messages where the property is set (3.8.1.2).
     */
    @Test
    void isNullMatchesUnsetProperty(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            final var noProperty = session.createMessage();
            final var withProperty = session.createMessage();
            withProperty.setStringProperty("color", "red");

            producer.send(noProperty);
            producer.send(withProperty);

            final var nullConsumer = session.createConsumer(queue, "color IS NULL");
            connection.start();

            final Message nullMatched = nullConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(nullMatched, "Message without property should match IS NULL");
            assertNull(nullMatched.getStringProperty("color"), "Property should be null on matched message");
            nullConsumer.close();

            final var notNullConsumer = session.createConsumer(queue, "color IS NOT NULL");
            final Message notNullMatched = notNullConsumer.receive(Timeouts.receiveMillis());
            assertNotNull(notNullMatched, "Message with property should match IS NOT NULL");
            assertEquals("red", notNullMatched.getStringProperty("color"));
        }
    }

    /**
     * Verifies that a property explicitly set to null is treated as null in selectors (3.8.1.2).
     */
    @Test
    void explicitNullPropertyMatchesIsNull(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().prefetch(0).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            final var message = session.createMessage();
            message.setStringProperty("category", null);
            producer.send(message);

            final var consumer = session.createConsumer(queue, "category IS NULL");
            connection.start();

            final Message received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message with null property should match IS NULL");
            assertNull(received.getStringProperty("category"), "Property should remain null on receive");
        }
    }
}
