/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.qpid.systests.jms_3_1.selector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * System tests for message selector type rules.
 * <br>
 * Jakarta Messaging defines the selector syntax and evaluation rules, including:
 * - the property type used in selectors is the type used to set the property; and
 * - comparisons between non-like types evaluate to false (with a numeric exception).
 * <br>
 * The tests in this class are aligned with:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html#message-selector-syntax">
 *   3.8.1.1 "Message selector syntax"</a>
 */
@JmsSystemTest
@Tag("filters")
@Tag("queue")
class SelectorTypeRulesTest
{
    private static final String ID = "id";
    private static final String NUM = "num";
    private static final String NUMBER_OF_ORDERS = "NumberOfOrders";
    private static final String X = "x";

    /**
     * Verifies that comparisons of non-like types evaluate to false.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html#message-selector-syntax">3.8.1.1</a>
     * states: "Only like type values can be compared" and that comparing non-like types results in "false".
     */
    @Test
    void nonLikeTypeComparisonEvaluatesToFalse(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final MessageProducer producer = session.createProducer(queue))
        {
            sendIntProperty(session, producer, 1, NUM, 5);

            connection.start();

            try (final QueueBrowser all = session.createBrowser(queue);
                 final QueueBrowser nonLikeCompare = session.createBrowser(queue, "num = '5'"))
            {
                // Ensure the message is present on the queue.
                assertEquals(Set.of(1), browseIntProperty(all, ID), "Precondition failed: message not present on queue");

                // int property compared to a String literal => non-like types => must not match.
                assertEquals(Set.of(), browseIntProperty(nonLikeCompare, ID),
                        "Non-like type comparison must evaluate to false");
            }

            jms.queue().drain();
        }
    }

    /**
     * Verifies that selector evaluation uses the property type set on the message (no implicit "getXxx" conversions).
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html#message-selector-syntax">3.8.1.1</a>
     * states: "The type of a property value ... corresponds to the type used to set the property".
     * It also states: "Only like type values can be compared".
     * <br>
     * This test sends two messages:
     * - one with {@code NumberOfOrders} set as a String ("2"); and
     * - one with {@code NumberOfOrders} set as an int (2).
     * It then verifies that {@code NumberOfOrders > 1} matches only the int-typed property.
     */
    @Test
    void stringPropertyIsNotUsedAsNumberInComparison(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final MessageProducer producer = session.createProducer(queue))
        {
            final Message stringTyped = session.createMessage();
            stringTyped.setIntProperty(ID, 1);
            stringTyped.setStringProperty(NUMBER_OF_ORDERS, "2");
            producer.send(stringTyped);

            final Message intTyped = session.createMessage();
            intTyped.setIntProperty(ID, 2);
            intTyped.setIntProperty(NUMBER_OF_ORDERS, 2);
            producer.send(intTyped);

            connection.start();

            try (final QueueBrowser selector = session.createBrowser(queue, "NumberOfOrders > 1"))
            {
                assertEquals(Set.of(2), browseIntProperty(selector, ID),
                        "Selector must evaluate comparisons using the property type used when setting it");
            }

            jms.queue().drain();
        }
    }

    /**
     * Verifies the numeric exception to the "like type" comparison rule.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html#message-selector-syntax">3.8.1.1</a>
     * states it is "valid to compare exact numeric values and approximate numeric values" and that the
     * conversion follows "Java numeric promotion".
     */
    @Test
    void exactAndApproxNumericComparisonUsesNumericPromotion(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final MessageProducer producer = session.createProducer(queue))
        {
            sendIntProperty(session, producer, 1, X, 5);
            sendIntProperty(session, producer, 2, X, 6);

            connection.start();

            try (final QueueBrowser selector = session.createBrowser(queue, "x = 5.0"))
            {
                // x is an exact numeric property (int) and the selector uses an approximate numeric literal (double).
                assertEquals(Set.of(1), browseIntProperty(selector, ID),
                        "Exact vs approximate numeric comparison must use numeric promotion and match correctly");
            }

            jms.queue().drain();
        }
    }

    private static void sendIntProperty(final Session session,
                                        final MessageProducer producer,
                                        final int id,
                                        final String propertyName,
                                        final int value) throws Exception
    {
        final Message message = session.createMessage();
        message.setIntProperty(ID, id);
        message.setIntProperty(propertyName, value);
        producer.send(message);
    }

    private static Set<Integer> browseIntProperty(final QueueBrowser browser, final String property) throws Exception
    {
        final Set<Integer> values = new HashSet<>();
        final Enumeration enumeration = browser.getEnumeration();
        while (enumeration.hasMoreElements())
        {
            final Message message = (Message) enumeration.nextElement();
            values.add(message.getIntProperty(property));
        }
        return values;
    }
}