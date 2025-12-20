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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * System tests for message selector operator precedence and case sensitivity.
 * <br>
 * Jakarta Messaging defines that:
 * - "Logical operators in precedence order: NOT, AND, OR"
 * - "Parentheses can be used to change this order."
 * - "operator names ... are case insensitive."
 * - "Identifiers are case sensitive."
 * <br>
 * The tests in this class are aligned with:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">
 *   3.8.1.1 "Message selector syntax"</a>
 */
@JmsSystemTest
@Tag("filters")
@Tag("queue")
class SelectorPrecedenceAndCaseSensitivityTest
{
    private static final String ID = "id";
    private static final String A = "a";
    private static final String B = "b";

    /**
     * Verifies logical operator precedence (NOT, AND, OR) using an expression where the result depends
     * on correct precedence handling.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states: "Logical operators in precedence order: NOT, AND, OR".
     * <br>
     * This test checks that:
     * - {@code a = 1 OR a = 2 AND b = 1} is interpreted as {@code a = 1 OR (a = 2 AND b = 1)}.
     */
    @Test
    void logicalOperatorPrecedenceIsNotAndThenAndThenOr(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final MessageProducer producer = session.createProducer(queue))
        {
            // id=1: a=1, b=0  -> must match (a=1)
            // id=2: a=1, b=1  -> must match (a=1)
            // id=3: a=2, b=1  -> must match (a=2 AND b=1)
            // id=4: a=2, b=0  -> must NOT match
            send(session, producer, 1, 1, 0);
            send(session, producer, 2, 1, 1);
            send(session, producer, 3, 2, 1);
            send(session, producer, 4, 2, 0);

            final String selector = "a = 1 OR a = 2 AND b = 1";
            final String equivalent = "a = 1 OR (a = 2 AND b = 1)";

            connection.start();

            try (final QueueBrowser browser = session.createBrowser(queue, selector);
                 final QueueBrowser browserEquivalent = session.createBrowser(queue, equivalent))
            {
                final Set<Integer> matched = browseIntProperty(browser, ID);
                final Set<Integer> matchedEquivalent = browseIntProperty(browserEquivalent, ID);

                assertEquals(matchedEquivalent, matched,
                        "Selector must respect NOT/AND/OR precedence (AND binds tighter than OR)");
                assertEquals(Set.of(1, 2, 3), matched, "Unexpected selector matches");
            }

            drainQueue(session, queue);
        }
    }

    /**
     * Verifies that parentheses override precedence.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states: "Parentheses can be used to change this order."
     * <br>
     * This test checks that:
     * - {@code (a = 1 OR a = 2) AND b = 1} produces a different result than {@code a = 1 OR a = 2 AND b = 1}.
     */
    @Test
    void parenthesesOverridePrecedence(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final MessageProducer producer = session.createProducer(queue))
        {
            send(session, producer, 1, 1, 0);
            send(session, producer, 2, 1, 1);
            send(session, producer, 3, 2, 1);
            send(session, producer, 4, 2, 0);

            final String withoutParentheses = "a = 1 OR a = 2 AND b = 1";
            final String withParentheses = "(a = 1 OR a = 2) AND b = 1";

            connection.start();

            try (final QueueBrowser browserWithout = session.createBrowser(queue, withoutParentheses);
                 final QueueBrowser browserWith = session.createBrowser(queue, withParentheses))
            {
                final Set<Integer> without = browseIntProperty(browserWithout, ID);
                final Set<Integer> with = browseIntProperty(browserWith, ID);

                assertNotEquals(without, with, "Parentheses must change evaluation order when they change grouping");
                assertEquals(Set.of(1, 2, 3), without, "Unexpected matches without parentheses");
                assertEquals(Set.of(2, 3), with, "Unexpected matches with parentheses");
            }

            drainQueue(session, queue);
        }
    }

    /**
     * Verifies that selector keywords/operators are case-insensitive, while identifiers are case-sensitive.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states:
     * - "operator names ... are case insensitive."
     * - "Identifiers are case sensitive."
     * <br>
     * This test sends two messages which differ only by the case of the property name ({@code Color} vs {@code color})
     * and checks that selectors distinguish between them.
     */
    @Test
    void keywordsAreCaseInsensitiveButIdentifiersAreCaseSensitive(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final MessageProducer producer = session.createProducer(queue))
        {
            final Message m1 = session.createMessage();
            m1.setIntProperty(ID, 1);
            m1.setStringProperty("Color", "red");
            producer.send(m1);

            final Message m2 = session.createMessage();
            m2.setIntProperty(ID, 2);
            m2.setStringProperty("color", "red");
            producer.send(m2);

            connection.start();

            try (final QueueBrowser colorUpper = session.createBrowser(queue, "Color = 'red'");
                 final QueueBrowser colorLower = session.createBrowser(queue, "color = 'red'");
                 final QueueBrowser mixedCaseIdentifier = session.createBrowser(queue, "CoLoR = 'red'");
                 final QueueBrowser keywordCaseInsensitive = session.createBrowser(queue, "color iS nOt nUlL"))
            {
                assertEquals(Set.of(1), browseIntProperty(colorUpper, ID),
                        "Identifier 'Color' must match only property named exactly 'Color'");
                assertEquals(Set.of(2), browseIntProperty(colorLower, ID),
                        "Identifier 'color' must match only property named exactly 'color'");

                assertEquals(Set.of(), browseIntProperty(mixedCaseIdentifier, ID),
                        "Identifiers are case sensitive: 'CoLoR' must not match 'Color' nor 'color'");

                assertEquals(Set.of(2), browseIntProperty(keywordCaseInsensitive, ID),
                        "Keywords/operators are case insensitive: IS/NOT/NULL casing must not affect result");
            }

            drainQueue(session, queue);
        }
    }

    private static void send(final Session session,
                             final MessageProducer producer,
                             final int id,
                             final int a,
                             final int b) throws Exception
    {
        final Message message = session.createMessage();
        message.setIntProperty(ID, id);
        message.setIntProperty(A, a);
        message.setIntProperty(B, b);
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

    private static void drainQueue(final Session session, final Queue queue) throws Exception
    {
        // Keep test queues clean to avoid accumulating unconsumed messages across the suite.
        try (final MessageConsumer consumer = session.createConsumer(queue))
        {
            Message m;
            while ((m = consumer.receive(Timeouts.noMessagesMillis())) != null)
            {
                // drain
            }
        }
    }
}