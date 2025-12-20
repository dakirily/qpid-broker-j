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

package org.apache.qpid.systests.jms_3_1.selector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import jakarta.jms.Connection;
import jakarta.jms.Message;
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
 * System tests for message selector operators {@code LIKE}, {@code IN} and {@code BETWEEN}.
 * <br>
 * Jakarta Messaging defines these operators as part of the SQL92-based message selector syntax.
 * <br>
 * The tests in this class are aligned with:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">
 *   3.8.1.1 "Message selector syntax"</a>
 */
@JmsSystemTest
@Tag("filters")
@Tag("queue")
class SelectorLikeInBetweenTest
{
    private static final String AGE = "age";
    private static final String COUNTRY = "Country";
    private static final String PHONE = "phone";
    private static final String UNDERSCORED = "underscored";

    /**
     * Verifies {@code BETWEEN} / {@code NOT BETWEEN} operator semantics using the normative examples.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states: "\"age BETWEEN 15 AND 19\" is equivalent to \"age >= 15 AND age <= 19\"".
     * It also states: "\"age NOT BETWEEN 15 AND 19\" is equivalent to \"age < 15 OR age > 19\"".
     */
    @Test
    void betweenAndNotBetweenAreEquivalentToRangePredicates(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final MessageProducer producer = session.createProducer(queue);
            sendAge(session, producer, 14);
            sendAge(session, producer, 15);
            sendAge(session, producer, 19);
            sendAge(session, producer, 20);

            final QueueBrowser between = session.createBrowser(queue, "age BETWEEN 15 AND 19");
            final QueueBrowser range = session.createBrowser(queue, "age >= 15 AND age <= 19");

            final QueueBrowser notBetween = session.createBrowser(queue, "age NOT BETWEEN 15 AND 19");
            final QueueBrowser outside = session.createBrowser(queue, "age < 15 OR age > 19");

            connection.start();

            final Set<Integer> betweenAges = browseIntProperty(between, AGE);
            final Set<Integer> rangeAges = browseIntProperty(range, AGE);
            assertEquals(betweenAges, rangeAges, "BETWEEN must match the same set as the equivalent range predicate");
            assertEquals(Set.of(15, 19), betweenAges, "Unexpected BETWEEN matches");

            final Set<Integer> notBetweenAges = browseIntProperty(notBetween, AGE);
            final Set<Integer> outsideAges = browseIntProperty(outside, AGE);
            assertEquals(notBetweenAges, outsideAges, "NOT BETWEEN must match the same set as the equivalent outside-range predicate");
            assertEquals(Set.of(14, 20), notBetweenAges, "Unexpected NOT BETWEEN matches");

            between.close();
            range.close();
            notBetween.close();
            outside.close();

            drainQueue(session, queue);
        }
    }

    /**
     * Verifies {@code IN} / {@code NOT IN} operator semantics using the normative example.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states: "\"Country IN ('UK', 'US', 'France')\" is true for 'UK' and false for 'Peru'".
     */
    @Test
    void inAndNotInMatchExpectedCountries(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final MessageProducer producer = session.createProducer(queue);
            sendCountry(session, producer, "UK");
            sendCountry(session, producer, "Peru");

            final QueueBrowser in = session.createBrowser(queue, "Country IN ('UK', 'US', 'France')");
            final QueueBrowser inExpanded = session.createBrowser(queue,
                    "(Country = 'UK') OR (Country = 'US') OR (Country = 'France')");

            final QueueBrowser notIn = session.createBrowser(queue, "Country NOT IN ('UK', 'US', 'France')");
            final QueueBrowser notInExpanded = session.createBrowser(queue,
                    "(Country <> 'UK') AND (Country <> 'US') AND (Country <> 'France')");

            connection.start();

            final Set<String> inCountries = browseStringProperty(in, COUNTRY);
            final Set<String> inExpandedCountries = browseStringProperty(inExpanded, COUNTRY);
            assertEquals(inCountries, inExpandedCountries, "IN must match the same set as the equivalent disjunction");
            assertEquals(Set.of("UK"), inCountries, "Unexpected IN matches");

            final Set<String> notInCountries = browseStringProperty(notIn, COUNTRY);
            final Set<String> notInExpandedCountries = browseStringProperty(notInExpanded, COUNTRY);
            assertEquals(notInCountries, notInExpandedCountries, "NOT IN must match the same set as the equivalent conjunction");
            assertEquals(Set.of("Peru"), notInCountries, "Unexpected NOT IN matches");

            in.close();
            inExpanded.close();
            notIn.close();
            notInExpanded.close();

            drainQueue(session, queue);
        }
    }

    /**
     * Verifies {@code LIKE} / {@code NOT LIKE} wildcard semantics using the normative example.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states: "\"phone LIKE '12%3'\" is true for '123' or '12993' and false for '1234'".
     */
    @Test
    void likeAndNotLikeMatchExpectedPhoneNumbers(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final MessageProducer producer = session.createProducer(queue);
            sendPhone(session, producer, "123");
            sendPhone(session, producer, "12993");
            sendPhone(session, producer, "1234");

            final QueueBrowser like = session.createBrowser(queue, "phone LIKE '12%3'");
            final QueueBrowser notLike = session.createBrowser(queue, "phone NOT LIKE '12%3'");

            connection.start();

            final Set<String> likePhones = browseStringProperty(like, PHONE);
            assertEquals(Set.of("123", "12993"), likePhones, "Unexpected LIKE matches");

            final Set<String> notLikePhones = browseStringProperty(notLike, PHONE);
            assertEquals(Set.of("1234"), notLikePhones, "Unexpected NOT LIKE matches");

            like.close();
            notLike.close();

            drainQueue(session, queue);
        }
    }

    /**
     * Verifies {@code LIKE ... ESCAPE} semantics for treating wildcard characters as literals.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * defines that: "The optional escape-character ... is used to escape the special meaning of the '_' and '%'".
     */
    @Test
    void likeEscapeAllowsLiteralUnderscoreMatching(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final MessageProducer producer = session.createProducer(queue);
            sendUnderscored(session, producer, "_foo");
            sendUnderscored(session, producer, "bar");

            // Escape '_' so that the pattern matches strings starting with a literal underscore.
            // Selector string produced at runtime is: underscored LIKE '\_%' ESCAPE '\'
            final String selector = "underscored LIKE '\\_%' ESCAPE '\\'";
            final QueueBrowser escapedLike = session.createBrowser(queue, selector);

            connection.start();

            final Set<String> matches = browseStringProperty(escapedLike, UNDERSCORED);
            assertEquals(Set.of("_foo"), matches, "Unexpected LIKE ... ESCAPE matches");

            escapedLike.close();

            drainQueue(session, queue);
        }
    }

    private static void sendAge(final Session session, final MessageProducer producer, final int age) throws Exception
    {
        final Message message = session.createMessage();
        message.setIntProperty(AGE, age);
        producer.send(message);
    }

    private static void sendCountry(final Session session, final MessageProducer producer, final String country) throws Exception
    {
        final Message message = session.createMessage();
        message.setStringProperty(COUNTRY, country);
        producer.send(message);
    }

    private static void sendPhone(final Session session, final MessageProducer producer, final String phone) throws Exception
    {
        final Message message = session.createMessage();
        message.setStringProperty(PHONE, phone);
        producer.send(message);
    }

    private static void sendUnderscored(final Session session, final MessageProducer producer, final String value) throws Exception
    {
        final Message message = session.createMessage();
        message.setStringProperty(UNDERSCORED, value);
        producer.send(message);
    }

    private static Set<Integer> browseIntProperty(final QueueBrowser browser, final String property) throws Exception
    {
        final Set<Integer> values = new HashSet<>();
        final Enumeration<?> enumeration = browser.getEnumeration();
        while (enumeration.hasMoreElements())
        {
            final Message message = (Message) enumeration.nextElement();
            values.add(message.getIntProperty(property));
        }
        return values;
    }

    private static Set<String> browseStringProperty(final QueueBrowser browser, final String property) throws Exception
    {
        final Set<String> values = new HashSet<>();
        final Enumeration<?> enumeration = browser.getEnumeration();
        while (enumeration.hasMoreElements())
        {
            final Message message = (Message) enumeration.nextElement();
            values.add(message.getStringProperty(property));
        }
        return values;
    }

    private static void drainQueue(final Session session, final Queue queue) throws Exception
    {
        // Keep test queues clean to avoid accumulating unconsumed messages across the suite.
        try (final var consumer = session.createConsumer(queue))
        {
            Message m;
            while ((m = consumer.receive(Timeouts.noMessagesMillis())) != null)
            {
                // drain
            }
        }
    }
}
