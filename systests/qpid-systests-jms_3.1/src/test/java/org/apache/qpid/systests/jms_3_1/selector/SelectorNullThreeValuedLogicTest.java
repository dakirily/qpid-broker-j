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
 * System tests for SQL92 NULL semantics ("three-valued logic") in message selectors.
 * <br>
 * Jakarta Messaging specifies:
 * - "SQL treats a NULL value as unknown. Comparison or arithmetic with an unknown value always yields an unknown value."
 * - "The boolean operators use three-valued logic..."
 * - "A selector ... that evaluates to false or unknown does not match."
 * <br>
 * See:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html#message-selector-syntax">3.8.1.1</a>
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html#null-values">Section 3.8.1.2 "Null values"</a>
 */
@JmsSystemTest
@Tag("filters")
@Tag("queue")
class SelectorNullThreeValuedLogicTest
{
    private static final String ID = "id";
    private static final String X = "x";
    private static final String COLOR = "color";

    /**
     * Verifies that referencing a missing property yields NULL and therefore makes a comparison "unknown",
     * which does not match.
     * <br>
     * Section 3.8.1.1 states: "If a property that does not exist ... is referenced, its value is NULL".
     * Section 3.8.1.2 states: "SQL treats a NULL value as unknown."
     * Section 3.8.1.1 also states: "a selector that evaluates to false or unknown does not match."
     * <br>
     * See:
     * - https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html#message-selector-syntax
     * - https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html (Section 3.8.1.2)
     */
    @Test
    void comparisonWithMissingPropertyIsUnknownAndDoesNotMatch(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final MessageProducer producer = session.createProducer(queue))
        {
            // id=1: no property "x" => x is NULL in selector evaluation.
            final Message missing = session.createMessage();
            missing.setIntProperty(ID, 1);
            producer.send(missing);

            // id=2: x=1 => comparison should evaluate to TRUE.
            final Message present = session.createMessage();
            present.setIntProperty(ID, 2);
            present.setIntProperty(X, 1);
            producer.send(present);

            connection.start();

            try (final QueueBrowser browser = session.createBrowser(queue, "x = 1"))
            {
                assertEquals(Set.of(2), browseIntProperty(browser, ID),
                        "Comparison with missing property must be unknown and therefore not match");
            }

            drainQueue(session, queue);
        }
    }

    /**
     * Verifies OR truth table behaviour when one operand is "unknown".
     * <br>
     * Section 3.8.1.2 defines three-valued logic for boolean operators (including OR) and treats NULL as unknown.
     * This implies: unknown OR true == true.
     * <br>
     * See:
     * - https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html (Section 3.8.1.2)
     */
    @Test
    void unknownOrTrueEvaluatesToTrue(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final MessageProducer producer = session.createProducer(queue))
        {
            // id=1: x is missing => (x = 1) is unknown; color='red' => TRUE => unknown OR true => TRUE
            final Message unknownOrTrue = session.createMessage();
            unknownOrTrue.setIntProperty(ID, 1);
            unknownOrTrue.setStringProperty(COLOR, "red");
            producer.send(unknownOrTrue);

            // id=2: x=1 => TRUE regardless of color => TRUE OR ... => TRUE
            final Message trueOrAnything = session.createMessage();
            trueOrAnything.setIntProperty(ID, 2);
            trueOrAnything.setIntProperty(X, 1);
            trueOrAnything.setStringProperty(COLOR, "blue");
            producer.send(trueOrAnything);

            // id=3: x missing => unknown; color='blue' => FALSE => unknown OR false => unknown (does not match)
            final Message unknownOrFalse = session.createMessage();
            unknownOrFalse.setIntProperty(ID, 3);
            unknownOrFalse.setStringProperty(COLOR, "blue");
            producer.send(unknownOrFalse);

            connection.start();

            try (final QueueBrowser browser = session.createBrowser(queue, "x = 1 OR color = 'red'"))
            {
                assertEquals(Set.of(1, 2), browseIntProperty(browser, ID),
                        "unknown OR true must evaluate to true; unknown OR false must not match");
            }

            drainQueue(session, queue);
        }
    }

    /**
     * Verifies AND truth table behaviour when one operand is "unknown".
     * <br>
     * Section 3.8.1.2 defines three-valued logic for boolean operators (including AND) and treats NULL as unknown.
     * This implies: unknown AND true == unknown (and therefore does not match).
     * <br>
     * Also note Section 3.8.1.1: "a selector that evaluates to false or unknown does not match."
     * <br>
     * See:
     * - https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html (Section 3.8.1.2)
     * - https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html#message-selector-syntax
     */
    @Test
    void unknownAndTrueEvaluatesToUnknown(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create();
             final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final MessageProducer producer = session.createProducer(queue))
        {
            // id=1: x missing => unknown; color='red' => TRUE => unknown AND true => unknown => must not match
            final Message unknownAndTrue = session.createMessage();
            unknownAndTrue.setIntProperty(ID, 1);
            unknownAndTrue.setStringProperty(COLOR, "red");
            producer.send(unknownAndTrue);

            // id=2: x=1 => TRUE; color='red' => TRUE => TRUE AND TRUE => TRUE => must match
            final Message trueAndTrue = session.createMessage();
            trueAndTrue.setIntProperty(ID, 2);
            trueAndTrue.setIntProperty(X, 1);
            trueAndTrue.setStringProperty(COLOR, "red");
            producer.send(trueAndTrue);

            // id=3: x missing => unknown; color='blue' => FALSE => unknown AND false => FALSE => must not match
            final Message unknownAndFalse = session.createMessage();
            unknownAndFalse.setIntProperty(ID, 3);
            unknownAndFalse.setStringProperty(COLOR, "blue");
            producer.send(unknownAndFalse);

            connection.start();

            try (final QueueBrowser browser = session.createBrowser(queue, "x = 1 AND color = 'red'"))
            {
                assertEquals(Set.of(2), browseIntProperty(browser, ID),
                        "unknown AND true must not match; true AND true must match");
            }

            drainQueue(session, queue);
        }
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