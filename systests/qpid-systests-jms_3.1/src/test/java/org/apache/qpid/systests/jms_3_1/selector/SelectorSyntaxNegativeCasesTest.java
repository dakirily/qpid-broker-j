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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;

import jakarta.jms.Connection;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * System tests for syntactically invalid message selectors.
 * <br>
 * Jakarta Messaging requires providers to validate selector syntax when it is presented and to reject
 * syntactically invalid selectors with {@link jakarta.jms.InvalidSelectorException}.
 * <br>
 * The tests in this class are aligned with:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">
 *   3.8.1.1 "Message selector syntax"</a>
 */
@JmsSystemTest
@Tag("filters")
@Tag("queue")
class SelectorSyntaxNegativeCasesTest
{
    /**
     * Verifies that the provider rejects syntactically malformed selectors when they are presented.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states: "providers are required to verify the syntactic correctness ... at the time it is presented"
     * and that a method given a syntactically incorrect selector "must result in ... InvalidSelectorException".
     */
    @ParameterizedTest
    @MethodSource("malformedSelectors")
    void malformedSelectorsAreRejectedAtPresentationTime(final String selector, final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create())
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertInvalidSelector(session, queue, selector);
        }
    }

    /**
     * Verifies that the optional escape-character for {@code LIKE ... ESCAPE} must be a single-character
     * string literal.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states: "The optional escape-character is a single-character string literal".
     */
    @ParameterizedTest
    @MethodSource("invalidLikeEscapeSelectors")
    void invalidLikeEscapeIsRejected(final String selector, final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create())
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertInvalidSelector(session, queue, selector);
        }
    }

    /**
     * Verifies that reserved words cannot be used as identifiers in selectors.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * states:
     * - "Identifiers cannot be the names NULL, TRUE, or FALSE."
     * - "Identifiers cannot be NOT, AND, OR, BETWEEN, LIKE, IN, IS, or ESCAPE."
     */
    @ParameterizedTest
    @MethodSource("reservedWordIdentifierSelectors")
    void reservedWordsCannotBeUsedAsIdentifiers(final String selector, final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create())
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertInvalidSelector(session, queue, selector);
        }
    }

    private static void assertInvalidSelector(final Session session, final Queue queue, final String selector)
    {
        // Both consumer and browser creation accept selectors and must reject syntactically invalid selectors.
        assertThrows(InvalidSelectorException.class, () -> session.createConsumer(queue, selector),
                "Expected InvalidSelectorException for selector: " + selector);
        assertThrows(InvalidSelectorException.class, () -> session.createBrowser(queue, selector),
                "Expected InvalidSelectorException for selector: " + selector);
    }

    private static Stream<String> malformedSelectors()
    {
        return Stream.of(
                // Unbalanced parentheses.
                "a = 1 OR (b = 1",
                "a = 1)",

                // Malformed IN (missing parentheses / list).
                "Country IN 'UK', 'US'",
                "Country IN ()",

                // Malformed BETWEEN (missing AND as well as third operand).
                "age BETWEEN 1 2",
                "age BETWEEN 1 AND",

                // Unterminated string literal.
                "color = 'red",

                // Identifier must begin with a Java identifier start character.
                "1abc = 1"
        );
    }

    private static Stream<String> invalidLikeEscapeSelectors()
    {
        return Stream.of(
                // ESCAPE must be a single-character string literal; here it is two characters.
                "underscored LIKE '\\\\_%' ESCAPE 'xx'",
                // ESCAPE present but empty string.
                "underscored LIKE '\\\\_%' ESCAPE ''"
        );
    }

    private static Stream<String> reservedWordIdentifierSelectors()
    {
        return Stream.of(
                // NULL/TRUE/FALSE cannot be identifiers.
                "NULL = 1",
                "TRUE BETWEEN 1 AND 2",
                "FALSE IN (0, 1)",

                // Reserved keywords cannot be identifiers.
                "NOT = 1",
                "AND = 1",
                "OR = 1",
                "BETWEEN = 1",
                "LIKE = 'x'",
                "IN = 'x'",
                "IS = 1",
                "ESCAPE = 'x'"
        );
    }
}