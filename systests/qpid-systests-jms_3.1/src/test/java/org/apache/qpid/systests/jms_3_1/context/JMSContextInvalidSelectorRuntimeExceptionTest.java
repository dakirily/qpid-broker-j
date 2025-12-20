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

package org.apache.qpid.systests.jms_3_1.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Enumeration;

import jakarta.jms.InvalidSelectorRuntimeException;
import jakarta.jms.JMSContext;
import jakarta.jms.Message;
import jakarta.jms.QueueBrowser;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * System tests focusing on how the simplified API reports invalid message selector expressions
 * and how it treats an empty selector string.
 *
 * The tests in this class are aligned with:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selection">Message selection</a>
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">Message selector syntax</a>
 *   (including the rule that an empty selector is treated as {@code null})
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jmsexception-and-jmsruntimeexception">JMSException and JMSRuntimeException</a>
 *   (simplified API methods throw runtime exceptions)
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">Standard exceptions</a>
 *   ({@link InvalidSelectorRuntimeException})
 */
@JmsSystemTest
@Tag("filters")
@Tag("queue")
@Tag("simplified-api")
class JMSContextInvalidSelectorRuntimeExceptionTest
{
    private static final String INVALID_SELECTOR = "Cost LIKE 5";

    /**
     * Verifies that a syntactically incorrect selector passed to
     * {@link JMSContext#createConsumer(jakarta.jms.Destination, String)} causes an
     * {@link InvalidSelectorRuntimeException} to be thrown.
     *
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">Message selector syntax</a>
     * requires providers to verify syntactic correctness of a selector when it is presented.
     * A syntactically incorrect selector must result in an InvalidSelectorException.
     * For simplified API methods which cannot throw the checked exception, the corresponding
     * runtime exception is mandated by
     * <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">Standard exceptions</a>.
     */
    @Test
    void invalidSelectorOnCreateConsumerThrowsInvalidSelectorRuntimeException(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var ctx = jms.builder().connection().connectionFactory().createContext())
        {
            assertThrows(InvalidSelectorRuntimeException.class,
                    () -> ctx.createConsumer(queue, INVALID_SELECTOR),
                    "Invalid selector should cause InvalidSelectorRuntimeException");
        }
    }

    /**
     * Verifies that a syntactically incorrect selector passed to
     * {@link JMSContext#createBrowser(jakarta.jms.Queue, String)} causes an
     * {@link InvalidSelectorRuntimeException} to be thrown.
     *
     * Selectors apply to queue browsers in the same way as to consumers
     * (see <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selection">Message selection</a>).
     * The simplified API reports invalid syntax using runtime exceptions
     * as described in
     * <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#jmsexception-and-jmsruntimeexception">JMSException and JMSRuntimeException</a>
     * and the mandatory mapping in
     * <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#standard-exceptions">Standard exceptions</a>.
     */
    @Test
    void invalidSelectorOnCreateBrowserThrowsInvalidSelectorRuntimeException(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var ctx = jms.builder().connection().connectionFactory().createContext())
        {
            assertThrows(InvalidSelectorRuntimeException.class,
                    () -> ctx.createBrowser(queue, INVALID_SELECTOR),
                    "Invalid selector should cause InvalidSelectorRuntimeException");
        }
    }

    /**
     * Verifies that an empty selector string passed to
     * {@link JMSContext#createConsumer(jakarta.jms.Destination, String)} is treated as
     * {@code null} (that is, the consumer has no selector).
     *
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">Message selector syntax</a>
     * states that if the value of a message selector is an empty string, it is treated as
     * {@code null} and indicates that there is no message selector for the consumer.
     */
    @Test
    void emptySelectorOnCreateConsumerIsTreatedAsNull(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var ctx = jms.builder().connection().connectionFactory().createContext())
        {
            ctx.createProducer().setProperty("color", "blue").send(queue, "blue");
            ctx.createProducer().setProperty("color", "red").send(queue, "red");

            final var consumer = ctx.createConsumer(queue, "");
            ctx.start();

            final Message m1 = consumer.receive(Timeouts.receiveMillis());
            final Message m2 = consumer.receive(Timeouts.receiveMillis());

            assertNotNull(m1, "First message should be received");
            assertNotNull(m2, "Second message should be received");

            final var colors = java.util.Set.of(m1.getStringProperty("color"), m2.getStringProperty("color"));
            assertEquals(java.util.Set.of("blue", "red"), colors,
                    "Empty selector must behave like no selector and deliver all messages");

            assertNull(consumer.receive(Timeouts.noMessagesMillis()), "No further messages expected");
        }
    }

    /**
     * Verifies that an empty selector string passed to
     * {@link JMSContext#createBrowser(jakarta.jms.Queue, String)} is treated as {@code null}
     * and results in a browser which enumerates all messages on the queue.
     *
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">Message selector syntax</a>
     * defines the empty-string selector rule, and
     * <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#queuebrowser">Queue browsing</a>
     * describes a QueueBrowser as a non-destructive way to view messages on a queue.
     */
    @Test
    void emptySelectorOnCreateBrowserIsTreatedAsNull(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var ctx = jms.builder().connection().connectionFactory().createContext())
        {
            final int count = 3;
            for (int i = 0; i < count; i++)
            {
                ctx.createProducer().setProperty("index", i).send(queue, "m" + i);
            }

            final QueueBrowser browser = ctx.createBrowser(queue, "");
            ctx.start();

            final Enumeration<?> enumeration = browser.getEnumeration();
            int browsed = 0;
            while (enumeration.hasMoreElements())
            {
                enumeration.nextElement();
                browsed++;
            }

            assertEquals(count, browsed, "Empty selector must browse all messages on the queue");

            // Drain the queue to keep the test isolated even if the broker retains resources.
            final var consumer = ctx.createConsumer(queue);
            for (int i = 0; i < count; i++)
            {
                assertNotNull(consumer.receive(Timeouts.receiveMillis()), "Expected message " + i + " to be receivable");
            }
            assertNull(consumer.receive(Timeouts.noMessagesMillis()), "Queue should be empty after draining");
        }
    }
}
