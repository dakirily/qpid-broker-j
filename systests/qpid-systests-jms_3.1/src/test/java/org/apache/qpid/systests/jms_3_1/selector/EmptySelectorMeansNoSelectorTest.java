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

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Enumeration;

import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * System tests verifying that an empty message selector string ({@code ""}) is treated as if no selector
 * was specified.
 * <br>
 * Jakarta Messaging defines that if the value of a message selector is an empty string, it is treated as
 * {@code null} and indicates that there is no message selector.
 * <br>
 * The tests in this class are aligned with:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selection">3.8 "Message selection"</a>
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1 "Message selector syntax"</a>
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#queuebrowser">4.1.6 "QueueBrowser"</a>
 */
@JmsSystemTest
@Tag("filters")
@Tag("queue")
class EmptySelectorMeansNoSelectorTest
{
    /**
     * Verifies that creating a {@link jakarta.jms.MessageConsumer} with an empty selector results in a consumer
     * that receives all messages.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1 "Message selector syntax"</a>
     * states that an empty selector string is treated as {@code null}, indicating that no selector is applied to
     * the message consumer.
     * <br>
     * This test:
     * - sends a sequence of messages to a queue;
     * - creates a consumer using {@link Session#createConsumer(jakarta.jms.Destination, String)} with an empty selector;
     * - verifies that all sent messages are delivered to the consumer.
     */
    @Test
    void emptySelectorOnConsumerIsTreatedAsNoSelector(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();
        final int messageCount = 5;

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            // Populate the queue with messages tagged with a sequential index property.
            jms.messages().send(session, queue, messageCount);

            try (final var consumer = session.createConsumer(queue, ""))
            {
                connection.start();

                Message lastMessage = null;
                for (int i = 0; i < messageCount; i++)
                {
                    lastMessage = consumer.receive(Timeouts.receiveMillis());
                    assertNotNull(lastMessage, "Message " + i + " should be received");
                }

                assertNotNull(lastMessage, "Last received message must not be null");
                assertEquals(messageCount - 1, lastMessage.getIntProperty(INDEX),
                        "Unexpected index on the last received message");

                assertNull(consumer.receive(Timeouts.noMessagesMillis()),
                        "No further messages should be delivered after receiving all sent messages");
            }
        }
    }

    /**
     * Verifies that creating a {@link QueueBrowser} with an empty selector results in browsing the entire
     * queue content.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#queuebrowser">4.1.6 "QueueBrowser"</a>
     * states that a {@link QueueBrowser} may contain only the messages matching a selector. Combined with the rule
     * in section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-selector-syntax">3.8.1.1</a>
     * that an empty selector string indicates no selector, passing {@code ""} should cause the browser to enumerate
     * all messages on the queue.
     * <br>
     * This test:
     * - sends a sequence of messages to a queue;
     * - creates a {@link QueueBrowser} with an empty selector;
     * - verifies that the enumeration contains all sent messages.
     */
    @Test
    void emptySelectorOnBrowserIsTreatedAsNoSelector(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();
        final int messageCount = 7;

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            jms.messages().send(session, queue, messageCount);

            connection.start();

            try (final QueueBrowser browser = session.createBrowser(queue, ""))
            {
                final Enumeration<?> enumeration = browser.getEnumeration();

                int browsed = 0;
                Message lastMessage = null;
                while (enumeration.hasMoreElements())
                {
                    browsed++;
                    lastMessage = (Message) enumeration.nextElement();
                }

                assertEquals(messageCount, browsed, "Unexpected number of messages returned by the browser");
                assertNotNull(lastMessage, "Last browsed message must not be null");
                assertEquals(messageCount - 1, lastMessage.getIntProperty(INDEX),
                        "Unexpected index on the last browsed message");
            }

            // Browsing must not remove messages. Verify that all messages are still consumable.
            try (final var consumer = session.createConsumer(queue))
            {
                for (int i = 0; i < messageCount; i++)
                {
                    final Message received = consumer.receive(Timeouts.receiveMillis());
                    assertNotNull(received, "Expected message " + i + " after browsing");
                }

                assertNull(consumer.receive(Timeouts.noMessagesMillis()),
                        "Browsing with an empty selector must not remove messages from the queue");
            }
        }
    }
}