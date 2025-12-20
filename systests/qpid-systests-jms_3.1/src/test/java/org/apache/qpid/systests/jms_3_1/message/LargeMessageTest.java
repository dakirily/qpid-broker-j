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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.stream.IntStream;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("message")
@Tag("queue")
class LargeMessageTest
{
    @ParameterizedTest
    @ValueSource(ints = { 256, 512, 1024 })
    void checkLargeMessage(final int messageSize, final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var messageText = buildLargeMessage(messageSize);
            final var producer = session.createProducer(queue);
            final var message = session.createTextMessage(messageText);
            producer.send(message);

            final var consumer = session.createConsumer(queue);
            connection.start();

            final var receivedMessage = consumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, receivedMessage, "TextMessage should be received");

            final var receivedMessageText = textMessage.getText();
            assertEquals(receivedMessageText, messageText,
                    "Unexpected large message content for size : %d".formatted(messageSize));
        }
    }

    private String buildLargeMessage(int size)
    {
        return IntStream.range(0, size)
                .map(operand -> (char)(operand % 26) + 'A')
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    }
}
