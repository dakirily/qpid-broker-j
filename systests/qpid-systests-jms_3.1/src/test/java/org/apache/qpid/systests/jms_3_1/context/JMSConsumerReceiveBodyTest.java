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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.JMSContext;
import jakarta.jms.MessageFormatRuntimeException;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("queue")
@Tag("message")
@Tag("simplified-api")
class JMSConsumerReceiveBodyTest
{
    @Test
    void receiveBodyStringFromTextMessage(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var ctx = jms.builder().connection().connectionFactory().createContext(JMSContext.AUTO_ACKNOWLEDGE))
        {
            ctx.createProducer().send(queue, "hello");

            final var consumer = ctx.createConsumer(queue);
            final String body = consumer.receiveBody(String.class, Timeouts.receiveMillis());
            assertEquals("hello", body);
        }
    }

    @Test
    void receiveBodyThrowsOnIncompatibleType(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var ctx = jms.builder().connection().connectionFactory().createContext())
        {
            ctx.createProducer().send(queue, "text");

            final var consumer = ctx.createConsumer(queue);
            assertThrows(MessageFormatRuntimeException.class,
                    () -> consumer.receiveBody(byte[].class, Timeouts.receiveMillis()));
        }
    }

    @Test
    void receiveBodyNoWaitReturnsNullWhenNoMessage(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var ctx = jms.builder().connection().connectionFactory().createContext();
             final var consumer = ctx.createConsumer(queue))
        {
            assertNull(consumer.receiveBodyNoWait(String.class));
            assertNull(consumer.receiveBody(String.class, Timeouts.noMessagesMillis()));
        }
    }
}

