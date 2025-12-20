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

package org.apache.qpid.systests.jms_3_1.producer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.CompletionListener;
import jakarta.jms.Message;
import jakarta.jms.Session;

import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("queue")
@Tag("message")
@Tag("async-send")
class MessageProducerCompletionListenerTest
{
    @Test
    void onCompletionCalledAndMessageDelivered(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(queue);
            final var consumer = session.createConsumer(queue);

            final CountDownLatch completed = new CountDownLatch(1);
            final AtomicReference<Exception> error = new AtomicReference<>();

            final Message msg = session.createMessage();

            producer.send(msg, new CompletionListener()
            {
                @Override
                public void onCompletion(final Message message)
                {
                    completed.countDown();
                }

                @Override
                public void onException(final Message message, final Exception exception)
                {
                    error.set(exception);
                    completed.countDown();
                }
            });

            assertTrue(completed.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS), "Callback not invoked");
            assertNull(error.get(), "Unexpected onException: " + error.get());

            // provider-set headers should be available after callback
            assertNotNull(msg.getJMSMessageID(), "JMSMessageID should be available after onCompletion");

            connection.start();
            assertNotNull(consumer.receive(Timeouts.receiveMillis()), "Message should be consumable");
        }
    }

    @Test
    void onExceptionCalledWhenSendFails(final JmsSupport jms) throws Exception
    {
        final var invalidQueue = jms.builder().queue()
                .maximumQueueDepthBytes(0)
                .maximumQueueDepthMessages(0)
                .overflowPolicy(OverflowPolicy.REJECT)
                .create();
        try (final var connection = jms.builder().connection().syncPublish(true).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var producer = session.createProducer(invalidQueue);

            final CountDownLatch completed = new CountDownLatch(1);
            final AtomicReference<Exception> error = new AtomicReference<>();

            producer.send(session.createMessage(), new CompletionListener()
            {
                @Override
                public void onCompletion(final Message message)
                {
                    completed.countDown(); // unexpected success
                }

                @Override
                public void onException(final Message message, final Exception exception)
                {
                    error.set(exception);
                    completed.countDown();
                }
            });

            assertTrue(completed.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS), "Callback not invoked");
            assertNotNull(error.get(), "Expected onException for invalid destination");
        }
    }
}
