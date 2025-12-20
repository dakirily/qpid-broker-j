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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.CompletionListener;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on asynchronous send using JMSProducer.
 * <br>
 * The tests in this class are aligned with:
 * - 7.3.8 "Use of the CompletionListener by the Jakarta Messaging provider"
 * - 7.3   "Sending messages"
 */
@JmsSystemTest
@Tag("queue")
@Tag("message")
@Tag("async-send")
@Tag("simplified-api")
class JMSProducerAsyncSendTest
{
    /**
     * Verifies that:
     * - CompletionListener is invoked for an asynchronous send;
     * - the callback is not executed on the sending thread (7.3.8);
     * - provider-set headers are available after completion.
     */
    @Test
    void completionListenerIsInvokedAndHeadersAreAvailable(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final JMSContext context = jms.builder().connection().connectionFactory().createContext())
        {
            final JMSConsumer consumer = context.createConsumer(queue);
            final JMSProducer producer = context.createProducer();

            final CountDownLatch completed = new CountDownLatch(1);
            final AtomicReference<Exception> error = new AtomicReference<>();
            final AtomicReference<Thread> callbackThread = new AtomicReference<>();
            final Thread sendingThread = Thread.currentThread();

            producer.setAsync(new CompletionListener()
            {
                @Override
                public void onCompletion(final Message message)
                {
                    callbackThread.set(Thread.currentThread());
                    completed.countDown();
                }

                @Override
                public void onException(final Message message, final Exception exception)
                {
                    error.set(exception);
                    callbackThread.set(Thread.currentThread());
                    completed.countDown();
                }
            });

            final Message message = context.createMessage();
            producer.send(queue, message);

            assertTrue(completed.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "CompletionListener callback not invoked");
            assertNull(error.get(), "Unexpected onException: " + error.get());
            assertNotNull(message.getJMSMessageID(), "JMSMessageID should be set after completion");
            assertTrue(callbackThread.get() != null && callbackThread.get() != sendingThread,
                    "CompletionListener must not be invoked on the sending thread");

            final Message received = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(received, "Message should be consumable after completion");
        }
    }
}
