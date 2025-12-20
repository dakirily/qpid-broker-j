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

package org.apache.qpid.systests.jms_3_1.messagelistener;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a {@link Session} serializes asynchronous message delivery, i.e. does not execute
 * client {@link MessageListener#onMessage(jakarta.jms.Message)} callbacks concurrently.
 *
 * <p>Specification:</p>
 * <ul>
 *   <li>
 *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#serial-execution-of-client-code">
 *       <strong>Serial execution of client code</strong>
 *     </a>
 *   </li>
 *   <li>
 *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#receiving-messages-asynchronously">
 *       <strong>Receiving messages asynchronously</strong>
 *     </a>
 *   </li>
 *   <li>
 *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#threading-restrictions-on-a-session">
 *       <strong>Threading restrictions on a session</strong>
 *     </a>
 *   </li>
 * </ul>
 */
@JmsSystemTest
@Tag("queue")
class SessionSerialDispatchTest
{
    /**
     * Verifies that a {@link Session} does not invoke {@link MessageListener} callbacks concurrently
     * when there are multiple consumers on the same session.
     *
     * <p>The test:</p>
     * <ul>
     *   <li>creates two {@link MessageConsumer} instances on the same session, each with its own listener;</li>
     *   <li>sends a message to each consumer's queue;</li>
     *   <li>blocks the first onMessage callback and asserts the second callback is not invoked until the first returns.</li>
     * </ul>
     *
     * <p>Specification:</p>
     * <ul>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#serial-execution-of-client-code">
     *       <strong>Serial execution of client code</strong>
     *     </a>
     *   </li>
     *   <li>
     *     <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#receiving-messages-asynchronously">
     *       <strong>Receiving messages asynchronously</strong>
     *     </a>
     *   </li>
     * </ul>
     */
    @Test
    void onMessageCallbacksAreSerializedForConsumersOnSameSession(final JmsSupport jms) throws Exception
    {
        final Queue queue1 = jms.builder().queue().nameSuffix("_queue1").create();
        final Queue queue2 = jms.builder().queue().nameSuffix("_queue2").create();

        final CountDownLatch firstListenerEntered = new CountDownLatch(1);
        final CountDownLatch secondListenerEntered = new CountDownLatch(1);
        final CountDownLatch releaseFirstListener = new CountDownLatch(1);
        final CountDownLatch bothListenersCompleted = new CountDownLatch(2);

        final AtomicBoolean firstInvocation = new AtomicBoolean(true);
        final AtomicInteger inOnMessage = new AtomicInteger(0);
        final AtomicBoolean concurrentExecutionDetected = new AtomicBoolean(false);
        final AtomicReference<Throwable> listenerThrowable = new AtomicReference<>();

        try (final var connection = jms.builder().connection().create())
        {
            final Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer1 = consumerSession.createConsumer(queue1);
            final MessageConsumer consumer2 = consumerSession.createConsumer(queue2);

            final Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer1 = producerSession.createProducer(queue1);
            final MessageProducer producer2 = producerSession.createProducer(queue2);

            final MessageListener listener = message ->
            {
                final int inFlight = inOnMessage.incrementAndGet();
                if (inFlight != 1)
                {
                    concurrentExecutionDetected.set(true);
                }

                try
                {
                    if (firstInvocation.compareAndSet(true, false))
                    {
                        firstListenerEntered.countDown();
                        // Block the first callback to give an opportunity for a non-compliant implementation
                        // to attempt concurrent delivery of the second message.
                        if (!releaseFirstListener.await(Timeouts.receiveMillis() * 5, TimeUnit.MILLISECONDS))
                        {
                            listenerThrowable.compareAndSet(null,
                                    new AssertionError("Timed out waiting for test to release first listener"));
                        }
                    }
                    else
                    {
                        secondListenerEntered.countDown();
                    }
                }
                catch (Throwable e)
                {
                    listenerThrowable.compareAndSet(null, e);
                }
                finally
                {
                    inOnMessage.decrementAndGet();
                    bothListenersCompleted.countDown();
                }
            };

            consumer1.setMessageListener(listener);
            consumer2.setMessageListener(listener);

            // Send messages before starting delivery to ensure both are immediately available for dispatch.
            producer1.send(producerSession.createTextMessage("queue1"));
            producer2.send(producerSession.createTextMessage("queue2"));

            connection.start();

            try
            {
                assertTrue(firstListenerEntered.await(Timeouts.receiveMillis() * 5, TimeUnit.MILLISECONDS),
                        "First onMessage callback did not start in timely manner");

                // While the first callback is blocked, no second callback should be invoked on the same session.
                assertFalse(secondListenerEntered.await(1000, TimeUnit.MILLISECONDS),
                        "Second onMessage callback should not be invoked concurrently within the same session");

                releaseFirstListener.countDown();

                assertTrue(secondListenerEntered.await(Timeouts.receiveMillis() * 5, TimeUnit.MILLISECONDS),
                        "Second onMessage callback did not start after first callback completed");

                assertTrue(bothListenersCompleted.await(Timeouts.receiveMillis() * 5, TimeUnit.MILLISECONDS),
                        "Not all onMessage callbacks completed in timely manner");
            }
            finally
            {
                // Ensure the listener is not left blocked if the test fails early.
                releaseFirstListener.countDown();
            }
        }

        assertNull(listenerThrowable.get(), "Unexpected throwable thrown from MessageListener");
        assertFalse(concurrentExecutionDetected.get(), "Detected concurrent onMessage execution within a session");
        assertEquals(0, inOnMessage.get(), "Listener in-flight counter did not return to zero");
    }
}
