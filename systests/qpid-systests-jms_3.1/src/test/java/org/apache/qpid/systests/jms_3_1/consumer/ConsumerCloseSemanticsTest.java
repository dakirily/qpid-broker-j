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

package org.apache.qpid.systests.jms_3_1.consumer;

import static org.apache.qpid.systests.JmsAwait.jms;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * System tests for {@link jakarta.jms.MessageConsumer#close()} semantics.
 * <br>
 * Jakarta Messaging defines that {@code close} is the only consumer method that may be invoked from a
 * thread other than the one currently controlling the session, and that it must coordinate correctly
 * with in-flight {@code receive} calls and message listener execution.
 * <br>
 * The tests in this class are aligned with:
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#closing-a-consumer">8.8 "Closing a consumer"</a>
 * - <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#threading-restrictions-on-a-session">6.2.5 "Threading restrictions on a session"</a>
 */
@JmsSystemTest
@Tag("queue")
class ConsumerCloseSemanticsTest
{
    /**
     * Verifies that if {@link MessageConsumer#close()} is called while another thread is blocked in
     * {@link MessageConsumer#receive()}, then:
     * - {@code close} blocks until {@code receive} completes; and
     * - the blocked {@code receive} call returns {@code null}.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#closing-a-consumer">8.8</a>
     * states: "If close is called in one thread whilst another thread is calling receive on the same
     * consumer then the call to close must block until the receive call has completed. A blocked receive
     * call returns null when the consumer is closed."
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Disabled("QPID JMS Client fails here sometimes")
    void closeBlocksUntilReceiveReturnsAndReceiveReturnsNull(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create())
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(queue);

            connection.start();

            final Duration shortWait = shortWait();

            final CountDownLatch receiveStarted = new CountDownLatch(1);
            final CountDownLatch receiveCallReturned = new CountDownLatch(1);
            final CountDownLatch receiveThreadFinished = new CountDownLatch(1);

            final AtomicReference<Message> received = new AtomicReference<>();
            final AtomicReference<Throwable> receiveFailure = new AtomicReference<>();

            final Thread receiveThread = new Thread(() ->
            {
                receiveStarted.countDown();
                try
                {
                    final Message m = consumer.receive();
                    receiveCallReturned.countDown(); // signal as close as possible to receive() returning
                    received.set(m);
                }
                catch (Throwable t)
                {
                    receiveCallReturned.countDown();
                    receiveFailure.set(t);
                }
                finally
                {
                    receiveThreadFinished.countDown();
                }
            }, "receive-thread-" + jms.fullTestName());

            receiveThread.start();

            assertTrue(receiveStarted.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "receive() did not start within timeout");

            // Tiny settle to reduce scheduling races (kept small to avoid timing flakiness).
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(
                    Math.min(50, Math.max(10, shortWait.toMillis() / 10))));

            // If receive() returns immediately, the core assumption of this test (blocked receive) is invalid.
            assertFalse(receiveCallReturned.await(shortWait.toMillis(), TimeUnit.MILLISECONDS),
                    "receive() returned unexpectedly early; test assumptions invalid");

            final ExecutorService executor = Executors.newSingleThreadExecutor();
            try
            {
                final CountDownLatch closeStarted = new CountDownLatch(1);
                final CountDownLatch allowClose = new CountDownLatch(1);

                final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
                final AtomicReference<Boolean> receiveInFlightAtCloseCall = new AtomicReference<>(false);
                final AtomicReference<Boolean> closeObservedReceiveReturned = new AtomicReference<>(false);

                final Future<?> closeFuture = executor.submit(() ->
                {
                    closeStarted.countDown();
                    try
                    {
                        // Ensure the test thread has verified that receive() is still in-flight
                        // before we invoke close().
                        final boolean released = allowClose.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS);
                        if (!released)
                        {
                            throw new AssertionError("Test did not release close thread in time");
                        }

                        // Validate (best possible) that receive() was still in-flight when we attempted close().
                        receiveInFlightAtCloseCall.set(receiveCallReturned.getCount() != 0);

                        consumer.close();

                        // Spec-critical order check:
                        // close() must not return until the in-flight receive() call has completed.
                        closeObservedReceiveReturned.set(receiveCallReturned.getCount() == 0);
                    }
                    catch (Throwable t)
                    {
                        closeFailure.set(t);
                    }
                });

                assertTrue(closeStarted.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                        "close task did not start within timeout");

                // Re-check right before releasing close thread: receive() must still be in-flight.
                assertTrue(receiveCallReturned.getCount() != 0,
                        "receive() returned before close() was invoked; test assumptions invalid");

                // Now invoke close() while receive() is still in-flight.
                allowClose.countDown();

                // close() must eventually complete within the configured timeout (and must not deadlock).
                closeFuture.get(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS);

                assertNull(closeFailure.get(), () -> "Close thread failed: " + closeFailure.get());

                assertTrue(receiveInFlightAtCloseCall.get(),
                        "close() was invoked after receive() had already returned; test did not exercise spec scenario");

                assertTrue(closeObservedReceiveReturned.get(),
                        "consumer.close() returned before receive() returned (violates spec 8.8)");

                // After close() returns, the blocked receive() must complete and return null.
                assertTrue(receiveThreadFinished.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                        "consumer.close() returned but the blocked receive() did not complete within timeout");

                receiveThread.join(Timeouts.receiveMillis());
                assertFalse(receiveThread.isAlive(), "Receive thread did not terminate within timeout");

                assertNull(receiveFailure.get(), () -> "Receive thread failed: " + receiveFailure.get());
                assertNull(received.get(), "Blocked receive() should return null when the consumer is closed");
            }
            finally
            {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                        "Failed to shutdown executor in time");
            }
        }
    }

    /**
     * Verifies that if {@link MessageConsumer#close()} is called while a {@link jakarta.jms.MessageListener}
     * is executing for that consumer on another thread, then {@code close} blocks until the listener
     * completes.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#closing-a-consumer">8.8</a>
     * states: "If close is called in one thread whilst a message listener for this consumer is in progress
     * in another thread then the call to close must block until the message listener has completed."
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeBlocksUntilListenerCompletes(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create())
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(queue);

            final CountDownLatch enteredListener = new CountDownLatch(1);
            final CountDownLatch allowListenerToReturn = new CountDownLatch(1);
            final CountDownLatch listenerReturned = new CountDownLatch(1);

            final CountDownLatch closeStarted = new CountDownLatch(1);
            final CountDownLatch closeReturned = new CountDownLatch(1);

            final AtomicReference<Throwable> listenerFailure = new AtomicReference<>();

            consumer.setMessageListener(message ->
            {
                enteredListener.countDown();
                try
                {
                    // Block until the test explicitly allows the listener to return.
                    final boolean released = allowListenerToReturn.await(Timeouts.receiveMillis() * 4, TimeUnit.MILLISECONDS);
                    if (!released)
                    {
                        listenerFailure.set(new AssertionError("Test did not release listener in time"));
                    }
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    listenerFailure.set(e);
                }
                finally
                {
                    listenerReturned.countDown();
                }
            });

            connection.start();
            jms.messages().send(connection, queue, 1);

            // Wait until onMessage is definitely in progress before attempting to close the consumer.
            assertTrue(enteredListener.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "Listener did not start within timeout");

            final Duration shortWait = shortWait();

            final ExecutorService executor = Executors.newSingleThreadExecutor();
            try
            {
                final Future<?> closeFuture = executor.submit(() ->
                {
                    closeStarted.countDown();
                    try
                    {
                        consumer.close();
                    }
                    catch (Exception e)
                    {
                        // Propagate back to the test thread via Future#get.
                        throw new RuntimeException(e);
                    }
                    finally
                    {
                        closeReturned.countDown();
                    }
                });

                assertTrue(closeStarted.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                        "close task did not start within timeout");

                // While the listener is blocked, close must not return.
                assertFalse(closeReturned.await(shortWait.toMillis(), TimeUnit.MILLISECONDS),
                        "consumer.close() returned while listener was still running");

                // Allow the listener to complete and then verify close returns.
                allowListenerToReturn.countDown();

                assertTrue(listenerReturned.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                        "Listener did not complete within timeout");
                assertTrue(closeReturned.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                        "consumer.close() did not return after listener completion within timeout");

                // Ensure any exception from the close thread is surfaced to the test.
                closeFuture.get(1, TimeUnit.SECONDS);

                // Ensure any issue detected in the listener thread is surfaced to the test.
                assertNull(listenerFailure.get(),
                        () -> "Listener failed: " + listenerFailure.get());
            }
            finally
            {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                        "Failed to shutdown executor in time");
            }
        }
    }

    /**
     * Verifies that a message listener is explicitly permitted to call {@link MessageConsumer#close()} on its own
     * consumer and that, once {@code close} returns, the {@code onMessage} method is allowed to complete normally.
     * <br>
     * Section <a href="https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#closing-a-consumer">8.8</a>
     * states: "If close is called from a message listener’s onMessage method on its own consumer then after this
     * method returns the onMessage method must be allowed to complete normally." This requirement exists to avoid
     * deadlock during orderly shutdown.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeCalledFromOnMessageOnOwnConsumerAllowsOnMessageToComplete(final JmsSupport jms) throws Exception
    {
        final Queue queue = jms.builder().queue().create();

        try (final Connection connection = jms.builder().connection().prefetch(0).create())
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = session.createConsumer(queue);

            final CountDownLatch enteredListener = new CountDownLatch(1);
            final CountDownLatch closeReturnedInListener = new CountDownLatch(1);
            final CountDownLatch allowListenerToReturn = new CountDownLatch(1);
            final CountDownLatch listenerReturned = new CountDownLatch(1);

            final AtomicReference<Throwable> listenerFailure = new AtomicReference<>();

            consumer.setMessageListener(message ->
            {
                enteredListener.countDown();
                try
                {
                    // The spec explicitly allows a listener to close its own consumer.
                    consumer.close();
                    closeReturnedInListener.countDown();

                    // Simulate application work after closing the consumer.
                    final boolean released = allowListenerToReturn.await(Timeouts.receiveMillis() * 4, TimeUnit.MILLISECONDS);
                    if (!released)
                    {
                        listenerFailure.set(new AssertionError("Test did not release listener in time"));
                    }
                }
                catch (Throwable t)
                {
                    // Ensure the test can proceed even if close misbehaves.
                    closeReturnedInListener.countDown();
                    listenerFailure.set(t);
                }
                finally
                {
                    listenerReturned.countDown();
                }
            });

            connection.start();
            jms.messages().send(connection, queue, 1);

            assertTrue(enteredListener.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "Listener did not start within timeout");

            final Duration shortWait = shortWait();

            // The critical assertion: close must return even though onMessage has not yet completed.
            assertTrue(closeReturnedInListener.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "consumer.close() did not return when called from onMessage on its own consumer");

            // While the listener is still blocked (simulating additional work), onMessage must still be in progress.
            assertFalse(listenerReturned.await(shortWait.toMillis(), TimeUnit.MILLISECONDS),
                    "Listener returned unexpectedly early; test assumptions invalid");

            // Allow onMessage to complete normally.
            allowListenerToReturn.countDown();
            assertTrue(listenerReturned.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "Listener did not complete within timeout");

            assertNull(listenerFailure.get(),
                    () -> "Listener failed: " + listenerFailure.get());

            // In AUTO_ACKNOWLEDGE mode, once onMessage has completed normally the message should have been
            // acknowledged and therefore must no longer be available for delivery.
            jms(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertEquals(0, jms.queue(queue.getQueueName()).depthMessages(),
                            "Queue should be empty after onMessage completed normally"));

            try (MessageConsumer verifier = session.createConsumer(queue))
            {
                final Message redelivered = verifier.receive(shortWait.toMillis());
                assertNull(redelivered, "Message should have been acknowledged and must not be redelivered");
            }
        }
    }

    private static Duration shortWait()
    {
        // Small, but not too aggressive for slow CI.
        final long derived = Math.max(250L, Timeouts.receiveMillis() / 10);
        return Duration.ofMillis(Math.min(1_000L, derived));
    }
}