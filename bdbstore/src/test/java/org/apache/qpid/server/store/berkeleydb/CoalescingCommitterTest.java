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

package org.apache.qpid.server.store.berkeleydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.qpid.test.utils.UnitTestBase;

public class CoalescingCommitterTest extends UnitTestBase
{
    private static final long DEFAULT_WAIT_TIMEOUT_MS = 500;
    private static final long LARGE_WAIT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long ASSERT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(2);

    private EnvironmentFacade _environmentFacade;
    private CoalescingCommiter _coalescingCommitter;

    @BeforeEach
    public void setUp() throws Exception
    {
        _environmentFacade = mock(EnvironmentFacade.class);
        _coalescingCommitter = new CoalescingCommiter("Test", 8, DEFAULT_WAIT_TIMEOUT_MS, _environmentFacade);
        _coalescingCommitter.start();
    }

    @AfterEach
    public void tearDown()
    {
        stopCommitter();
    }

    private void stopCommitter()
    {
        if (_coalescingCommitter != null)
        {
            _coalescingCommitter.stop();
            _coalescingCommitter = null;
        }
    }

    private void recreateCommitter(final int notifyThreshold,
                                   final long waitTimeoutMs,
                                   final EnvironmentFacade environmentFacade)
    {
        stopCommitter();
        _environmentFacade = environmentFacade;
        _coalescingCommitter = new CoalescingCommiter("Test", notifyThreshold, waitTimeoutMs, _environmentFacade);
        _coalescingCommitter.start();
    }

    @Test
    public void committerEnvironmentFacadeInteractionsOnSyncCommit()
    {
        RuntimeException testFailure = new RuntimeException("Test");
        doThrow(testFailure).when(_environmentFacade).flushLog();

        try
        {
            _coalescingCommitter.commit(null, true);
            fail("Commit should fail");
        }
        catch (RuntimeException e)
        {
            assertEquals(testFailure, e, "Unexpected failure");
        }

        verify(_environmentFacade, times(1)).flushLog();

        doNothing().when(_environmentFacade).flushLog();
        _coalescingCommitter.commit(null, true);

        verify(_environmentFacade, times(2)).flushLog();
        verify(_environmentFacade, times(1)).flushLogFailed(testFailure);
    }

    @Test
    public void committerEnvironmentFacadeInteractionsOnAsyncCommit() throws Exception
    {
        RuntimeException testFailure = new RuntimeException("Test");
        doThrow(testFailure).when(_environmentFacade).flushLog();

        try
        {
            CompletableFuture<?> future = _coalescingCommitter.commitAsync(null, null);
            future.get(1000, TimeUnit.MILLISECONDS);
            fail("Async commit should fail");
        }
        catch (ExecutionException e)
        {
            assertEquals(testFailure, e.getCause(), "Unexpected failure");
        }

        verify(_environmentFacade, times(1)).flushLog();

        doNothing().when(_environmentFacade).flushLog();
        final String expectedResult = "Test";
        CompletableFuture<?> future = _coalescingCommitter.commitAsync(null, expectedResult);
        Object result = future.get(1000, TimeUnit.MILLISECONDS);
        assertEquals(expectedResult, result, "Unexpected result");

        verify(_environmentFacade, times(2)).flushLog();
        verify(_environmentFacade, times(1)).flushLogFailed(testFailure);
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void commitAsyncReachesNotifyThresholdTriggersFlushWithoutWaitingForTimeout() throws Exception
    {
        final CountDownLatch flushLatch = new CountDownLatch(1);
        final EnvironmentFacade environmentFacade = mock(EnvironmentFacade.class);
        doAnswer(invocation ->
        {
            flushLatch.countDown();
            return null;
        }).when(environmentFacade).flushLog();

        // Large commit timeout ensures flush is triggered by the notify threshold rather than by the periodic wake-up.
        recreateCommitter(2, LARGE_WAIT_TIMEOUT_MS, environmentFacade);

        final CompletableFuture<?> future1 = _coalescingCommitter.commitAsync(null, "R1");
        final CompletableFuture<?> future2 = _coalescingCommitter.commitAsync(null, "R2");

        assertTrue(flushLatch.await(1, TimeUnit.SECONDS), "flushLog should occur promptly once threshold is reached");
        assertEquals("R1", future1.get(1, TimeUnit.SECONDS), "Unexpected result");
        assertEquals("R2", future2.get(1, TimeUnit.SECONDS), "Unexpected result");

        verify(environmentFacade, times(1)).flushLog();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void stopAbortsPendingAsyncCommitsAndDoesNotHang() throws Exception
    {
        // Force the close() path to abort pending commits.
        final EnvironmentFacade environmentFacade = mock(EnvironmentFacade.class);
        doThrow(new RuntimeException("flush failed")).when(environmentFacade).flushLog();

        // Large threshold and timeout ensure the commit thread does not process jobs before stop() is invoked.
        recreateCommitter(100, LARGE_WAIT_TIMEOUT_MS, environmentFacade);

        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            futures.add(_coalescingCommitter.commitAsync(null, "R" + i));
        }

        // Execute stop() on a separate thread to avoid a hard hang if join() does not return.
        // Use a daemon thread so the JVM can still exit if something goes wrong.
        final CoalescingCommiter committer = _coalescingCommitter;
        _coalescingCommitter = null;

        final Thread stopper = new Thread(committer::stop, "Stopper");
        stopper.setDaemon(true);
        stopper.start();
        stopper.join(2_000);
        if (stopper.isAlive())
        {
            stopper.interrupt();
            fail("stop() did not return promptly");
        }

        for (CompletableFuture<?> future : futures)
        {
            try
            {
                future.get(1, TimeUnit.SECONDS);
                fail("Expected pending async commit to be completed exceptionally");
            }
            catch (ExecutionException e)
            {
                assertInstanceOf(RuntimeException.class, e.getCause(), "Unexpected failure type");
            }
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void stopWhenIdleDoesFlushLog()
    {
        doNothing().when(_environmentFacade).flushLog();
        _coalescingCommitter.stop();
        _coalescingCommitter = null;

        verify(_environmentFacade, times(1)).flushLog();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void commitAsyncBelowThresholdCompletesAfterFutureGetExplicitNotify() throws Exception
    {
        final CountDownLatch flushLatch = new CountDownLatch(1);
        final EnvironmentFacade environmentFacade = mock(EnvironmentFacade.class);
        doAnswer(invocation ->
        {
            flushLatch.countDown();
            return null;
        }).when(environmentFacade).flushLog();

        recreateCommitter(100, LARGE_WAIT_TIMEOUT_MS, environmentFacade);

        final CompletableFuture<?> future = _coalescingCommitter.commitAsync(null, "R");
        assertEquals("R", future.get(1, TimeUnit.SECONDS), "Unexpected result");
        assertTrue(flushLatch.await(1, TimeUnit.SECONDS), "flushLog should occur promptly after explicit notify");
        verify(environmentFacade, times(1)).flushLog();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void commitAsyncBelowThresholdCompletesOnPeriodicTimeoutWakeup() throws Exception
    {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final CountDownLatch flushLatch = new CountDownLatch(1);

        final EnvironmentFacade environmentFacade = mock(EnvironmentFacade.class);
        doAnswer(invocation ->
        {
            flushLatch.countDown();
            return null;
        }).when(environmentFacade).flushLog();

        // Small wait timeout ensures the commit thread wakes up without an explicit notify.
        recreateCommitter(100, 25, environmentFacade);

        final CompletableFuture<?> future = _coalescingCommitter.commitAsync(null, "R");
        future.whenComplete((r, e) -> completionLatch.countDown());

        assertTrue(completionLatch.await(ASSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                "Commit should complete after periodic wake-up");
        assertTrue(flushLatch.await(ASSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS), "flushLog should occur during processing");
        assertEquals("R", future.get(1, TimeUnit.SECONDS), "Unexpected result");

        verify(environmentFacade, times(1)).flushLog();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void commitAsyncManyJobsCoalescedIntoSingleFlush() throws Exception
    {
        final CountDownLatch flushStarted = new CountDownLatch(1);
        final CountDownLatch allowFlushToReturn = new CountDownLatch(1);
        final AtomicInteger flushCount = new AtomicInteger();

        final EnvironmentFacade environmentFacade = mock(EnvironmentFacade.class);
        doAnswer(invocation ->
        {
            flushCount.incrementAndGet();
            flushStarted.countDown();
            assertTrue(allowFlushToReturn.await(ASSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS), "flush was not released");
            return null;
        }).when(environmentFacade).flushLog();

        recreateCommitter(1_000_000, LARGE_WAIT_TIMEOUT_MS, environmentFacade);

        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 20; i++)
        {
            futures.add(_coalescingCommitter.commitAsync(null, "R" + i));
        }

        // trigger processing explicitly after all jobs have been queued
        final Thread waiter = new Thread(() ->
        {
            try
            {
                futures.get(0).get(ASSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            catch (Exception e)
            {
                // failures are asserted below
            }
        }, "Waiter");
        waiter.setDaemon(true);
        waiter.start();

        assertTrue(flushStarted.await(ASSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS), "flushLog should have started");
        allowFlushToReturn.countDown();
        waiter.join(ASSERT_TIMEOUT_MS);
        assertFalse(waiter.isAlive(), "Waiter did not complete promptly");

        for (int i = 0; i < futures.size(); i++)
        {
            assertEquals("R" + i, futures.get(i).get(ASSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Unexpected result");
        }

        assertEquals(1, flushCount.get(), "Unexpected number of flush calls");
        verify(environmentFacade, times(1)).flushLog();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void flushFailureAbortsAllJobsInBatchAndFlushLogFailedCalledOnce() throws Exception
    {
        final RuntimeException testFailure = new RuntimeException("flush failed");
        final EnvironmentFacade environmentFacade = mock(EnvironmentFacade.class);
        doThrow(testFailure).when(environmentFacade).flushLog();

        recreateCommitter(1_000_000, LARGE_WAIT_TIMEOUT_MS, environmentFacade);

        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            futures.add(_coalescingCommitter.commitAsync(null, "R" + i));
        }

        // Trigger processing explicitly after all jobs have been queued.
        try
        {
            futures.get(0).get(ASSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            fail("Async commit should fail");
        }
        catch (ExecutionException e)
        {
            assertEquals(testFailure, e.getCause(), "Unexpected failure");
        }

        for (CompletableFuture<?> future : futures)
        {
            try
            {
                future.get(ASSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                fail("Expected async commit to be completed exceptionally");
            }
            catch (ExecutionException e)
            {
                assertEquals(testFailure, e.getCause(), "Unexpected failure");
            }
        }

        verify(environmentFacade, times(1)).flushLog();
        verify(environmentFacade, times(1)).flushLogFailed(testFailure);
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void commitAsyncAfterStopThrowsIllegalStateException()
    {
        try
        {
            final CoalescingCommiter committer = new CoalescingCommiter("Test", 1, DEFAULT_WAIT_TIMEOUT_MS, _environmentFacade);
            committer.start();
            committer.stop();
            committer.commitAsync(null, "R");
            fail("Commit should fail");
        }
        catch (IllegalStateException e)
        {
            // expected
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void stopCalledFromCommitThreadDoesNotDeadlockOnJoin() throws Exception
    {
        final CountDownLatch stopLatch = new CountDownLatch(1);
        doNothing().when(_environmentFacade).flushLog();

        final CompletableFuture<?> future = _coalescingCommitter.commitAsync(null, "R");
        future.thenRun(() ->
        {
            stopLatch.countDown();
            _coalescingCommitter.stop();
        });

        assertEquals("R", future.get(ASSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS), "Unexpected result");
        assertTrue(stopLatch.await(ASSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS), "stop() was not invoked from commit thread");
    }
}
