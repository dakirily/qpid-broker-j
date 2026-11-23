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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.qpid.test.utils.UnitTestBase;

@Timeout(5)
public class CoalescingCommitterTest extends UnitTestBase
{
    private static final int NOTIFY_THRESHOLD = 2;
    private static final long COMMIT_WAIT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long ASSERT_TIMEOUT_SECONDS = 2L;

    private EnvironmentFacade _environmentFacade;
    private CoalescingCommiter _coalescingCommitter;

    @BeforeEach
    public void setUp()
    {
        _environmentFacade = mock(EnvironmentFacade.class);
        _coalescingCommitter = new CoalescingCommiter("Test", NOTIFY_THRESHOLD, COMMIT_WAIT_TIMEOUT_MS,
                                                   _environmentFacade);
    }

    @AfterEach
    public void stopCommitter()
    {
        if (_coalescingCommitter != null)
        {
            _coalescingCommitter.stop();
            _coalescingCommitter = null;
        }
    }

    @Test
    public void testCommitterEnvironmentFacadeInteractionsOnSyncCommit()
    {
        final RuntimeException testFailure = new RuntimeException("Test");
        doThrow(testFailure).when(_environmentFacade).flushLog();
        _coalescingCommitter.start();

        assertSame(testFailure, assertThrows(RuntimeException.class, () -> _coalescingCommitter.commit(null, true)));

        verify(_environmentFacade, times(1)).flushLog();

        doNothing().when(_environmentFacade).flushLog();
        _coalescingCommitter.commit(null, true);

        verify(_environmentFacade, times(2)).flushLog();
        verify(_environmentFacade, times(1)).flushLogFailed(testFailure);
    }

    @Test
    public void testCommitterEnvironmentFacadeInteractionsOnAsyncCommit() throws Exception
    {
        final RuntimeException testFailure = new RuntimeException("Test");
        doThrow(testFailure).when(_environmentFacade).flushLog();
        _coalescingCommitter.start();
        awaitIdleWorker();

        final CompletableFuture<?> failedCommit = _coalescingCommitter.commitAsync(null, null);
        assertSame(testFailure, assertThrows(ExecutionException.class,
                () -> failedCommit.get(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS)).getCause());

        verify(_environmentFacade, times(1)).flushLog();

        doNothing().when(_environmentFacade).flushLog();
        final String expectedResult = "Test";
        final CompletableFuture<String> future = _coalescingCommitter.commitAsync(null, expectedResult);
        final String result = future.get(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(expectedResult, result, "Unexpected result");

        verify(_environmentFacade, times(2)).flushLog();
        verify(_environmentFacade, times(1)).flushLogFailed(testFailure);
    }

    @Test
    public void testCommitAsyncReachesNotifyThreshold() throws Exception
    {
        _coalescingCommitter.start();
        final Object monitor = awaitIdleWorker();
        final CompletableFuture<String> first;
        final CompletableFuture<String> second;
        synchronized (monitor)
        {
            // Keep both commits queued until the notification for reaching the threshold.
            first = _coalescingCommitter.commitAsync(null, "R1");
            second = _coalescingCommitter.commitAsync(null, "R2");
        }

        // Getting an individual commit future would also wake the worker.
        CompletableFuture.allOf(first, second).get(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("R1", first.join());
        assertEquals("R2", second.join());
        verify(_environmentFacade).flushLog();
    }

    @Test
    public void testStopWhenIdleFlushesOnce() throws Exception
    {
        _coalescingCommitter.start();
        awaitIdleWorker();

        stopCommitter();

        verify(_environmentFacade).flushLog();
    }

    @Test
    public void testStopCompletesPendingAsyncCommit() throws Exception
    {
        _coalescingCommitter.start();
        awaitIdleWorker();
        final CompletableFuture<String> pendingCommit = _coalescingCommitter.commitAsync(null, "R");

        stopCommitter();

        assertTrue(pendingCommit.isDone(), "Stop should complete the pending commit");
        assertEquals("R", pendingCommit.join());
        verify(_environmentFacade).flushLog();
    }

    @Test
    public void testStopAbortsPendingAsyncCommitWhenFlushFails() throws Exception
    {
        doThrow(new RuntimeException("flush failed")).when(_environmentFacade).flushLog();
        _coalescingCommitter.start();
        awaitIdleWorker();
        final CompletableFuture<String> pendingCommit = _coalescingCommitter.commitAsync(null, "R");

        stopCommitter();

        assertTrue(pendingCommit.isCompletedExceptionally(), "Stop should abort the pending commit");
        final CompletionException failure = assertThrows(CompletionException.class, pendingCommit::join);
        assertInstanceOf(RuntimeException.class, failure.getCause());
        verify(_environmentFacade).flushLog();
    }

    @Test
    public void testCommitAsyncBatchCompletesAfterSingleFlush() throws Exception
    {
        final CountDownLatch flushStarted = new CountDownLatch(1);
        final CountDownLatch allowFlushToReturn = new CountDownLatch(1);
        doAnswer(invocation ->
        {
            flushStarted.countDown();
            assertTrue(allowFlushToReturn.await(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS), "Flush was not released");
            return null;
        }).when(_environmentFacade).flushLog();

        final CompletableFuture<String> first = _coalescingCommitter.commitAsync(null, "R1");
        final CompletableFuture<String> second = _coalescingCommitter.commitAsync(null, "R2");
        _coalescingCommitter.start();

        try
        {
            assertTrue(flushStarted.await(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS), "Flush did not start");
            assertFalse(first.isDone(), "First commit should wait for the flush");
            assertFalse(second.isDone(), "Second commit should wait for the flush");
        }
        finally
        {
            allowFlushToReturn.countDown();
        }

        CompletableFuture.allOf(first, second).get(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("R1", first.join());
        assertEquals("R2", second.join());
        verify(_environmentFacade).flushLog();
    }

    @Test
    public void testFlushFailureAbortsBatchAndNotifiesFacadeOnce() throws Exception
    {
        final RuntimeException testFailure = new RuntimeException("flush failed");
        final CountDownLatch flushFailureHandled = new CountDownLatch(1);
        doThrow(testFailure).when(_environmentFacade).flushLog();
        doAnswer(invocation ->
        {
            flushFailureHandled.countDown();
            return null;
        }).when(_environmentFacade).flushLogFailed(testFailure);

        final CompletableFuture<String> first = _coalescingCommitter.commitAsync(null, "R1");
        final CompletableFuture<String> second = _coalescingCommitter.commitAsync(null, "R2");
        _coalescingCommitter.start();

        assertSame(testFailure, assertThrows(ExecutionException.class,
                () -> first.get(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS)).getCause());
        assertSame(testFailure, assertThrows(ExecutionException.class,
                () -> second.get(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS)).getCause());
        assertTrue(flushFailureHandled.await(ASSERT_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                   "The facade should be notified after the batch is aborted");
        verify(_environmentFacade).flushLog();
        verify(_environmentFacade).flushLogFailed(testFailure);
    }

    private Object awaitIdleWorker() throws ReflectiveOperationException, InterruptedException
    {
        final Thread worker = (Thread) getField(_coalescingCommitter, "_commitThread");
        final Object monitor = getField(worker, "_lock");
        final long start = System.nanoTime();
        synchronized (monitor)
        {
            while (worker.getState() != Thread.State.TIMED_WAITING)
            {
                assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(ASSERT_TIMEOUT_SECONDS),
                           "Commit worker did not enter its empty-queue wait");
                monitor.wait(1L);
            }
        }
        return monitor;
    }

    private static Object getField(final Object target, final String name) throws ReflectiveOperationException
    {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
