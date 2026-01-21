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
package org.apache.qpid.server.store.rocksdb;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.rocksdb.RocksDBException;

import org.apache.qpid.server.store.StoreException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RocksDBCommitterTest
{
    @Test
    void commitAsyncCompletesWithoutFlushWhenDeferSyncFalse() throws Exception
    {
        AtomicBoolean flushCalled = new AtomicBoolean(false);
        RocksDBCommitter committer = new RocksDBCommitter(1,
                                                          50L,
                                                          () -> false,
                                                          () -> flushCalled.set(true),
                                                          error -> { });
        committer.start();
        try
        {
            CompletableFuture<Integer> future = committer.commitAsync(deferSync -> { }, 42);
            assertEquals(42, future.get(2, TimeUnit.SECONDS));
            assertFalse(flushCalled.get(), "flushWal should not be called when deferSync is false");
        }
        finally
        {
            committer.stop();
        }
    }

    @Test
    void commitAsyncFailsAndClosesOnFlushWalException() throws Exception
    {
        AtomicReference<RuntimeException> fatal = new AtomicReference<>();
        RocksDBCommitter committer = new RocksDBCommitter(1,
                                                          50L,
                                                          () -> true,
                                                          () -> { throw new RocksDBException("flush failed"); },
                                                          fatal::set);
        committer.start();
        try
        {
            CompletableFuture<String> future = committer.commitAsync(deferSync -> { }, "val");
            ExecutionException failure = assertThrows(ExecutionException.class,
                                                      () -> future.get(2, TimeUnit.SECONDS),
                                                      "Expected commit to fail on flushWal exception");
            assertTrue(failure.getCause() instanceof StoreException,
                       "Expected StoreException on flushWal failure");
            assertNotNull(fatal.get(), "Expected fatal handler to be invoked");

            assertThrows(StoreException.class,
                         () -> committer.commitAsync(deferSync -> { }, "afterFailure"),
                         "Expected committer to reject new jobs after fatal flush failure");
        }
        finally
        {
            committer.stop();
        }
    }

    @Test
    void commitActionFailureDoesNotPreventOtherJobs() throws Exception
    {
        RocksDBCommitter committer = new RocksDBCommitter(1,
                                                          50L,
                                                          () -> false,
                                                          () -> { },
                                                          error -> { });
        committer.start();
        try
        {
            CompletableFuture<Integer> failed = committer.commitAsync(deferSync ->
            {
                throw new RuntimeException("boom");
            }, 1);
            CompletableFuture<Integer> success = committer.commitAsync(deferSync -> { }, 2);

            assertThrows(ExecutionException.class, () -> failed.get(2, TimeUnit.SECONDS));
            assertEquals(2, success.get(2, TimeUnit.SECONDS));
        }
        finally
        {
            committer.stop();
        }
    }

    @Test
    void flushOccursWhenAnyJobSucceedsWithDeferSyncTrue() throws Exception
    {
        AtomicInteger flushCount = new AtomicInteger(0);
        RocksDBCommitter committer = new RocksDBCommitter(1,
                                                          50L,
                                                          () -> true,
                                                          flushCount::incrementAndGet,
                                                          error -> { });
        committer.start();
        try
        {
            CompletableFuture<Integer> failed = committer.commitAsync(deferSync ->
            {
                throw new RuntimeException("boom");
            }, 1);
            CompletableFuture<Integer> success = committer.commitAsync(deferSync -> { }, 2);

            assertThrows(ExecutionException.class, () -> failed.get(2, TimeUnit.SECONDS));
            assertEquals(2, success.get(2, TimeUnit.SECONDS));
            assertEquals(1, flushCount.get(), "Expected flushWal to be called once");
        }
        finally
        {
            committer.stop();
        }
    }

    @Test
    void commitAsyncRejectedAfterStop()
    {
        RocksDBCommitter committer = new RocksDBCommitter(1,
                                                          50L,
                                                          () -> false,
                                                          () -> { },
                                                          error -> { });
        committer.start();
        committer.stop();

        assertThrows(StoreException.class,
                     () -> committer.commitAsync(deferSync -> { }, 1),
                     "Expected commit to be rejected after stop");
    }
}
