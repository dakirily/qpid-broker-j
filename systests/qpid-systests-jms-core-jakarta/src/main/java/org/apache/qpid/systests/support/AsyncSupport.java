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

package org.apache.qpid.systests.support;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncSupport implements AutoCloseable
{
    @FunctionalInterface
    public interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    @FunctionalInterface
    public interface ThrowingSupplier<T>
    {
        T get() throws Exception;
    }

    private final ExecutorService _executor;
    private final List<Future<?>> _futures = new CopyOnWriteArrayList<>();

    public AsyncSupport(final String threadNamePrefix, final int parallelism)
    {
        final AtomicInteger idx = new AtomicInteger(0);
        final ThreadFactory threadFactory = r ->
        {
            final Thread thread = new Thread(r);
            thread.setName(threadNamePrefix + "-" + idx.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this._executor = Executors.newFixedThreadPool(Math.max(1, parallelism), threadFactory);
    }

    public ExecutorService executor()
    {
        return _executor;
    }

    public <T> Task<T> start(final String what, final ThrowingSupplier<T> body)
    {
        final CountDownLatch started = new CountDownLatch(1);
        final Future<T> future = _executor.submit(() ->
        {
            started.countDown();
            return body.get();
        });
        _futures.add(future);
        return new Task<>(what, started, future);
    }

    public Task<Void> run(final String what, final ThrowingRunnable body)
    {
        return start(what, () ->
        {
            body.run();
            return null;
        });
    }

    public <T> T call(final String what, final Duration timeout, final ThrowingSupplier<T> body)
    {
        return start(what, body).await(timeout, what + " timed out");
    }

    public void call(final String what, final Duration timeout, final ThrowingRunnable body)
    {
        run(what, body).await(timeout, what + " timed out");
    }

    @Override
    public void close()
    {
        // Best-effort cleanup
        for (Future<?> future : _futures)
        {
            if (!future.isDone())
            {
                future.cancel(true);
            }
        }
        _futures.clear();
        _executor.shutdownNow();
        try
        {
            _executor.awaitTermination(2, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    public static final class Task<T>
    {
        private final String _what;
        private final CountDownLatch _started;
        private final Future<T> _future;

        private Task(final String what, final CountDownLatch started, final Future<T> future)
        {
            this._what = what;
            this._started = started;
            this._future = future;
        }

        public void awaitStarted(final Duration timeout, final String message)
        {
            try
            {
                if (!_started.await(timeout.toMillis(), TimeUnit.MILLISECONDS))
                {
                    throw new AssertionError(message + " (waited " + timeout + ")");
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new AssertionError(message + " (interrupted)", e);
            }
        }

        public T await(final Duration timeout, final String message)
        {
            try
            {
                return _future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException e)
            {
                _future.cancel(true);
                throw new AssertionError(message + " (waited " + timeout + " for: " + _what + ")", e);
            }
            catch (ExecutionException e)
            {
                final Throwable cause = e.getCause();
                if (cause instanceof Error err)
                {
                    throw err;
                }
                if (cause instanceof RuntimeException re)
                {
                    throw re;
                }
                throw new AssertionError("Failed: " + _what, cause);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for: " + _what, e);
            }
        }

        public boolean isDone()
        {
            return _future.isDone();
        }

        public void cancel()
        {
            _future.cancel(true);
        }
    }
}
