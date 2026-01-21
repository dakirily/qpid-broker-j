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

package org.apache.qpid.server.store.rocksdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.rocksdb.RocksDBException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.store.StoreException;

final class RocksDBCommitter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBCommitter.class);

    @FunctionalInterface
    interface CommitAction
    {
        void commit(boolean deferSync);
    }

    @FunctionalInterface
    interface DeferSyncPolicy
    {
        boolean shouldDeferSync();
    }

    @FunctionalInterface
    interface WalFlusher
    {
        void flushWal() throws RocksDBException;
    }

    @FunctionalInterface
    interface FatalErrorHandler
    {
        void onFatalError(RuntimeException error);
    }

    private final CommitThread _commitThread;
    private final DeferSyncPolicy _deferSyncPolicy;
    private final WalFlusher _walFlusher;
    private final FatalErrorHandler _fatalErrorHandler;

    RocksDBCommitter(final int notifyThreshold,
                     final long waitTimeoutMs,
                     final DeferSyncPolicy deferSyncPolicy,
                     final WalFlusher walFlusher,
                     final FatalErrorHandler fatalErrorHandler)
    {
        _commitThread = new CommitThread(notifyThreshold, waitTimeoutMs);
        _deferSyncPolicy = deferSyncPolicy;
        _walFlusher = walFlusher;
        _fatalErrorHandler = fatalErrorHandler;
    }

    void start()
    {
        _commitThread.start();
    }

    void stop()
    {
        _commitThread.close();
        if (Thread.currentThread() != _commitThread)
        {
            try
            {
                _commitThread.join();
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new StoreException("Commit thread has not shutdown", e);
            }
        }
    }

    <X> CompletableFuture<X> commitAsync(final CommitAction action, final X value)
    {
        CommitJob<X> job = new CommitJob<>(action, value);
        _commitThread.addJob(job, false);
        return job.getFuture();
    }

    private final class CommitThread extends Thread
    {
        private final int _jobQueueNotifyThreshold;
        private final long _committerWaitTimeoutMs;
        private final AtomicBoolean _stopped = new AtomicBoolean(false);
        private final Queue<CommitJob<?>> _jobQueue = new ConcurrentLinkedQueue<>();
        private final Object _lock = new Object();
        private final List<CommitJob<?>> _inProcessJobs = new ArrayList<>(256);

        private CommitThread(final int notifyThreshold, final long waitTimeoutMs)
        {
            super("rocksdb-commit-thread");
            _jobQueueNotifyThreshold = notifyThreshold;
            _committerWaitTimeoutMs = waitTimeoutMs;
        }

        @Override
        public void run()
        {
            while (true)
            {
                synchronized (_lock)
                {
                    while (!_stopped.get() && !hasJobs())
                    {
                        try
                        {
                            _lock.wait(_committerWaitTimeoutMs);
                        }
                        catch (InterruptedException e)
                        {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
                processJobs();
                if (_stopped.get() && !hasJobs())
                {
                    return;
                }
            }
        }

        private boolean hasJobs()
        {
            return !_jobQueue.isEmpty();
        }

        private void processJobs()
        {
            CommitJob<?> job;
            while ((job = _jobQueue.poll()) != null)
            {
                _inProcessJobs.add(job);
            }
            if (_inProcessJobs.isEmpty())
            {
                return;
            }

            boolean deferSync = _deferSyncPolicy.shouldDeferSync();
            try
            {
                for (CommitJob<?> commitJob : _inProcessJobs)
                {
                    try
                    {
                        commitJob.commit(deferSync);
                    }
                    catch (RuntimeException e)
                    {
                        commitJob.abort(e);
                    }
                }

                if (deferSync && hasSuccessfulJobs())
                {
                    try
                    {
                        _walFlusher.flushWal();
                    }
                    catch (RocksDBException e)
                    {
                        handleFlushWalFailure(e);
                        return;
                    }
                }

                for (CommitJob<?> commitJob : _inProcessJobs)
                {
                    commitJob.complete();
                }
            }
            catch (RuntimeException e)
            {
                for (CommitJob<?> commitJob : _inProcessJobs)
                {
                    commitJob.abort(e);
                }
            }
            finally
            {
                _inProcessJobs.clear();
            }
        }

        private boolean hasSuccessfulJobs()
        {
            for (CommitJob<?> job : _inProcessJobs)
            {
                if (job.isCommitted())
                {
                    return true;
                }
            }
            return false;
        }

        private void addJob(final CommitJob<?> commit, final boolean sync)
        {
            if (_stopped.get())
            {
                throw new StoreException("Commit thread is stopped");
            }
            _jobQueue.add(commit);
            if (sync || _jobQueue.size() >= _jobQueueNotifyThreshold)
            {
                synchronized (_lock)
                {
                    _lock.notifyAll();
                }
            }
        }

        private void close()
        {
            _stopped.set(true);
            synchronized (_lock)
            {
                _lock.notifyAll();
            }
        }

        private void handleFlushWalFailure(final RocksDBException cause)
        {
            RuntimeException failure = new StoreException("Failed to flush RocksDB WAL", cause);
            LOGGER.error("WAL flush failed; closing RocksDB message store", failure);
            for (CommitJob<?> commitJob : _inProcessJobs)
            {
                commitJob.abort(failure);
            }
            CommitJob<?> queued;
            while ((queued = _jobQueue.poll()) != null)
            {
                queued.abort(failure);
            }
            try
            {
                if (_fatalErrorHandler != null)
                {
                    _fatalErrorHandler.onFatalError(failure);
                }
            }
            finally
            {
                close();
            }
        }
    }

    private static final class CommitJob<X>
    {
        private final CommitAction _action;
        private final X _value;
        private final CompletableFuture<X> _future = new CompletableFuture<>();
        private boolean _committed;

        private CommitJob(final CommitAction action, final X value)
        {
            _action = action;
            _value = value;
        }

        private void commit(final boolean deferSync)
        {
            _action.commit(deferSync);
            _committed = true;
        }

        private void complete()
        {
            if (_committed)
            {
                _future.complete(_value);
            }
        }

        private void abort(final Throwable error)
        {
            if (!_future.isDone())
            {
                _future.completeExceptionally(error);
            }
        }

        private CompletableFuture<X> getFuture()
        {
            return _future;
        }

        private boolean isCommitted()
        {
            return _committed;
        }
    }
}
