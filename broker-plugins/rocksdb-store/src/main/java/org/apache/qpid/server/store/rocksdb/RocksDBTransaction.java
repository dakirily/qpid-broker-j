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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteOptions;

import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;

/**
 * Provides a RocksDB-backed transaction.
 * <br>
 * Thread-safety: not thread-safe.
 */
final class RocksDBTransaction implements Transaction
{
    interface StoreAccess
    {
        TransactionDB getTransactionDb();

        WriteOptions createWriteOptions(boolean deferSync);

        void applyQueueEntriesToTransaction(org.rocksdb.Transaction txn,
                                            List<RocksDBEnqueueRecord> enqueues,
                                            List<RocksDBEnqueueRecord> dequeues);

        void applyXidRecordsToTransaction(org.rocksdb.Transaction txn,
                                          List<XidRecord> xidRecords,
                                          List<byte[]> xidRemovals);

        void storedSizeChangeOccurred(int delta);

        boolean isRetryable(RocksDBException exception);

        int getTransactionRetryAttempts();

        long getTransactionRetryBaseSleepMillis();

        long getTransactionLockTimeoutMs();

        RocksDBCommitter getCommitter();
    }

    private final StoreAccess _access;
    private final TransactionDB _database;
    private final List<RocksDBStoredMessage<?>> _pendingMessages = new ArrayList<>();
    private final List<RocksDBEnqueueRecord> _enqueues = new ArrayList<>();
    private final List<RocksDBEnqueueRecord> _dequeues = new ArrayList<>();
    private final List<XidRecord> _xidRecords = new ArrayList<>();
    private final List<byte[]> _xidRemovals = new ArrayList<>();
    private int _storeSizeIncrease;

    RocksDBTransaction(final StoreAccess access)
    {
        _access = access;
        _database = access.getTransactionDb();
    }

    @Override
    public MessageEnqueueRecord enqueueMessage(final TransactionLogResource queue,
                                               final EnqueueableMessage message)
    {
        if (message != null && message.isPersistent())
        {
            StoredMessage<?> storedMessage = message.getStoredMessage();
            if (storedMessage != null)
            {
                stageMessage(storedMessage);
            }
        }
        RocksDBEnqueueRecord record = new RocksDBEnqueueRecord(queue.getId(), message.getMessageNumber());
        _enqueues.add(record);
        return record;
    }

    @Override
    public void dequeueMessage(final MessageEnqueueRecord enqueueRecord)
    {
        _dequeues.add(new RocksDBEnqueueRecord(enqueueRecord.getQueueId(), enqueueRecord.getMessageNumber()));
    }

    @Override
    public void commitTran()
    {
        commitInternal(false);
    }

    private void commitInternal(final boolean deferSync)
    {
        if (_enqueues.isEmpty()
            && _dequeues.isEmpty()
            && _xidRecords.isEmpty()
            && _xidRemovals.isEmpty()
            && _pendingMessages.isEmpty())
        {
            clear();
            return;
        }
        int retryAttempts = _access.getTransactionRetryAttempts();
        for (int attempt = 0; attempt < retryAttempts; attempt++)
        {
            WriteOptions writeOptions = _access.createWriteOptions(deferSync);
            TransactionOptions transactionOptions = new TransactionOptions();
            long lockTimeoutMs = _access.getTransactionLockTimeoutMs();
            if (lockTimeoutMs > 0L)
            {
                transactionOptions.setLockTimeout(lockTimeoutMs);
            }
            org.rocksdb.Transaction txn = _database.beginTransaction(writeOptions, transactionOptions);
            try
            {
                for (RocksDBStoredMessage<?> message : _pendingMessages)
                {
                    message.storeInTransaction(txn);
                }
                _access.applyQueueEntriesToTransaction(txn, _enqueues, _dequeues);
                _access.applyXidRecordsToTransaction(txn, _xidRecords, _xidRemovals);
                txn.commit();
                for (RocksDBStoredMessage<?> message : _pendingMessages)
                {
                    message.commitStore();
                }
                if (_storeSizeIncrease != 0)
                {
                    _access.storedSizeChangeOccurred(_storeSizeIncrease);
                }
                clear();
                return;
            }
            catch (RocksDBException e)
            {
                try
                {
                    txn.rollback();
                }
                catch (RocksDBException ignore)
                {
                }
                for (RocksDBStoredMessage<?> message : _pendingMessages)
                {
                    message.abortStore();
                }
                if (!_access.isRetryable(e) || attempt == retryAttempts - 1)
                {
                    throw new StoreException("Failed to commit transaction", e);
                }
                sleepBeforeRetry(attempt);
            }
            finally
            {
                txn.close();
                transactionOptions.close();
                writeOptions.close();
            }
        }
        throw new StoreException("Failed to commit transaction");
    }

    @Override
    public <X> CompletableFuture<X> commitTranAsync(final X val)
    {
        RocksDBCommitter committer = _access.getCommitter();
        if (committer == null)
        {
            commitTran();
            return CompletableFuture.completedFuture(val);
        }
        return committer.commitAsync(this::commitInternal, val);
    }

    @Override
    public void abortTran()
    {
        for (RocksDBStoredMessage<?> message : _pendingMessages)
        {
            message.abortStore();
        }
        clear();
    }

    @Override
    public void removeXid(final StoredXidRecord record)
    {
        _xidRemovals.add(RocksDBXidRecordMapper.encodeXidKey(record.getFormat(),
                                                             record.getGlobalId(),
                                                             record.getBranchId()));
    }

    @Override
    public StoredXidRecord recordXid(final long format,
                                     final byte[] globalId,
                                     final byte[] branchId,
                                     final EnqueueRecord[] enqueues,
                                     final DequeueRecord[] dequeues)
    {
        ensureMessagesStored(enqueues);
        byte[] key = RocksDBXidRecordMapper.encodeXidKey(format, globalId, branchId);
        byte[] value = RocksDBXidRecordMapper.encodeXidValue(format, globalId, branchId, enqueues, dequeues);
        _xidRecords.add(new XidRecord(key, value));
        return new RocksDBStoredXidRecord(format, globalId, branchId);
    }

    void stageMessage(final StoredMessage<?> storedMessage)
    {
        if (storedMessage instanceof RocksDBStoredMessage<?> rocksMessage)
        {
            if (!rocksMessage.isStored() && !_pendingMessages.contains(rocksMessage))
            {
                _pendingMessages.add(rocksMessage);
                int contentSize = rocksMessage.getContentSize();
                if (contentSize > 0)
                {
                    _storeSizeIncrease += contentSize;
                }
            }
        }
        else
        {
            storedMessage.flowToDisk();
        }
    }

    private void ensureMessagesStored(final Transaction.EnqueueRecord[] enqueues)
    {
        if (enqueues == null)
        {
            return;
        }
        for (Transaction.EnqueueRecord record : enqueues)
        {
            EnqueueableMessage<?> message = record.getMessage();
            if (message != null && message.isPersistent())
            {
                StoredMessage<?> storedMessage = message.getStoredMessage();
                if (storedMessage != null)
                {
                    stageMessage(storedMessage);
                }
            }
        }
    }

    private void sleepBeforeRetry(final int attempt)
    {
        long delay = _access.getTransactionRetryBaseSleepMillis() * (1L << attempt);
        try
        {
            Thread.sleep(delay);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private void clear()
    {
        _pendingMessages.clear();
        _enqueues.clear();
        _dequeues.clear();
        _xidRecords.clear();
        _xidRemovals.clear();
        _storeSizeIncrease = 0;
    }
}
