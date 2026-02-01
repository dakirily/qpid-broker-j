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

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RocksDBTransactionTest
{
    @Test
    void commitTranAsyncReturnsCompletedFutureWhenNoCommitter() throws Exception
    {
        RocksDBTransaction.StoreAccess access = new EmptyAccess();
        RocksDBTransaction transaction = new RocksDBTransaction(access);

        CompletableFuture<Integer> future = transaction.commitTranAsync(7);

        assertEquals(7, future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void stageMessageCallsFlowToDiskForNonRocksMessage()
    {
        RocksDBTransaction.StoreAccess access = new EmptyAccess();
        RocksDBTransaction transaction = new RocksDBTransaction(access);

        @SuppressWarnings("unchecked")
        StoredMessage<TestMessageMetaData> message = mock(StoredMessage.class);
        transaction.stageMessage(message);

        verify(message).flowToDisk();
    }

    @Test
    void abortTranClearsPendingMessages() throws Exception
    {
        RocksDBTransaction.StoreAccess access = new EmptyAccess();
        RocksDBTransaction transaction = new RocksDBTransaction(access);

        TestMessageMetaData metaData = new TestMessageMetaData(1L, 1);
        RocksDBStoredMessage<TestMessageMetaData> stored =
                new RocksDBStoredMessage<>(new MinimalStoreAccess(), 1L, metaData);

        transaction.stageMessage(stored);

        List<?> pending = getPendingMessages(transaction);
        assertEquals(1, pending.size(), "Expected pending messages before abort");

        transaction.abortTran();

        pending = getPendingMessages(transaction);
        assertEquals(0, pending.size(), "Expected pending messages to be cleared");
    }

    private List<?> getPendingMessages(final RocksDBTransaction transaction) throws Exception
    {
        Field field = RocksDBTransaction.class.getDeclaredField("_pendingMessages");
        field.setAccessible(true);
        return (List<?>) field.get(transaction);
    }

    private static final class EmptyAccess implements RocksDBTransaction.StoreAccess
    {
        @Override
        public org.rocksdb.TransactionDB getTransactionDb()
        {
            return null;
        }

        @Override
        public org.rocksdb.WriteOptions createWriteOptions(final boolean deferSync)
        {
            throw new IllegalStateException("Should not be called for empty transaction");
        }

        @Override
        public void applyQueueEntriesToTransaction(final org.rocksdb.Transaction txn,
                                                   final List<RocksDBEnqueueRecord> enqueues,
                                                   final List<RocksDBEnqueueRecord> dequeues)
        {
            throw new IllegalStateException("Should not be called for empty transaction");
        }

        @Override
        public void applyXidRecordsToTransaction(final org.rocksdb.Transaction txn,
                                                 final List<XidRecord> xidRecords,
                                                 final List<byte[]> xidRemovals)
        {
            throw new IllegalStateException("Should not be called for empty transaction");
        }

        @Override
        public void storedSizeChangeOccurred(final int delta)
        {
        }

        @Override
        public boolean isRetryable(final org.rocksdb.RocksDBException exception)
        {
            return false;
        }

        @Override
        public int getTransactionRetryAttempts()
        {
            return 1;
        }

        @Override
        public long getTransactionRetryBaseSleepMillis()
        {
            return 0L;
        }

        @Override
        public long getTransactionLockTimeoutMs()
        {
            return 0L;
        }

        @Override
        public RocksDBCommitter getCommitter()
        {
            return null;
        }
    }

    private static final class MinimalStoreAccess implements RocksDBStoredMessage.StoreAccess
    {
        private final AtomicLong inMemory = new AtomicLong();
        private final AtomicLong evacuated = new AtomicLong();

        @Override
        public int getMessageChunkSize()
        {
            return 0;
        }

        @Override
        public int getDefaultChunkSize()
        {
            return 0;
        }

        @Override
        public boolean shouldChunk(final int contentSize)
        {
            return false;
        }

        @Override
        public byte[] loadContent(final long messageId)
        {
            return new byte[0];
        }

        @Override
        public MetaDataRecord loadMetaDataRecord(final long messageId)
        {
            return null;
        }

        @Override
        public byte[] loadChunkedContentSlice(final long messageId,
                                              final int contentSize,
                                              final int chunkSize,
                                              final int offset,
                                              final int length)
        {
            return new byte[0];
        }

        @Override
        public void storeChunk(final org.rocksdb.Transaction txn,
                               final long messageId,
                               final int chunkIndex,
                               final byte[] data)
        {
        }

        @Override
        public void storeChunkedMessage(final org.rocksdb.Transaction txn,
                                        final long messageId,
                                        final org.apache.qpid.server.store.StorableMessageMetaData metaData,
                                        final int chunkSize,
                                        final java.util.List<byte[]> chunks,
                                        final byte[] trailingChunk)
        {
        }

        @Override
        public void storeMessageMetadata(final org.rocksdb.Transaction txn,
                                         final long messageId,
                                         final org.apache.qpid.server.store.StorableMessageMetaData metaData,
                                         final boolean chunked,
                                         final int chunkSize)
        {
        }

        @Override
        public void storeMessage(final org.rocksdb.Transaction txn,
                                 final long messageId,
                                 final org.apache.qpid.server.store.StorableMessageMetaData metaData,
                                 final byte[] content)
        {
        }

        @Override
        public void deleteMessage(final long messageId)
        {
        }

        @Override
        public void storedSizeChangeOccurred(final int delta)
        {
        }

        @Override
        public void notifyMessageDeleted(final StoredMessage<?> message)
        {
        }

        @Override
        public AtomicLong getInMemorySize()
        {
            return inMemory;
        }

        @Override
        public AtomicLong getBytesEvacuatedFromMemory()
        {
            return evacuated;
        }

        @Override
        public byte[] emptyValue()
        {
            return new byte[0];
        }
    }
}
