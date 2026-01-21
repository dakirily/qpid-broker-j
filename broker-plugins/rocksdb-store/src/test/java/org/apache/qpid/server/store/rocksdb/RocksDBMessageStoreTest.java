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

import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreTestCase;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestRecord;
import org.apache.qpid.server.store.TestMessageMetaData;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.rocksdb.RocksDBVirtualHost;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Runs standard message store tests against the RocksDB message store.
 *
 * Thread-safety: runs in a single-threaded test context.
 */
public class RocksDBMessageStoreTest extends MessageStoreTestCase
{
    private String _storeLocation;

    /**
     * Closes the store and removes test data.
     *
     * @throws Exception on cleanup errors.
     */
    @Override
    @AfterEach
    public void tearDown() throws Exception
    {
        try
        {
            MessageStore store = getStore();
            if (store != null)
            {
                store.closeMessageStore();
            }
        }
        finally
        {
            deleteStoreIfExists();
            super.tearDown();
        }
    }

    /**
     * Verifies that onDelete removes the store data.
     */
    @Test
    public void testOnDelete()
    {
        String storeLocation = getStore().getStoreLocation();
        File location = new File(storeLocation);
        assertTrue(location.exists(), "Store does not exist at " + storeLocation);

        getStore().closeMessageStore();
        assertTrue(location.exists(), "Store does not exist at " + storeLocation);

        getStore().onDelete(getVirtualHost());
        assertFalse(location.exists(), "Store exists at " + storeLocation);
    }

    /**
     * Verifies that onDelete fails while the store is open.
     */
    @Test
    public void testOnDeleteWhileOpenFails()
    {
        assertThrows(IllegalStateException.class,
                     () -> getStore().onDelete(getVirtualHost()),
                     "Expected onDelete to fail while the store is open");
    }

    /**
     * Verifies that next message id is preserved across reopen without messages.
     */
    @Test
    public void testNextMessageIdPersistsWithoutMessages()
    {
        long firstId = getStore().getNextMessageId();
        reopenStore();
        long secondId = getStore().getNextMessageId();
        assertEquals(firstId + 1, secondId, "Unexpected next message id after reopen");
    }

    /**
     * Verifies queue-scoped message instance iteration.
     */
    @Test
    public void testVisitMessageInstancesForQueue()
    {
        MessageStore store = getStore();
        UUID queueId1 = UUID.randomUUID();
        UUID queueId2 = UUID.randomUUID();
        TransactionLogResource queue1 = createTransactionLogResource(queueId1, "queue1");
        TransactionLogResource queue2 = createTransactionLogResource(queueId2, "queue2");

        StoredMessage<TestMessageMetaData> message1 =
                store.addMessage(new TestMessageMetaData(1L, 0)).allContentAdded();
        StoredMessage<TestMessageMetaData> message2 =
                store.addMessage(new TestMessageMetaData(2L, 0)).allContentAdded();
        StoredMessage<TestMessageMetaData> message3 =
                store.addMessage(new TestMessageMetaData(3L, 0)).allContentAdded();

        Transaction transaction = store.newTransaction();
        transaction.enqueueMessage(queue1, createEnqueueableMessage(message1));
        transaction.enqueueMessage(queue2, createEnqueueableMessage(message2));
        transaction.enqueueMessage(queue1, createEnqueueableMessage(message3));
        transaction.commitTran();

        List<Long> queue1Ids = new ArrayList<>();
        MessageStore.MessageStoreReader reader = store.newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queue1, record ->
            {
                queue1Ids.add(record.getMessageNumber());
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        Set<Long> expected = new HashSet<>();
        expected.add(message1.getMessageNumber());
        expected.add(message3.getMessageNumber());
        assertEquals(expected, new HashSet<>(queue1Ids), "Unexpected queue entries for queue1");
    }

    @Test
    public void testChunkedContentReadAndReopen()
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "chunkedQueue");
        int contentSize = 70 * 1024;
        byte[] content = new byte[contentSize];
        for (int i = 0; i < content.length; i++)
        {
            content[i] = (byte) (i & 0xff);
        }

        MessageHandle<TestMessageMetaData> handle =
                store.addMessage(new TestMessageMetaData(1L, contentSize));
        int step = 8192;
        for (int offset = 0; offset < content.length; offset += step)
        {
            int size = Math.min(step, content.length - offset);
            handle.addContent(QpidByteBuffer.wrap(content, offset, size));
        }
        StoredMessage<TestMessageMetaData> stored = handle.allContentAdded();

        Transaction enqueueTxn = store.newTransaction();
        enqueueTxn.enqueueMessage(queue, createEnqueueableMessage(stored));
        enqueueTxn.commitTran();

        byte[] read = new byte[contentSize];
        try (QpidByteBuffer buffer = stored.getContent(0, contentSize))
        {
            assertTrue(buffer.isDirect(), "Expected direct buffer for full content");
            buffer.get(read);
        }
        assertArrayEquals(content, read, "Content mismatch before reopen");

        byte[] slice = new byte[4096];
        try (QpidByteBuffer buffer = stored.getContent(1024, slice.length))
        {
            assertTrue(buffer.isDirect(), "Expected direct buffer for content slice");
            buffer.get(slice);
        }
        assertArrayEquals(java.util.Arrays.copyOfRange(content, 1024, 1024 + slice.length),
                          slice,
                          "Content slice mismatch");

        reopenStore();

        MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            StoredMessage<?> recovered = reader.getMessage(stored.getMessageNumber());
            byte[] reread = new byte[contentSize];
            try (QpidByteBuffer buffer = recovered.getContent(0, contentSize))
            {
                assertTrue(buffer.isDirect(), "Expected direct buffer for recovered content");
                buffer.get(reread);
            }
            assertArrayEquals(content, reread, "Content mismatch after reopen");
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testMessageStoreVersionTooNewFailsOpen() throws Exception
    {
        RocksDBMessageStore store = (RocksDBMessageStore) getStore();
        RocksDBEnvironment environment = store.getEnvironment();
        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle versionHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.VERSION);

        database.put(versionHandle, MESSAGE_STORE_VERSION_KEY, encodeLong(2L));
        store.closeMessageStore();

        RocksDBMessageStore reopen = new RocksDBMessageStore();
        try
        {
            StoreException exception = assertThrows(StoreException.class,
                                                    () -> reopen.openMessageStore(getVirtualHost()),
                                                    "Expected open to fail with newer store version");
            assertTrue(exception.getMessage().contains("Unsupported"),
                       "Unexpected message: " + exception.getMessage());
        }
        finally
        {
            reopen.closeMessageStore();
        }
    }

    @Test
    public void testMessageStoreVersionTooOldFailsOpen() throws Exception
    {
        RocksDBMessageStore store = (RocksDBMessageStore) getStore();
        RocksDBEnvironment environment = store.getEnvironment();
        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle versionHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.VERSION);

        database.put(versionHandle, MESSAGE_STORE_VERSION_KEY, encodeLong(0L));
        store.closeMessageStore();

        RocksDBMessageStore reopen = new RocksDBMessageStore();
        try
        {
            StoreException exception = assertThrows(StoreException.class,
                                                    () -> reopen.openMessageStore(getVirtualHost()),
                                                    "Expected open to fail with older store version");
            assertTrue(exception.getMessage().contains("requires upgrade"),
                       "Unexpected message: " + exception.getMessage());
        }
        finally
        {
            reopen.closeMessageStore();
        }
    }

    @Test
    public void testUpgradeStoreStructureRemovesOrphanedQueueSegments() throws Exception
    {
        RocksDBMessageStore store = (RocksDBMessageStore) getStore();
        RocksDBEnvironment environment = store.getEnvironment();
        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle segmentHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_SEG);

        UUID queueId = UUID.randomUUID();
        long segmentNo = 1L;
        QueueSegment segment = new QueueSegment(new long[]{1L}, new java.util.BitSet(), 1, 0);
        byte[] segmentKey = encodeQueueSegmentKey(queueId, segmentNo);
        database.put(segmentHandle, segmentKey, RocksDBQueueRecordMapper.encodeQueueSegment(segment));

        store.upgradeStoreStructure();

        byte[] stored = database.get(segmentHandle, segmentKey);
        assertNull(stored, "Expected orphaned queue segment to be removed");
    }

    @Test
    public void testAsyncCommitClosesStoreOnWalFlushFailure() throws Exception
    {
        RocksDBMessageStore store = (RocksDBMessageStore) getStore();
        java.util.concurrent.CountDownLatch closed = new java.util.concurrent.CountDownLatch(1);
        RocksDBCommitter failingCommitter = new RocksDBCommitter(1,
                                                                 50L,
                                                                 () -> true,
                                                                 () -> { throw new RocksDBException("flush failed"); },
                                                                 error ->
                                                                 {
                                                                     store.closeMessageStore();
                                                                     closed.countDown();
                                                                 });
        failingCommitter.start();

        RocksDBCommitter previous = replaceCommitter(store, failingCommitter);
        try
        {
            CompletableFuture<Long> future =
                    failingCommitter.commitAsync(deferSync -> { }, 1L);

            assertThrows(java.util.concurrent.ExecutionException.class,
                         () -> future.get(2, TimeUnit.SECONDS),
                         "Expected async commit to fail on WAL flush");
            assertTrue(closed.await(2, TimeUnit.SECONDS),
                       "Expected store close to complete after fatal WAL flush");
            assertThrows(IllegalStateException.class,
                         store::newTransaction,
                         "Expected store to be closed after fatal WAL flush");
        }
        finally
        {
            if (previous != null)
            {
                previous.stop();
            }
        }
    }

    @Test
    public void testChunkedContentReadFailsWhenChunkMissing()
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "missingChunkQueue");
        int contentSize = 70 * 1024;
        byte[] content = new byte[contentSize];
        for (int i = 0; i < content.length; i++)
        {
            content[i] = (byte) (i & 0xff);
        }

        MessageHandle<TestMessageMetaData> handle =
                store.addMessage(new TestMessageMetaData(1L, contentSize));
        int step = 8192;
        for (int offset = 0; offset < content.length; offset += step)
        {
            int size = Math.min(step, content.length - offset);
            handle.addContent(QpidByteBuffer.wrap(content, offset, size));
        }
        StoredMessage<TestMessageMetaData> stored = handle.allContentAdded();

        Transaction transaction = store.newTransaction();
        transaction.enqueueMessage(queue, createEnqueueableMessage(stored));
        transaction.commitTran();

        RocksDBMessageStore rocksStore = (RocksDBMessageStore) store;
        RocksDBEnvironment environment = rocksStore.getEnvironment();
        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle chunkHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CHUNKS);
        byte[] lastChunkKey = findLastChunkKey(database, chunkHandle, stored.getMessageNumber());
        try
        {
            database.delete(chunkHandle, lastChunkKey);
        }
        catch (org.rocksdb.RocksDBException e)
        {
            throw new StoreException("Failed to delete chunk for test", e);
        }

        reopenStore();

        MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            StoredMessage<?> recovered = reader.getMessage(stored.getMessageNumber());
            assertNotNull(recovered, "Expected message to be present after chunk delete");
            assertThrows(StoreException.class, () ->
            {
                try (QpidByteBuffer buffer = recovered.getContent(0, contentSize))
                {
                    buffer.get(new byte[contentSize]);
                }
            }, "Expected missing chunk to throw StoreException");
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testChunkedContentReadFailsWhenChunkTruncated()
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "truncatedChunkQueue");
        int contentSize = 70 * 1024;
        byte[] content = new byte[contentSize];
        for (int i = 0; i < content.length; i++)
        {
            content[i] = (byte) (i & 0xff);
        }

        MessageHandle<TestMessageMetaData> handle =
                store.addMessage(new TestMessageMetaData(1L, contentSize));
        int step = 8192;
        for (int offset = 0; offset < content.length; offset += step)
        {
            int size = Math.min(step, content.length - offset);
            handle.addContent(QpidByteBuffer.wrap(content, offset, size));
        }
        StoredMessage<TestMessageMetaData> stored = handle.allContentAdded();

        Transaction transaction = store.newTransaction();
        transaction.enqueueMessage(queue, createEnqueueableMessage(stored));
        transaction.commitTran();

        RocksDBMessageStore rocksStore = (RocksDBMessageStore) store;
        RocksDBEnvironment environment = rocksStore.getEnvironment();
        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle chunkHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CHUNKS);
        byte[] lastChunkKey = findLastChunkKey(database, chunkHandle, stored.getMessageNumber());
        try
        {
            database.put(chunkHandle, lastChunkKey, new byte[] { 1 });
        }
        catch (org.rocksdb.RocksDBException e)
        {
            throw new StoreException("Failed to truncate chunk for test", e);
        }

        reopenStore();

        MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            StoredMessage<?> recovered = reader.getMessage(stored.getMessageNumber());
            assertNotNull(recovered, "Expected message to be present after chunk truncation");
            assertThrows(StoreException.class, () ->
            {
                try (QpidByteBuffer buffer = recovered.getContent(0, contentSize))
                {
                    buffer.get(new byte[contentSize]);
                }
            }, "Expected truncated chunk to throw StoreException");
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testQueueSegmentsSkipAckedAndPreserveOrder()
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "queueSegments");

        List<StoredMessage<TestMessageMetaData>> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            messages.add(store.addMessage(new TestMessageMetaData(i + 1, 0)).allContentAdded());
        }

        Transaction enqueueTxn = store.newTransaction();
        List<MessageEnqueueRecord> records = new ArrayList<>();
        for (StoredMessage<TestMessageMetaData> message : messages)
        {
            records.add(enqueueTxn.enqueueMessage(queue, createEnqueueableMessage(message)));
        }
        enqueueTxn.commitTran();

        Transaction dequeueTxn = store.newTransaction();
        dequeueTxn.dequeueMessage(records.get(2));
        dequeueTxn.commitTran();

        List<Long> actual = new ArrayList<>();
        MessageStore.MessageStoreReader reader = store.newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        List<Long> expected = List.of(messages.get(0).getMessageNumber(),
                                      messages.get(1).getMessageNumber(),
                                      messages.get(3).getMessageNumber(),
                                      messages.get(4).getMessageNumber());
        assertEquals(expected, actual, "Unexpected queue order after ack");
    }

    @Test
    public void testQueueSegmentsAckPersistedOnReopen()
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "queueSegmentsPersist");

        StoredMessage<TestMessageMetaData> message1 =
                store.addMessage(new TestMessageMetaData(1L, 0)).allContentAdded();
        StoredMessage<TestMessageMetaData> message2 =
                store.addMessage(new TestMessageMetaData(2L, 0)).allContentAdded();
        StoredMessage<TestMessageMetaData> message3 =
                store.addMessage(new TestMessageMetaData(3L, 0)).allContentAdded();

        Transaction enqueueTxn = store.newTransaction();
        MessageEnqueueRecord record1 = enqueueTxn.enqueueMessage(queue, createEnqueueableMessage(message1));
        MessageEnqueueRecord record2 = enqueueTxn.enqueueMessage(queue, createEnqueueableMessage(message2));
        MessageEnqueueRecord record3 = enqueueTxn.enqueueMessage(queue, createEnqueueableMessage(message3));
        enqueueTxn.commitTran();

        Transaction dequeueTxn = store.newTransaction();
        dequeueTxn.dequeueMessage(record2);
        dequeueTxn.commitTran();

        reopenStore();

        List<Long> actual = new ArrayList<>();
        MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        List<Long> expected = List.of(message1.getMessageNumber(), message3.getMessageNumber());
        assertEquals(expected, actual, "Unexpected queue order after reopen");
    }

    @Test
    public void testQueueSegmentsOutOfOrderAckPersistedOnReopen()
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "queueSegmentsOooAck");

        List<StoredMessage<TestMessageMetaData>> messages = new ArrayList<>();
        for (int i = 0; i < 20; i++)
        {
            messages.add(store.addMessage(new TestMessageMetaData(i + 1, 0)).allContentAdded());
        }

        Transaction enqueueTxn = store.newTransaction();
        List<MessageEnqueueRecord> records = new ArrayList<>();
        for (StoredMessage<TestMessageMetaData> message : messages)
        {
            records.add(enqueueTxn.enqueueMessage(queue, createEnqueueableMessage(message)));
        }
        enqueueTxn.commitTran();

        List<MessageEnqueueRecord> shuffled = new ArrayList<>(records);
        Collections.shuffle(shuffled, new Random(42));

        Set<Long> expectedRemaining = new HashSet<>();
        int toAck = shuffled.size() / 2;
        Transaction dequeueTxn = store.newTransaction();
        for (int i = 0; i < shuffled.size(); i++)
        {
            MessageEnqueueRecord record = shuffled.get(i);
            if (i < toAck)
            {
                dequeueTxn.dequeueMessage(record);
            }
            else
            {
                expectedRemaining.add(record.getMessageNumber());
            }
        }
        dequeueTxn.commitTran();

        reopenStore();

        Set<Long> actualRemaining = new HashSet<>();
        MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queue, record ->
            {
                actualRemaining.add(record.getMessageNumber());
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        assertEquals(expectedRemaining, actualRemaining, "Unexpected remaining messages after reopen");
    }

    @Test
    public void testDistributedTransactionRoundTripAfterReopen()
    {
        MessageStore store = getStore();
        UUID queueId1 = UUID.randomUUID();
        UUID queueId2 = UUID.randomUUID();
        TransactionLogResource queue1 = createTransactionLogResource(queueId1, "xidQueue1");
        TransactionLogResource queue2 = createTransactionLogResource(queueId2, "xidQueue2");

        StoredMessage<TestMessageMetaData> message1 =
                store.addMessage(new TestMessageMetaData(1L, 0)).allContentAdded();
        StoredMessage<TestMessageMetaData> message2 =
                store.addMessage(new TestMessageMetaData(2L, 0)).allContentAdded();
        StoredMessage<TestMessageMetaData> message3 =
                store.addMessage(new TestMessageMetaData(3L, 0)).allContentAdded();
        StoredMessage<TestMessageMetaData> message4 =
                store.addMessage(new TestMessageMetaData(4L, 0)).allContentAdded();

        TestRecord enqueue1 = new TestRecord(queue1, createEnqueueableMessage(message1));
        TestRecord enqueue2 = new TestRecord(queue2, createEnqueueableMessage(message2));
        TestRecord dequeue1 = new TestRecord(queue1, createEnqueueableMessage(message3));
        TestRecord dequeue2 = new TestRecord(queue2, createEnqueueableMessage(message4));

        byte[] globalId = new byte[130];
        byte[] branchId = new byte[260];
        for (int i = 0; i < globalId.length; i++)
        {
            globalId[i] = (byte) i;
        }
        for (int i = 0; i < branchId.length; i++)
        {
            branchId[i] = (byte) (255 - i);
        }
        long format = 0x0102030405060708L;

        Transaction transaction = store.newTransaction();
        Transaction.EnqueueRecord[] enqueues = { enqueue1, enqueue2 };
        Transaction.DequeueRecord[] dequeues = { dequeue1, dequeue2 };
        transaction.recordXid(format, globalId, branchId, enqueues, dequeues);
        transaction.commitTran();

        reopenStore();

        MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        List<Transaction.EnqueueRecord> actualEnqueues = new ArrayList<>();
        List<Transaction.DequeueRecord> actualDequeues = new ArrayList<>();
        List<Transaction.StoredXidRecord> actualXids = new ArrayList<>();
        try
        {
            reader.visitDistributedTransactions((xid, visitedEnqueues, visitedDequeues) ->
            {
                actualXids.add(xid);
                Collections.addAll(actualEnqueues, visitedEnqueues);
                Collections.addAll(actualDequeues, visitedDequeues);
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        assertEquals(1, actualXids.size(), "Unexpected number of XID records");
        Transaction.StoredXidRecord actualXid = actualXids.get(0);
        assertEquals(format, actualXid.getFormat(), "Unexpected XID format");
        assertArrayEquals(globalId, actualXid.getGlobalId(), "Unexpected XID global id");
        assertArrayEquals(branchId, actualXid.getBranchId(), "Unexpected XID branch id");

        assertEquals(enqueues.length, actualEnqueues.size(), "Unexpected enqueue count");
        for (int i = 0; i < enqueues.length; i++)
        {
            assertEquals(enqueues[i].getResource().getId(),
                         actualEnqueues.get(i).getResource().getId(),
                         "Unexpected enqueue queue id");
            assertEquals(enqueues[i].getMessage().getMessageNumber(),
                         actualEnqueues.get(i).getMessage().getMessageNumber(),
                         "Unexpected enqueue message id");
        }

        assertEquals(dequeues.length, actualDequeues.size(), "Unexpected dequeue count");
        for (int i = 0; i < dequeues.length; i++)
        {
            assertEquals(dequeues[i].getEnqueueRecord().getQueueId(),
                         actualDequeues.get(i).getEnqueueRecord().getQueueId(),
                         "Unexpected dequeue queue id");
            assertEquals(dequeues[i].getEnqueueRecord().getMessageNumber(),
                         actualDequeues.get(i).getEnqueueRecord().getMessageNumber(),
                         "Unexpected dequeue message id");
        }
    }

    @Test
    public void testOrphanedChunksRemovedOnUpgrade() throws Exception
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "orphanedChunkQueue");
        int contentSize = 70 * 1024;
        byte[] content = new byte[contentSize];
        for (int i = 0; i < content.length; i++)
        {
            content[i] = (byte) (i & 0xff);
        }

        MessageHandle<TestMessageMetaData> handle =
                store.addMessage(new TestMessageMetaData(1L, contentSize));
        try (QpidByteBuffer buffer = QpidByteBuffer.wrap(content))
        {
            handle.addContent(buffer);
        }
        StoredMessage<TestMessageMetaData> stored = handle.allContentAdded();

        Transaction enqueueTxn = store.newTransaction();
        enqueueTxn.enqueueMessage(queue, createEnqueueableMessage(stored));
        enqueueTxn.commitTran();

        RocksDBMessageStore rocksStore = (RocksDBMessageStore) store;
        RocksDBEnvironment environment = rocksStore.getEnvironment();
        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle chunkHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CHUNKS);

        long messageId = stored.getMessageNumber();
        byte[] validChunkKey = ByteBuffer.allocate(12).putLong(messageId).putInt(0).array();
        byte[] validChunk = database.get(chunkHandle, validChunkKey);
        assertTrue(validChunk != null && validChunk.length > 0, "Expected stored chunk to exist");

        long orphanId = messageId + 1000;
        byte[] orphanChunkKey = ByteBuffer.allocate(12).putLong(orphanId).putInt(0).array();
        database.put(chunkHandle, orphanChunkKey, new byte[]{1, 2, 3});
        assertTrue(database.get(chunkHandle, orphanChunkKey) != null, "Expected orphaned chunk to be present");

        store.upgradeStoreStructure();

        assertTrue(database.get(chunkHandle, orphanChunkKey) == null, "Expected orphaned chunk to be removed");
        assertTrue(database.get(chunkHandle, validChunkKey) != null, "Expected valid chunk to remain");
    }

    @Test
    public void testAbortedTransactionDoesNotPersistMessage()
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "abortTxnQueue");

        int contentSize = 48 * 1024;
        byte[] content = new byte[contentSize];
        for (int i = 0; i < content.length; i++)
        {
            content[i] = (byte) (i & 0xff);
        }

        MessageHandle<TestMessageMetaData> handle =
                store.addMessage(new TestMessageMetaData(1L, contentSize));
        try (QpidByteBuffer buffer = QpidByteBuffer.wrap(content))
        {
            handle.addContent(buffer);
        }
        StoredMessage<TestMessageMetaData> stored = handle.allContentAdded();

        Transaction transaction = store.newTransaction();
        transaction.enqueueMessage(queue, createEnqueueableMessage(stored));
        transaction.abortTran();

        reopenStore();

        MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            StoredMessage<?> recovered = reader.getMessage(stored.getMessageNumber());
            assertNull(recovered, "Expected aborted message to be absent");
            List<Long> actual = new ArrayList<>();
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
            assertTrue(actual.isEmpty(), "Expected no queue entries after abort");
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testOrphanedSegmentsRemovedOnUpgrade() throws Exception
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "queueSegmentsOrphan");

        StoredMessage<TestMessageMetaData> message =
                store.addMessage(new TestMessageMetaData(1L, 0)).allContentAdded();
        Transaction enqueueTxn = store.newTransaction();
        enqueueTxn.enqueueMessage(queue, createEnqueueableMessage(message));
        enqueueTxn.commitTran();

        RocksDBMessageStore rocksStore = (RocksDBMessageStore) store;
        RocksDBEnvironment environment = rocksStore.getEnvironment();
        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle segmentHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_SEG);
        ColumnFamilyHandle stateHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_STATE);

        long segmentNo = message.getMessageNumber() >>> 16;
        byte[] segmentKey = ByteBuffer.allocate(24)
                                      .putLong(queueId.getMostSignificantBits())
                                      .putLong(queueId.getLeastSignificantBits())
                                      .putLong(segmentNo)
                                      .array();
        assertTrue(database.get(segmentHandle, segmentKey) != null, "Expected queue segment to exist");

        byte[] stateKey = ByteBuffer.allocate(16)
                                    .putLong(queueId.getMostSignificantBits())
                                    .putLong(queueId.getLeastSignificantBits())
                                    .array();
        database.delete(stateHandle, stateKey);

        store.upgradeStoreStructure();

        assertTrue(database.get(segmentHandle, segmentKey) == null, "Expected orphaned segment to be removed");
    }

    @Test
    public void testChunkedMessageEnqueueAtomicCommit() throws Exception
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "chunkedAtomicQueue");

        int contentSize = 70 * 1024;
        byte[] content = new byte[contentSize];
        for (int i = 0; i < content.length; i++)
        {
            content[i] = (byte) (i & 0xff);
        }

        MessageHandle<TestMessageMetaData> handle =
                store.addMessage(new TestMessageMetaData(1L, contentSize));
        int step = 8192;
        for (int offset = 0; offset < content.length; offset += step)
        {
            int size = Math.min(step, content.length - offset);
            handle.addContent(QpidByteBuffer.wrap(content, offset, size));
        }
        StoredMessage<TestMessageMetaData> stored = handle.allContentAdded();

        Transaction transaction = store.newTransaction();
        transaction.enqueueMessage(queue, createEnqueueableMessage(stored));

        MessageStore.MessageStoreReader reader = store.newMessageStoreReader();
        try
        {
            assertNull(reader.getMessage(stored.getMessageNumber()), "Expected message to be absent before commit");
            List<Long> actual = new ArrayList<>();
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
            assertTrue(actual.isEmpty(), "Expected no queue entries before commit");
        }
        finally
        {
            reader.close();
        }

        transaction.commitTran();

        reader = store.newMessageStoreReader();
        try
        {
            StoredMessage<?> recovered = reader.getMessage(stored.getMessageNumber());
            assertNotNull(recovered, "Expected message to be present after commit");
            byte[] read = new byte[contentSize];
            try (QpidByteBuffer buffer = recovered.getContent(0, contentSize))
            {
                buffer.get(read);
            }
            assertArrayEquals(content, read, "Content mismatch after commit");

            List<Long> actual = new ArrayList<>();
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
            assertEquals(List.of(stored.getMessageNumber()), actual, "Unexpected queue entries after commit");
        }
        finally
        {
            reader.close();
        }

        reopenStore();

        reader = getStore().newMessageStoreReader();
        try
        {
            StoredMessage<?> recovered = reader.getMessage(stored.getMessageNumber());
            assertNotNull(recovered, "Expected message to be present after reopen");
            byte[] read = new byte[contentSize];
            try (QpidByteBuffer buffer = recovered.getContent(0, contentSize))
            {
                buffer.get(read);
            }
            assertArrayEquals(content, read, "Content mismatch after reopen");

            List<Long> actual = new ArrayList<>();
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
            assertEquals(List.of(stored.getMessageNumber()), actual, "Unexpected queue entries after reopen");
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testAsyncCommitFlushesOnTimeout() throws Exception
    {
        applyCommitterSettings(1000, 50L);
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "asyncTimeoutQueue");

        StoredMessage<TestMessageMetaData> message =
                store.addMessage(new TestMessageMetaData(1L, 0)).allContentAdded();
        Transaction transaction = store.newTransaction();
        transaction.enqueueMessage(queue, createEnqueueableMessage(message));
        CompletableFuture<Long> future = transaction.commitTranAsync(message.getMessageNumber());

        assertEquals(message.getMessageNumber(), future.get(5, TimeUnit.SECONDS),
                     "Async commit did not complete within timeout");

        List<Long> actual = new ArrayList<>();
        MessageStore.MessageStoreReader reader = store.newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        assertEquals(List.of(message.getMessageNumber()), actual,
                     "Unexpected queue entries after async timeout flush");
    }

    @Test
    public void testAsyncCommitChunkedMessagePersists() throws Exception
    {
        applyCommitterSettings(2, 500L);
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "asyncChunkedQueue");

        int contentSize = 70 * 1024;
        byte[] content = new byte[contentSize];
        for (int i = 0; i < content.length; i++)
        {
            content[i] = (byte) (i & 0xff);
        }

        MessageHandle<TestMessageMetaData> handle =
                store.addMessage(new TestMessageMetaData(1L, contentSize));
        int step = 8192;
        for (int offset = 0; offset < content.length; offset += step)
        {
            int size = Math.min(step, content.length - offset);
            handle.addContent(QpidByteBuffer.wrap(content, offset, size));
        }
        StoredMessage<TestMessageMetaData> stored = handle.allContentAdded();

        Transaction transaction = store.newTransaction();
        transaction.enqueueMessage(queue, createEnqueueableMessage(stored));
        CompletableFuture<Long> future = transaction.commitTranAsync(stored.getMessageNumber());

        assertEquals(stored.getMessageNumber(), future.get(5, TimeUnit.SECONDS),
                     "Async commit did not complete for chunked message");

        MessageStore.MessageStoreReader reader = store.newMessageStoreReader();
        try
        {
            StoredMessage<?> recovered = reader.getMessage(stored.getMessageNumber());
            assertNotNull(recovered, "Expected message to be present after async commit");
            byte[] read = new byte[contentSize];
            try (QpidByteBuffer buffer = recovered.getContent(0, contentSize))
            {
                buffer.get(read);
            }
            assertArrayEquals(content, read, "Content mismatch after async commit");
        }
        finally
        {
            reader.close();
        }

        reopenStore();

        reader = getStore().newMessageStoreReader();
        try
        {
            StoredMessage<?> recovered = reader.getMessage(stored.getMessageNumber());
            assertNotNull(recovered, "Expected message to be present after reopen");
            byte[] read = new byte[contentSize];
            try (QpidByteBuffer buffer = recovered.getContent(0, contentSize))
            {
                buffer.get(read);
            }
            assertArrayEquals(content, read, "Content mismatch after reopen");

            List<Long> actual = new ArrayList<>();
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
            assertEquals(List.of(stored.getMessageNumber()), actual, "Unexpected queue entries after reopen");
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testDurabilityProfileStrictRestart() throws Exception
    {
        applyWriteSettings(true, false);
        assertMessagePersistedAfterRestart("durabilityStrictQueue");
    }

    @Test
    public void testDurabilityProfileBalancedRestart() throws Exception
    {
        applyWriteSettings(false, false);
        assertMessagePersistedAfterRestart("durabilityBalancedQueue");
    }

    @Test
    public void testDurabilityProfileFastRestart() throws Exception
    {
        applyWriteSettings(false, true);
        assertMessagePersistedAfterRestart("durabilityFastQueue");
    }

    @Test
    public void testConcurrentNextMessageIdUnique() throws Exception
    {
        MessageStore store = getStore();
        int threads = 8;
        int perThread = 200;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        ConcurrentLinkedQueue<Long> ids = new ConcurrentLinkedQueue<>();
        try
        {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < threads; i++)
            {
                tasks.add(() ->
                {
                    for (int j = 0; j < perThread; j++)
                    {
                        ids.add(store.getNextMessageId());
                    }
                    return null;
                });
            }
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures)
            {
                future.get();
            }
        }
        finally
        {
            executor.shutdownNow();
        }

        assertEquals(threads * perThread, ids.size(), "Unexpected id count");
        Set<Long> unique = new HashSet<>(ids);
        assertEquals(ids.size(), unique.size(), "Duplicate ids detected");
    }

    @Test
    public void testConcurrentEnqueueDequeueSameQueue() throws Exception
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "concurrentQueue");

        int threads = 4;
        int perThread = 25;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        ConcurrentLinkedQueue<MessageEnqueueRecord> records = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> allMessageIds = new ConcurrentLinkedQueue<>();

        try
        {
            List<Callable<Void>> enqueueTasks = new ArrayList<>();
            for (int i = 0; i < threads; i++)
            {
                enqueueTasks.add(() ->
                {
                    Transaction txn = store.newTransaction();
                    for (int j = 0; j < perThread; j++)
                    {
                        StoredMessage<TestMessageMetaData> message =
                                store.addMessage(new TestMessageMetaData(1L, 0)).allContentAdded();
                        MessageEnqueueRecord record =
                                txn.enqueueMessage(queue, createEnqueueableMessage(message));
                        records.add(record);
                        allMessageIds.add(message.getMessageNumber());
                    }
                    txn.commitTran();
                    return null;
                });
            }
            List<Future<Void>> futures = executor.invokeAll(enqueueTasks);
            for (Future<Void> future : futures)
            {
                future.get();
            }
        }
        finally
        {
            executor.shutdownNow();
        }

        List<MessageEnqueueRecord> recordList = new ArrayList<>(records);
        int total = recordList.size();
        int toDequeue = total / 2;
        Set<Long> dequeuedIds = ConcurrentHashMap.newKeySet();
        ExecutorService dequeueExecutor = Executors.newFixedThreadPool(threads);
        try
        {
            List<Callable<Void>> dequeueTasks = new ArrayList<>();
            int chunkSize = Math.max(1, toDequeue / threads);
            for (int i = 0; i < threads; i++)
            {
                int start = i * chunkSize;
                int end = i == threads - 1 ? toDequeue : Math.min(toDequeue, start + chunkSize);
                if (start >= end)
                {
                    break;
                }
                List<MessageEnqueueRecord> slice = recordList.subList(start, end);
                dequeueTasks.add(() ->
                {
                    Transaction txn = store.newTransaction();
                    for (MessageEnqueueRecord record : slice)
                    {
                        txn.dequeueMessage(record);
                        dequeuedIds.add(record.getMessageNumber());
                    }
                    txn.commitTran();
                    return null;
                });
            }
            List<Future<Void>> futures = dequeueExecutor.invokeAll(dequeueTasks);
            for (Future<Void> future : futures)
            {
                future.get();
            }
        }
        finally
        {
            dequeueExecutor.shutdownNow();
        }

        Set<Long> expectedRemaining = new HashSet<>(allMessageIds);
        expectedRemaining.removeAll(dequeuedIds);

        List<Long> actual = new ArrayList<>();
        MessageStore.MessageStoreReader reader = store.newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        Set<Long> actualRemaining = new HashSet<>(actual);
        assertEquals(expectedRemaining, actualRemaining, "Unexpected remaining messages after concurrent ops");
    }

    @Test
    public void testAsyncCommitCoalescingPersistsMessages() throws Exception
    {
        applyCommitterSettings(2, 500L);
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "asyncCoalesceQueue");

        int commitCount = 6;
        List<Long> expected = new ArrayList<>();
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (int i = 0; i < commitCount; i++)
        {
            StoredMessage<TestMessageMetaData> message =
                    store.addMessage(new TestMessageMetaData(i + 1L, 0)).allContentAdded();
            expected.add(message.getMessageNumber());
            Transaction transaction = store.newTransaction();
            transaction.enqueueMessage(queue, createEnqueueableMessage(message));
            futures.add(transaction.commitTranAsync(message.getMessageNumber()));
        }

        for (int i = 0; i < futures.size(); i++)
        {
            assertEquals(expected.get(i), futures.get(i).get(5, TimeUnit.SECONDS),
                         "Unexpected async commit result");
        }

        List<Long> actual = new ArrayList<>();
        MessageStore.MessageStoreReader reader = store.newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        assertEquals(new HashSet<>(expected), new HashSet<>(actual),
                     "Unexpected queue entries after async commit");
    }

    @Test
    public void testAsyncCommitDrainsOnShutdown() throws Exception
    {
        applyCommitterSettings(1000, 60000L);
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, "asyncShutdownQueue");

        List<Long> expected = new ArrayList<>();
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++)
        {
            StoredMessage<TestMessageMetaData> message =
                    store.addMessage(new TestMessageMetaData(i + 10L, 0)).allContentAdded();
            expected.add(message.getMessageNumber());
            Transaction transaction = store.newTransaction();
            transaction.enqueueMessage(queue, createEnqueueableMessage(message));
            futures.add(transaction.commitTranAsync(message.getMessageNumber()));
        }

        store.closeMessageStore();

        for (int i = 0; i < futures.size(); i++)
        {
            assertEquals(expected.get(i), futures.get(i).get(5, TimeUnit.SECONDS),
                         "Async commit did not complete before shutdown");
        }

        reopenStore();
        MessageStore reopen = getStore();
        List<Long> actual = new ArrayList<>();
        MessageStore.MessageStoreReader reader = reopen.newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        assertEquals(new HashSet<>(expected), new HashSet<>(actual),
                     "Unexpected queue entries after shutdown drain");
    }

    /**
     * Creates a RocksDB virtual host stub for message store tests.
     *
     * @return the virtual host.
     */
    @Override
    protected VirtualHost<?> createVirtualHost()
    {
        Assumptions.assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        _storeLocation = TMP_FOLDER + File.separator + getTestName();
        deleteStoreIfExists();

        final RocksDBVirtualHost<?> parent = mock(RocksDBVirtualHost.class);
        when(parent.getStorePath()).thenReturn(_storeLocation);
        when(parent.getCreateIfMissing()).thenReturn(true);
        when(parent.getCreateMissingColumnFamilies()).thenReturn(true);
        when(parent.getMaxOpenFiles()).thenReturn(0);
        when(parent.getMaxBackgroundJobs()).thenReturn(0);
        when(parent.getMaxSubcompactions()).thenReturn(0);
        when(parent.getWalDir()).thenReturn("");
        when(parent.getBytesPerSync()).thenReturn(0L);
        when(parent.getWalBytesPerSync()).thenReturn(0L);
        when(parent.getEnableStatistics()).thenReturn(false);
        when(parent.getStatsDumpPeriodSec()).thenReturn(0);
        when(parent.getMaxTotalWalSize()).thenReturn(0L);
        when(parent.getWalTtlSeconds()).thenReturn(0);
        when(parent.getWalSizeLimitMb()).thenReturn(0);
        when(parent.getWriteBufferSize()).thenReturn(0L);
        when(parent.getMaxWriteBufferNumber()).thenReturn(0);
        when(parent.getMinWriteBufferNumberToMerge()).thenReturn(0);
        when(parent.getTargetFileSizeBase()).thenReturn(0L);
        when(parent.getLevelCompactionDynamicLevelBytes()).thenReturn(false);
        when(parent.getCompactionStyle()).thenReturn("");
        when(parent.getCompressionType()).thenReturn("");
        when(parent.getBlockCacheSize()).thenReturn(0L);
        when(parent.getBlockSize()).thenReturn(0L);
        when(parent.getBloomFilterBitsPerKey()).thenReturn(0);
        when(parent.getCacheIndexAndFilterBlocks()).thenReturn(false);
        when(parent.getPinL0FilterAndIndexBlocksInCache()).thenReturn(false);
        return parent;
    }

    /**
     * Removes the store directory if it exists.
     */
    private void deleteStoreIfExists()
    {
        if (_storeLocation != null)
        {
            File location = new File(_storeLocation);
            if (location.exists())
            {
                FileUtils.delete(location, true);
            }
        }
    }

    /**
     * Creates the RocksDB message store instance.
     *
     * @return the message store.
     */
    @Override
    protected MessageStore createMessageStore()
    {
        return new RocksDBMessageStore();
    }

    /**
     * Indicates whether flow-to-disk tests are supported.
     *
     * @return true when flow-to-disk is supported.
     */
    @Override
    protected boolean flowToDiskSupported()
    {
        return true;
    }

    /**
     * Creates a transaction log resource stub.
     *
     * @param queueId queue id.
     * @param name queue name.
     *
     * @return the resource.
     */
    private TransactionLogResource createTransactionLogResource(final UUID queueId, final String name)
    {
        TransactionLogResource queue = mock(TransactionLogResource.class);
        when(queue.getId()).thenReturn(queueId);
        when(queue.getName()).thenReturn(name);
        when(queue.getMessageDurability()).thenReturn(MessageDurability.DEFAULT);
        return queue;
    }

    private void applyWriteSettings(final boolean writeSync, final boolean disableWAL)
    {
        RocksDBVirtualHost<?> parent = (RocksDBVirtualHost<?>) getVirtualHost();
        when(parent.getWriteSync()).thenReturn(writeSync);
        when(parent.getDisableWAL()).thenReturn(disableWAL);
        reopenStore();
    }

    private void applyCommitterSettings(final int notifyThreshold, final long waitTimeoutMs)
    {
        RocksDBVirtualHost<?> parent = (RocksDBVirtualHost<?>) getVirtualHost();
        when(parent.getCommitterNotifyThreshold()).thenReturn(notifyThreshold);
        when(parent.getCommitterWaitTimeoutMs()).thenReturn(waitTimeoutMs);
        reopenStore();
    }

    private void assertMessagePersistedAfterRestart(final String queueName) throws Exception
    {
        MessageStore store = getStore();
        UUID queueId = UUID.randomUUID();
        TransactionLogResource queue = createTransactionLogResource(queueId, queueName);

        StoredMessage<TestMessageMetaData> message =
                store.addMessage(new TestMessageMetaData(1L, 0)).allContentAdded();
        long messageNumber = message.getMessageNumber();

        Transaction transaction = store.newTransaction();
        transaction.enqueueMessage(queue, createEnqueueableMessage(message));
        transaction.commitTran();

        reopenStore();

        MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            StoredMessage<?> recovered = reader.getMessage(messageNumber);
            assertNotNull(recovered, "Expected message after restart");

            List<Long> actual = new ArrayList<>();
            reader.visitMessageInstances(queue, record ->
            {
                actual.add(record.getMessageNumber());
                return true;
            });
            assertEquals(List.of(messageNumber), actual, "Unexpected queue entries after restart");
        }
        finally
        {
            reader.close();
        }
    }

    /**
     * Creates an enqueueable message wrapper for a stored message.
     *
     * @param message stored message.
     *
     * @return the enqueueable message.
     */
    private EnqueueableMessage<TestMessageMetaData> createEnqueueableMessage(
            final StoredMessage<TestMessageMetaData> message)
    {
        EnqueueableMessage<TestMessageMetaData> enqueueableMessage = mock(EnqueueableMessage.class);
        when(enqueueableMessage.isPersistent()).thenReturn(true);
        when(enqueueableMessage.getMessageNumber()).thenReturn(message.getMessageNumber());
        when(enqueueableMessage.getStoredMessage()).thenReturn(message);
        return enqueueableMessage;
    }

    private RocksDBCommitter replaceCommitter(final RocksDBMessageStore store,
                                              final RocksDBCommitter replacement) throws Exception
    {
        Field field = RocksDBMessageStore.class.getDeclaredField("_committer");
        field.setAccessible(true);
        RocksDBCommitter current = (RocksDBCommitter) field.get(store);
        if (current != null)
        {
            current.stop();
        }
        field.set(store, replacement);
        return current;
    }

    private byte[] encodeQueueSegmentKey(final UUID queueId, final long segmentNo)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Long.BYTES + Long.BYTES);
        buffer.putLong(queueId.getMostSignificantBits());
        buffer.putLong(queueId.getLeastSignificantBits());
        buffer.putLong(segmentNo);
        return buffer.array();
    }

    private byte[] encodeLong(final long value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    private static final byte[] MESSAGE_STORE_VERSION_KEY =
            "rocksdb_message_store_version".getBytes(StandardCharsets.US_ASCII);

    private byte[] findLastChunkKey(final RocksDB database,
                                    final ColumnFamilyHandle chunkHandle,
                                    final long messageId)
    {
        byte[] prefix = encodeMessageKey(messageId);
        byte[] lastKey = null;
        int lastIndex = -1;
        try (org.rocksdb.RocksIterator iterator = database.newIterator(chunkHandle))
        {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next())
            {
                byte[] key = iterator.key();
                if (!hasPrefix(key, prefix))
                {
                    break;
                }
                int index = decodeChunkIndex(key);
                if (index > lastIndex)
                {
                    lastIndex = index;
                    lastKey = Arrays.copyOf(key, key.length);
                }
            }
        }
        assertNotNull(lastKey, "Expected chunk keys for message " + messageId);
        return lastKey;
    }

    private int decodeChunkIndex(final byte[] key)
    {
        ByteBuffer buffer = ByteBuffer.wrap(key, Long.BYTES, Integer.BYTES);
        return buffer.getInt();
    }

    private byte[] encodeMessageKey(final long messageId)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(messageId);
        return buffer.array();
    }

    private boolean hasPrefix(final byte[] key, final byte[] prefix)
    {
        if (key.length < prefix.length)
        {
            return false;
        }
        for (int i = 0; i < prefix.length; i++)
        {
            if (key[i] != prefix[i])
            {
                return false;
            }
        }
        return true;
    }
}
