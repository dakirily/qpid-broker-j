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

package org.apache.qpid.server.store.berkeleydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.server.store.StorableMessageMetaData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreTestCase;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.store.berkeleydb.tuple.QueueEntryBinding;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;

/**
 * Tests for {@link MessageStore.MessageStoreReader#visitMessageInstances(TransactionLogResource, MessageInstanceHandler)}
 * and related overloads for the BDB store.
 */
public class BDBMessageStoreReaderVisitMessageInstancesTest extends MessageStoreTestCase
{
    private String _storeLocation;

    @AfterEach
    @Override
    public void tearDown() throws Exception
    {
        try
        {
            final MessageStore store = getStore();
            if (store != null)
            {
                store.closeMessageStore();
            }
        }
        finally
        {
            try
            {
                super.tearDown();
            }
            finally
            {
                deleteStoreIfExists();
            }
        }
    }

    @Override
    protected VirtualHost<?> createVirtualHost()
    {
        _storeLocation = TMP_FOLDER + File.separator + getTestClassName() + "_" + getTestName();
        deleteStoreIfExists();

        final BDBVirtualHost parent = mock(BDBVirtualHost.class);
        when(parent.getStorePath()).thenReturn(_storeLocation);
        return parent;
    }

    @Override
    protected MessageStore createMessageStore()
    {
        return new BDBMessageStore();
    }

    @Override
    protected boolean flowToDiskSupported()
    {
        return true;
    }

    @Test
    public void visitMessageInstancesForQueueWithNoEntriesInvokesNoHandler()
    {
        final TransactionLogResource queueA = createQueue(UUID.randomUUID(), "queueA");

        final AtomicInteger invocationCount = new AtomicInteger();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queueA, record ->
            {
                invocationCount.incrementAndGet();
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        assertEquals(0, invocationCount.get(), "Handler must not be invoked for empty queue");
    }

    @Test
    public void visitMessageInstanceForQueueWhenOnlyOtherQueuesHaveEntriesInvokesNoHandler()
    {
        final TransactionLogResource queueA = createQueue(UUID.randomUUID(), "queueA");
        final TransactionLogResource queueB = createQueue(UUID.randomUUID(), "queueB");

        for (int i = 0; i < 3; i++)
        {
            enqueueNewMessage(queueB);
        }

        final AtomicInteger invocationCount = new AtomicInteger();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queueA, record ->
            {
                invocationCount.incrementAndGet();
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        assertEquals(0, invocationCount.get(), "Handler must not be invoked when queue has no entries");
    }

    @Test
    public void visitMessageInstancesForQueueReturnsOnlyThatQueueEntries()
    {
        final UUID queueAId = UUID.randomUUID();
        final UUID queueBId = UUID.randomUUID();
        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        final List<Long> queueAMessageIds = new ArrayList<>();
        final List<Long> queueBMessageIds = new ArrayList<>();

        queueAMessageIds.add(enqueueNewMessage(queueA));
        queueAMessageIds.add(enqueueNewMessage(queueA));

        queueBMessageIds.add(enqueueNewMessage(queueB));
        queueBMessageIds.add(enqueueNewMessage(queueB));
        queueBMessageIds.add(enqueueNewMessage(queueB));

        final Set<Long> seenQueueAIds = new HashSet<>();
        final AtomicBoolean sawNonQueueA = new AtomicBoolean(false);

        final MessageInstanceHandler handler = record ->
        {
            if (!queueAId.equals(record.getQueueId()))
            {
                sawNonQueueA.set(true);
            }
            else
            {
                seenQueueAIds.add(record.getMessageNumber());
            }
            return true;
        };

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queueA, handler);
        }
        finally
        {
            reader.close();
        }

        assertFalse(sawNonQueueA.get(), "Entries for other queues must not be visited");
        assertEquals(queueAMessageIds.size(), seenQueueAIds.size(), "Unexpected number of visited entries");
        assertTrue(seenQueueAIds.containsAll(queueAMessageIds), "Not all queueA entries were visited");
        assertTrue(Collections.disjoint(seenQueueAIds, queueBMessageIds),
                "queueA results must not include queueB message ids");
    }

    @Test
    public void visitMessageInstancesForQueueStopsAtQueueBoundary()
    {
        // Use adjacent queue ids in the key space to maximise the chance of detecting boundary issues.
        final UUID queueAId = new UUID(0x0102030405060708L, 0x0000000000000000L);
        final UUID queueBId = new UUID(0x0102030405060708L, 0x0000000000000001L);

        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        final List<Long> queueAMessageIds = new ArrayList<>();
        final List<Long> queueBMessageIds = new ArrayList<>();

        queueAMessageIds.add(enqueueNewMessage(queueA));
        queueAMessageIds.add(enqueueNewMessage(queueA));
        queueAMessageIds.add(enqueueNewMessage(queueA));

        queueBMessageIds.add(enqueueNewMessage(queueB));
        queueBMessageIds.add(enqueueNewMessage(queueB));

        final List<MessageEnqueueRecord> visited = new ArrayList<>();

        final MessageInstanceHandler handler = record ->
        {
            visited.add(record);
            return true;
        };

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queueA, handler);
        }
        finally
        {
            reader.close();
        }

        assertEquals(queueAMessageIds.size(), visited.size(), "Unexpected number of visited entries");
        for (MessageEnqueueRecord record : visited)
        {
            assertEquals(queueAId, record.getQueueId(), "Queue boundary was not respected");
        }

        final Set<Long> visitedIds = new HashSet<>();
        visited.forEach(r -> visitedIds.add(r.getMessageNumber()));
        assertTrue(visitedIds.containsAll(queueAMessageIds), "Not all queueA entries were visited");
        assertTrue(Collections.disjoint(visitedIds, queueBMessageIds), "queueB entries leaked past the boundary");
    }

    @Test
    public void visitMessageInstancesForQueueRespectsHandlerStop()
    {
        final UUID queueAId = UUID.randomUUID();
        final UUID queueBId = UUID.randomUUID();
        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        final List<Long> queueAMessageIds = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            queueAMessageIds.add(enqueueNewMessage(queueA));
        }
        for (int i = 0; i < 5; i++)
        {
            enqueueNewMessage(queueB);
        }

        final int stopAfter = 3;
        final AtomicInteger invocationCount = new AtomicInteger();
        final AtomicBoolean stopReturned = new AtomicBoolean(false);
        final List<MessageEnqueueRecord> visited = new ArrayList<>();

        final MessageInstanceHandler handler = record ->
        {
            if (stopReturned.get())
            {
                // If the reader invokes the handler after it returned false, the stop contract is broken.
                throw new AssertionError("Handler was invoked after returning false");
            }

            visited.add(record);
            final int count = invocationCount.incrementAndGet();
            final boolean shouldContinue = count < stopAfter;
            if (!shouldContinue)
            {
                stopReturned.set(true);
            }
            return shouldContinue;
        };

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queueA, handler);
        }
        finally
        {
            reader.close();
        }

        assertEquals(stopAfter, invocationCount.get(), "Reader did not stop when handler returned false");
        assertEquals(stopAfter, visited.size(), "Unexpected number of visited entries");
        for (MessageEnqueueRecord record : visited)
        {
            assertEquals(queueAId, record.getQueueId(), "Unexpected queue id in visited entries");
            assertTrue(queueAMessageIds.contains(record.getMessageNumber()), "Unexpected message id in visited entries");
        }
    }

    @Test
    public void visitMessageInstancesForQueueWithFilterRoutesValidAndInvalidRecords()
    {
        final UUID queueAId = UUID.randomUUID();
        final UUID queueBId = UUID.randomUUID();
        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        final List<Long> queueAMessageIds = new ArrayList<>();
        final List<Long> queueBMessageIds = new ArrayList<>();

        for (int i = 0; i < 5; i++)
        {
            queueAMessageIds.add(enqueueNewMessage(queueA));
        }
        for (int i = 0; i < 3; i++)
        {
            queueBMessageIds.add(enqueueNewMessage(queueB));
        }

        // Treat the first two messages on queueA as valid and the rest as invalid.
        final Set<Long> validIds = new HashSet<>(queueAMessageIds.subList(0, 2));

        final List<Long> visitedValid = new ArrayList<>();
        final List<Long> visitedInvalid = new ArrayList<>();
        final AtomicBoolean sawNonQueueA = new AtomicBoolean(false);

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queueA,
                    record ->
                    {
                        if (!queueAId.equals(record.getQueueId()))
                        {
                            sawNonQueueA.set(true);
                        }
                        visitedValid.add(record.getMessageNumber());
                        return true;
                    },
                    record -> validIds.contains(record.getMessageNumber()),
                    record ->
                    {
                        if (!queueAId.equals(record.getQueueId()))
                        {
                            sawNonQueueA.set(true);
                        }
                        visitedInvalid.add(record.getMessageNumber());
                    });
        }
        finally
        {
            reader.close();
        }

        assertFalse(sawNonQueueA.get(), "Entries for other queues must not be visited");

        final Set<Long> validSet = new HashSet<>(visitedValid);
        final Set<Long> invalidSet = new HashSet<>(visitedInvalid);

        assertEquals(validIds, validSet, "Unexpected valid message ids");

        final Set<Long> expectedInvalid = new HashSet<>(queueAMessageIds);
        expectedInvalid.removeAll(validIds);
        assertEquals(expectedInvalid, invalidSet, "Unexpected invalid message ids");

        assertTrue(Collections.disjoint(validSet, queueBMessageIds), "queueB message ids must not be visited as valid");
        assertTrue(Collections.disjoint(invalidSet, queueBMessageIds), "queueB message ids must not be visited as invalid");
    }

    @Test
    public void visitMessageInstancesForQueueWithFilterStopsWhenValidHandlerReturnsFalse()
    {
        // Use adjacent queue ids in the key space to maximise the chance of detecting boundary issues.
        final UUID queueAId = new UUID(0x0102030405060708L, 0x0000000000000000L);
        final UUID queueBId = new UUID(0x0102030405060708L, 0x0000000000000001L);
        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        for (int i = 0; i < 5; i++)
        {
            enqueueNewMessage(queueA);
        }
        for (int i = 0; i < 5; i++)
        {
            enqueueNewMessage(queueB);
        }

        final int stopAfter = 3;
        final AtomicInteger invocationCount = new AtomicInteger();
        final AtomicBoolean stopReturned = new AtomicBoolean(false);
        final AtomicInteger invalidCount = new AtomicInteger();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queueA,
                    record ->
                    {
                        if (stopReturned.get())
                        {
                            throw new AssertionError("Handler was invoked after returning false");
                        }

                        assertEquals(queueAId, record.getQueueId(), "Unexpected queue id");
                        final int count = invocationCount.incrementAndGet();
                        final boolean shouldContinue = count < stopAfter;
                        if (!shouldContinue)
                        {
                            stopReturned.set(true);
                        }
                        return shouldContinue;
                    },
                    record -> true,
                    record -> invalidCount.incrementAndGet());
        }
        finally
        {
            reader.close();
        }

        assertEquals(stopAfter, invocationCount.get(), "Reader did not stop when handler returned false");
        assertEquals(0, invalidCount.get(), "Invalid collector must not have been invoked");
    }

    @Test
    public void visitMessageInstancesForQueueWithFilterRespectsQueueBoundaryAdjacentQueueIds()
    {
        // Use adjacent queue ids in the key space to maximise the chance of detecting boundary issues.
        final UUID queueAId = new UUID(0x0102030405060708L, 0x0000000000000000L);
        final UUID queueBId = new UUID(0x0102030405060708L, 0x0000000000000001L);
        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        final List<Long> queueAMessageIds = new ArrayList<>();
        final List<Long> queueBMessageIds = new ArrayList<>();
        for (int i = 0; i < 4; i++)
        {
            queueAMessageIds.add(enqueueNewMessage(queueA));
        }
        for (int i = 0; i < 3; i++)
        {
            queueBMessageIds.add(enqueueNewMessage(queueB));
        }

        final AtomicInteger validCount = new AtomicInteger();
        final List<MessageEnqueueRecord> visitedInvalid = new ArrayList<>();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queueA,
                    record ->
                    {
                        validCount.incrementAndGet();
                        return true;
                    },
                    record -> false,
                    record -> visitedInvalid.add(record));
        }
        finally
        {
            reader.close();
        }

        assertEquals(0, validCount.get(), "Valid handler must not be invoked");
        assertEquals(queueAMessageIds.size(), visitedInvalid.size(), "Unexpected number of visited invalid entries");

        final Set<Long> visitedInvalidIds = new HashSet<>();
        for (MessageEnqueueRecord record : visitedInvalid)
        {
            assertEquals(queueAId, record.getQueueId(), "Queue boundary was not respected");
            visitedInvalidIds.add(record.getMessageNumber());
        }

        assertTrue(visitedInvalidIds.containsAll(queueAMessageIds), "Not all queueA entries were visited");
        assertTrue(Collections.disjoint(visitedInvalidIds, queueBMessageIds), "queueB entries leaked past the boundary");
    }

    @Test
    public void visitMessageInstancesWithFilterStopsWhenValidHandlerReturnsFalse()
    {
        final UUID queueAId = new UUID(0x0102030405060708L, 0x0000000000000000L);
        final UUID queueBId = new UUID(0x0102030405060708L, 0x0000000000000001L);
        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        for (int i = 0; i < 5; i++)
        {
            enqueueNewMessage(queueA);
        }
        for (int i = 0; i < 5; i++)
        {
            enqueueNewMessage(queueB);
        }

        final int stopAfter = 4;
        final AtomicInteger invocationCount = new AtomicInteger();
        final AtomicBoolean stopReturned = new AtomicBoolean(false);
        final AtomicInteger invalidCount = new AtomicInteger();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(record ->
                    {
                        if (stopReturned.get())
                        {
                            throw new AssertionError("Handler was invoked after returning false");
                        }

                        final int count = invocationCount.incrementAndGet();
                        final boolean shouldContinue = count < stopAfter;
                        if (!shouldContinue)
                        {
                            stopReturned.set(true);
                        }
                        return shouldContinue;
                    },
                    record -> true,
                    record -> invalidCount.incrementAndGet());
        }
        finally
        {
            reader.close();
        }

        assertEquals(stopAfter, invocationCount.get(), "Reader did not stop when handler returned false");
        assertEquals(0, invalidCount.get(), "Invalid collector must not have been invoked");
    }

    @Test
    public void visitMessageInstancesWithFilterRoutesValidAndInvalidAcrossQueues()
    {
        final UUID queueAId = UUID.randomUUID();
        final UUID queueBId = UUID.randomUUID();
        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        final Map<Long, UUID> expectedQueueByMessageId = new HashMap<>();
        final List<Long> allMessageIds = new ArrayList<>();

        for (int i = 0; i < 4; i++)
        {
            final long messageId = enqueueNewMessage(queueA);
            expectedQueueByMessageId.put(messageId, queueAId);
            allMessageIds.add(messageId);
        }
        for (int i = 0; i < 3; i++)
        {
            final long messageId = enqueueNewMessage(queueB);
            expectedQueueByMessageId.put(messageId, queueBId);
            allMessageIds.add(messageId);
        }

        // Mark a subset of message ids as valid.
        final Set<Long> validIds = new HashSet<>();
        validIds.add(allMessageIds.get(0));
        validIds.add(allMessageIds.get(allMessageIds.size() - 1));

        final List<MessageEnqueueRecord> visitedValid = new ArrayList<>();
        final List<MessageEnqueueRecord> visitedInvalid = new ArrayList<>();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(record ->
                    {
                        visitedValid.add(record);
                        return true;
                    },
                    record -> validIds.contains(record.getMessageNumber()),
                    record -> visitedInvalid.add(record));
        }
        finally
        {
            reader.close();
        }

        final Set<Long> visitedValidIds = new HashSet<>();
        visitedValid.forEach(r -> visitedValidIds.add(r.getMessageNumber()));
        final Set<Long> visitedInvalidIds = new HashSet<>();
        visitedInvalid.forEach(r -> visitedInvalidIds.add(r.getMessageNumber()));

        final Set<Long> union = new HashSet<>(visitedValidIds);
        union.addAll(visitedInvalidIds);
        assertEquals(new HashSet<>(allMessageIds), union, "Unexpected set of visited message ids");
        assertTrue(Collections.disjoint(visitedValidIds, visitedInvalidIds), "Valid and invalid sets must be disjoint");
        assertEquals(validIds, visitedValidIds, "Unexpected valid message ids");

        for (MessageEnqueueRecord record : visitedValid)
        {
            assertEquals(expectedQueueByMessageId.get(record.getMessageNumber()), record.getQueueId(),
                    "Unexpected queue id for valid record");
        }
        for (MessageEnqueueRecord record : visitedInvalid)
        {
            assertEquals(expectedQueueByMessageId.get(record.getMessageNumber()), record.getQueueId(),
                    "Unexpected queue id for invalid record");
        }
    }

    @Test
    public void visitMessageInstancesWithFilterWhenNoEntriesInvokesNoHandlers()
    {
        final AtomicInteger validCount = new AtomicInteger();
        final AtomicInteger invalidCount = new AtomicInteger();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(record ->
                    {
                        validCount.incrementAndGet();
                        return true;
                    },
                    record -> true,
                    record -> invalidCount.incrementAndGet());
        }
        finally
        {
            reader.close();
        }

        assertEquals(0, validCount.get(), "Valid handler must not be invoked when store has no entries");
        assertEquals(0, invalidCount.get(), "Invalid collector must not be invoked when store has no entries");
    }

    @Test
    public void visitMessageInstancesWithFilterWrapsExceptionFromPredicate()
    {
        final TransactionLogResource queueA = createQueue(UUID.randomUUID(), "queueA");
        enqueueNewMessage(queueA);

        final RuntimeException failure = new RuntimeException("boom");

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final StoreException exception = assertThrows(StoreException.class, () ->
                    reader.visitMessageInstances(record -> true,
                            record ->
                            {
                                throw failure;
                            },
                            record -> { }));

            assertTrue(exception.getMessage().contains("Cannot visit message instances"),
                    "Unexpected exception message");
            assertEquals(failure, exception.getCause(), "Unexpected cause");
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void visitMessageInstancesWithFilterWrapsExceptionFromValidHandler()
    {
        final TransactionLogResource queueA = createQueue(UUID.randomUUID(), "queueA");
        enqueueNewMessage(queueA);

        final RuntimeException failure = new RuntimeException("boom");

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final StoreException exception = assertThrows(StoreException.class, () ->
                    reader.visitMessageInstances(record ->
                            {
                                throw failure;
                            },
                            record -> true,
                            record -> { }));

            assertTrue(exception.getMessage().contains("Cannot visit message instances"),
                    "Unexpected exception message");
            assertEquals(failure, exception.getCause(), "Unexpected cause");
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void visitMessageInstancesWithFilterWrapsExceptionFromInvalidCollector()
    {
        final TransactionLogResource queueA = createQueue(UUID.randomUUID(), "queueA");
        enqueueNewMessage(queueA);

        final RuntimeException failure = new RuntimeException("boom");

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final StoreException exception = assertThrows(StoreException.class, () ->
                    reader.visitMessageInstances(record -> true,
                            record -> false,
                            record ->
                            {
                                throw failure;
                            }));

            assertTrue(exception.getMessage().contains("Cannot visit message instances"),
                    "Unexpected exception message");
            assertEquals(failure, exception.getCause(), "Unexpected cause");
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void visitMessageInstancesWithFilterThrowsStoreExceptionOnCorruptedKey()
    {
        final byte[] invalidKey = new byte[] { 0x00 };
        insertDeliveryDbRecord(new DatabaseEntry(invalidKey));

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final StoreException exception = assertThrows(StoreException.class, () ->
                    reader.visitMessageInstances(record -> true,
                            record -> true,
                            record -> { }));

            assertTrue(exception.getMessage().contains("Cannot visit message instances"),
                    "Unexpected exception message");
            assertTrue(exception.getCause() instanceof IllegalArgumentException, "Unexpected cause type");
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void visitMessageInstancesForQueueReturnsRecordsInAscendingMessageIdOrder()
    {
        final UUID queueAId = UUID.randomUUID();
        final TransactionLogResource queueA = createQueue(queueAId, "queueA");

        // Create out-of-order message ids directly in the delivery database to verify key ordering.
        insertQueueEntry(queueAId, 50L);
        insertQueueEntry(queueAId, 10L);
        insertQueueEntry(queueAId, 1L);
        insertQueueEntry(queueAId, 30L);

        final List<Long> visited = new ArrayList<>();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(queueA, record ->
            {
                visited.add(record.getMessageNumber());
                return true;
            });
        }
        finally
        {
            reader.close();
        }

        final List<Long> expected = new ArrayList<>(visited);
        Collections.sort(expected);
        assertEquals(expected, visited, "Message ids must be visited in ascending order");
    }

    private TransactionLogResource createQueue(final UUID id, final String name)
    {
        final TransactionLogResource queue = mock(TransactionLogResource.class);
        when(queue.getId()).thenReturn(id);
        when(queue.getName()).thenReturn(name);
        when(queue.getMessageDurability()).thenReturn(MessageDurability.DEFAULT);
        return queue;
    }

    private long enqueueNewMessage(final TransactionLogResource queue)
    {
        final StoredMessage<StorableMessageMetaData> storedMessage =
                getStore().addMessage((StorableMessageMetaData) new TestMessageMetaData(0L, 0)).allContentAdded();

        final Transaction txn = getStore().newTransaction();
        txn.enqueueMessage(queue, new EnqueueableMessage<>()
        {
            @Override
            public long getMessageNumber()
            {
                return storedMessage.getMessageNumber();
            }

            @Override
            public boolean isPersistent()
            {
                return true;
            }

            @Override
            public StoredMessage<StorableMessageMetaData> getStoredMessage()
            {
                return storedMessage;
            }
        });
        txn.commitTran();

        return storedMessage.getMessageNumber();
    }

    private void insertQueueEntry(final UUID queueId, final long messageId)
    {
        final DatabaseEntry key = new DatabaseEntry();
        QueueEntryBinding.objectToEntry(queueId, messageId, key);
        insertDeliveryDbRecord(key);
    }

    private void insertDeliveryDbRecord(final DatabaseEntry key)
    {
        final BDBMessageStore store = (BDBMessageStore) getStore();
        final EnvironmentFacade environmentFacade = store.getEnvironmentFacade();
        final Database deliveryDb = environmentFacade.openDatabase("QUEUE_ENTRIES", BDBUtils.DEFAULT_DATABASE_CONFIG);

        com.sleepycat.je.Transaction tx = null;
        try
        {
            tx = environmentFacade.beginTransaction(null);
            final DatabaseEntry value = new DatabaseEntry();
            value.setData(new byte[0]);
            deliveryDb.put(tx, key, value);
            environmentFacade.commit(tx);
        }
        catch (RuntimeException e)
        {
            BDBUtils.abortTransactionSafely(tx, environmentFacade);
            throw e;
        }
    }

    private void deleteStoreIfExists()
    {
        if (_storeLocation != null)
        {
            final File location = new File(_storeLocation);
            if (location.exists())
            {
                FileUtils.delete(location, true);
            }
        }
    }
}
