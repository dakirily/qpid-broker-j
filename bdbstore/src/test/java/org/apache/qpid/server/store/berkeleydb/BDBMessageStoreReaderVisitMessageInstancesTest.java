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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;

/**
 * Tests for {@link MessageStore.MessageStoreReader#visitMessageInstances(TransactionLogResource, MessageInstanceHandler)}
 * for the BDB store.
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
        _storeLocation = TMP_FOLDER + File.separator + getTestName();
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
    public void visitMessageInstances_forQueue_returnsOnlyThatQueueEntries()
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
    public void visitMessageInstances_forQueue_stopsAtQueueBoundary()
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
    public void visitMessageInstances_forQueue_respectsHandlerStop()
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
    public void visitMessageInstances_forQueue_withFilter_routesValidAndInvalidRecords()
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
            reader.visitMessageInstances(queueA, record ->
            {
                if (!queueAId.equals(record.getQueueId()))
                {
                    sawNonQueueA.set(true);
                }
                visitedValid.add(record.getMessageNumber());
                return true;
            },record -> validIds.contains(record.getMessageNumber()),record ->
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
    public void visitMessageInstances_withFilter_stopsWhenValidHandlerReturnsFalse()
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
