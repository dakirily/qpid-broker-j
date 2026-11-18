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
package org.apache.qpid.server.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;

public class MemoryMessageStoreTest extends MessageStoreTestCase
{
    @Override
    protected VirtualHost<?> createVirtualHost()
    {
        return mock(TestMemoryVirtualHost.class);
    }

    @Override
    protected MessageStore createMessageStore()
    {
        return new MemoryMessageStore();
    }

    @Override
    protected boolean flowToDiskSupported()
    {
        return false;
    }

    @Override
    protected void reopenStore()
    {
        // cannot re-open memory message store as it is not persistent
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFilteredVisitCanDequeueFullAndPartialBatches(final boolean queueScoped)
    {
        final TransactionLogResource queue = createQueue();
        final TransactionLogResource other = createQueue();
        enqueue(queue, 1, 1001);
        enqueue(other, 1, 1);

        final List<UUID> accepted = new ArrayList<>();
        final List<List<MessageEnqueueRecord>> batches = new ArrayList<>();
        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            visit(reader, queueScoped ? queue : null, record -> accepted.add(record.getQueueId()),
                  record -> other.getId().equals(record.getQueueId()), 1000, records ->
                  {
                      batches.add(records);
                      dequeue(records);
                  });
            assertEquals(queueScoped ? List.of() : List.of(other.getId()), accepted);
            assertEquals(List.of(1000, 1), batches.stream().map(List::size).toList());
            assertThrows(UnsupportedOperationException.class, () -> batches.get(0).clear());

            final List<UUID> remaining = new ArrayList<>();
            reader.visitMessageInstances(record -> remaining.add(record.getQueueId()));
            assertEquals(List.of(other.getId()), remaining);
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    void testGlobalFilteredVisitCanRemoveQueuesAndEnqueueWhileVisiting()
    {
        final TransactionLogResource first = createQueue();
        final TransactionLogResource second = createQueue();
        enqueue(first, 1, 1);
        enqueue(second, 1, 1);
        final List<UUID> removed = new ArrayList<>();
        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            reader.visitMessageInstances(record -> true, record -> false, 1, records ->
            {
                final UUID queueId = records.get(0).getQueueId();
                assertEquals(1L, records.get(0).getMessageNumber());
                removed.add(queueId);
                dequeue(records);
                if (removed.size() == 1)
                {
                    enqueue(first.getId().equals(queueId) ? second : first, 2, 1);
                }
            });
            assertEquals(2, removed.size());
            assertTrue(removed.containsAll(List.of(first.getId(), second.getId())));

            final List<Long> remaining = new ArrayList<>();
            reader.visitMessageInstances(record -> remaining.add(record.getMessageNumber()));
            assertEquals(List.of(2L), remaining);
        }
        finally
        {
            reader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFilteredVisitFlushesPendingRejectionsWhenValidHandlerStops(final boolean queueScoped)
    {
        final TransactionLogResource queue = createQueue();
        enqueue(queue, 1, 3);
        final List<MessageEnqueueRecord> seen = new ArrayList<>();
        final List<MessageEnqueueRecord> accepted = new ArrayList<>();
        final List<MessageEnqueueRecord> rejected = new ArrayList<>();
        final ExecutorService writer = Executors.newSingleThreadExecutor();
        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            visit(reader, queueScoped ? queue : null, record ->
            {
                accepted.add(record);
                final Future<?> committed = writer.submit(() -> dequeue(List.of(record)));
                assertDoesNotThrow(() -> committed.get(10, TimeUnit.SECONDS),
                                   "A callback must be able to wait for another thread to commit");
                return false;
            }, record ->
            {
                seen.add(record);
                return seen.size() > 1;
            }, 2, rejected::addAll);
            assertEquals(2, seen.size());
            assertEquals(List.of(seen.get(1)), accepted);
            assertEquals(List.of(seen.get(0)), rejected);
        }
        finally
        {
            writer.shutdownNow();
            reader.close();
        }
    }

    private static void visit(final MessageStore.MessageStoreReader reader, final TransactionLogResource queue,
                              final MessageInstanceHandler handler, final Predicate<MessageEnqueueRecord> predicate,
                              final int batchSize, final Consumer<List<MessageEnqueueRecord>> consumer)
    {
        if (queue == null)
        {
            reader.visitMessageInstances(handler, predicate, batchSize, consumer);
        }
        else
        {
            reader.visitMessageInstances(queue, handler, predicate, batchSize, consumer);
        }
    }

    private static TransactionLogResource createQueue()
    {
        final TransactionLogResource queue = mock(TransactionLogResource.class);
        when(queue.getId()).thenReturn(UUID.randomUUID());
        return queue;
    }

    private void enqueue(final TransactionLogResource queue, final long firstId, final int count)
    {
        final Transaction txn = getStore().newTransaction();
        for (int i = 0; i < count; i++)
        {
            final EnqueueableMessage<?> message = mock(EnqueueableMessage.class);
            when(message.getMessageNumber()).thenReturn(firstId + i);
            txn.enqueueMessage(queue, message);
        }
        txn.commitTran();
    }

    private void dequeue(final List<MessageEnqueueRecord> records)
    {
        final Transaction txn = getStore().newTransaction();
        records.forEach(txn::dequeueMessage);
        txn.commitTran();
    }
}
