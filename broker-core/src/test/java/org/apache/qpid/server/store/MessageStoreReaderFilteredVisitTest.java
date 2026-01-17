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

package org.apache.qpid.server.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;

public class MessageStoreReaderFilteredVisitTest
{
    @Test
    public void filteredVisitDelegatesToLegacyVisit_andCollectsInvalidRecords()
            throws StoreException
    {
        final UUID queueId = UUID.randomUUID();

        final List<MessageEnqueueRecord> input = List.of(
                new TestRecord(queueId, 1L),
                new TestRecord(queueId, 2L),
                new TestRecord(queueId, 3L));

        final TestReader reader = new TestReader(input);

        final List<Long> valid = new ArrayList<>();
        final List<Long> invalid = new ArrayList<>();

        reader.visitMessageInstances(record ->
        {
            valid.add(record.getMessageNumber());
            return true;
        },
        record -> record.getMessageNumber() % 2 == 0,
        record -> invalid.add(record.getMessageNumber()));

        assertEquals(List.of(2L), valid, "Unexpected valid records");
        assertEquals(List.of(1L, 3L), invalid, "Unexpected invalid records");
    }

    @Test
    public void filteredVisitHonoursStopContract_forValidHandler()
            throws StoreException
    {
        final UUID queueId = UUID.randomUUID();

        final List<MessageEnqueueRecord> input = List.of(
                new TestRecord(queueId, 1L),
                new TestRecord(queueId, 2L),
                new TestRecord(queueId, 3L),
                new TestRecord(queueId, 4L));

        final TestReader reader = new TestReader(input);

        final AtomicInteger invoked = new AtomicInteger();
        final List<Long> visited = new ArrayList<>();

        reader.visitMessageInstances(record ->
        {
            visited.add(record.getMessageNumber());
            return invoked.incrementAndGet() < 2;
        },
        record -> true,
        record ->
        {
            throw new AssertionError("Invalid collector must not be invoked when predicate always returns true");
        });

        assertEquals(2, invoked.get(), "Expected stop after two invocations");
        assertEquals(List.of(1L, 2L), visited, "Unexpected visited sequence");
    }

    @Test
    public void filteredVisitForQueueDelegatesToLegacyQueueVisit()
            throws StoreException
    {
        final UUID queueIdA = new UUID(0x0102030405060708L, 0x0000000000000000L);
        final UUID queueIdB = new UUID(0x0102030405060708L, 0x0000000000000001L);

        final TestQueue queueA = new TestQueue(queueIdA, "queueA");

        final List<MessageEnqueueRecord> input = List.of(
                new TestRecord(queueIdA, 1L),
                new TestRecord(queueIdA, 2L),
                new TestRecord(queueIdB, 100L));

        final TestReader reader = new TestReader(input);

        final List<Long> valid = new ArrayList<>();
        final List<Long> invalid = new ArrayList<>();

        reader.visitMessageInstances(queueA,
                                    record ->
                                    {
                                        valid.add(record.getMessageNumber());
                                        return true;
                                    },
                                    record -> record.getMessageNumber() == 2L,
                                    record -> invalid.add(record.getMessageNumber()));

        assertEquals(List.of(2L), valid, "Unexpected valid records");
        assertEquals(List.of(1L), invalid, "Unexpected invalid records");

        // Ensure queue-specific overload does not leak other queue entries.
        assertTrue(valid.stream().noneMatch(id -> id == 100L));
        assertTrue(invalid.stream().noneMatch(id -> id == 100L));
    }

    private static final class TestReader implements MessageStore.MessageStoreReader
    {
        private final List<MessageEnqueueRecord> _instances;

        private TestReader(final List<MessageEnqueueRecord> instances)
        {
            _instances = instances;
        }

        @Override
        public void visitMessages(final MessageHandler handler)
        {
            // Not required for these tests
        }

        @Override
        public void visitMessageInstances(final MessageInstanceHandler handler)
        {
            for (final MessageEnqueueRecord record : _instances)
            {
                if (!handler.handle(record))
                {
                    break;
                }
            }
        }

        @Override
        public void visitMessageInstances(final TransactionLogResource queue, final MessageInstanceHandler handler)
        {
            for (final MessageEnqueueRecord record : _instances)
            {
                if (queue.getId().equals(record.getQueueId()))
                {
                    if (!handler.handle(record))
                    {
                        break;
                    }
                }
            }
        }

        @Override
        public void visitDistributedTransactions(final DistributedTransactionHandler handler)
        {
            // Not required for these tests
        }

        @Override
        public StoredMessage<?> getMessage(final long messageId)
        {
            return null;
        }

        @Override
        public void close()
        {
        }
    }

    private static final class TestRecord implements MessageEnqueueRecord
    {
        private final UUID _queueId;
        private final long _messageId;

        private TestRecord(final UUID queueId, final long messageId)
        {
            _queueId = queueId;
            _messageId = messageId;
        }

        @Override
        public UUID getQueueId()
        {
            return _queueId;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageId;
        }
    }

    private static final class TestQueue implements TransactionLogResource
    {
        private final UUID _id;
        private final String _name;

        private TestQueue(final UUID id, final String name)
        {
            _id = id;
            _name = name;
        }

        @Override
        public String getName()
        {
            return _name;
        }

        @Override
        public UUID getId()
        {
            return _id;
        }

        @Override
        public MessageDurability getMessageDurability()
        {
            return MessageDurability.DEFAULT;
        }
    }
}
