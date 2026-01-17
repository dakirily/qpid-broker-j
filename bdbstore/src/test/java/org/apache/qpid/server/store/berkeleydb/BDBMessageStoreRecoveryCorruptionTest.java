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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.berkeleydb.tuple.QueueEntryBinding;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

/**
 * Integration-style tests for corruption scenarios. The tests intentionally inject
 * semantically invalid delivery DB records (but still decodable) and verify that
 * recovery-style scans do not fail and route such records to the invalid collector.
 */
public class BDBMessageStoreRecoveryCorruptionTest extends org.apache.qpid.test.utils.UnitTestBase
{
    private static final int BUFFER_SIZE = 10;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 1.0;

    private String _storeLocation;
    private BDBMessageStore _store;
    private MessageStore.MessageStoreReader _storeReader;
    private VirtualHost<?> _virtualHost;

    @BeforeEach
    public void setUp()
    {
        assumeTrue(Objects.equals(getVirtualHostNodeStoreType(), VirtualHostNodeStoreType.BDB),
                "VirtualHostNodeStoreType should be BDB");

        _storeLocation = TMP_FOLDER + File.separator + getTestClassName() + File.separator + getTestName();
        deleteStoreIfExists();

        final BDBVirtualHost parent = mock(BDBVirtualHost.class);
        when(parent.getStorePath()).thenReturn(_storeLocation);
        _virtualHost = parent;

        _store = new BDBMessageStore();
        _store.openMessageStore(_virtualHost);
        _storeReader = _store.newMessageStoreReader();

        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE, SPARSITY_FRACTION);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        try
        {
            if (_storeReader != null)
            {
                _storeReader.close();
            }
        }
        finally
        {
            try
            {
                if (_store != null)
                {
                    _store.closeMessageStore();
                }
            }
            finally
            {
                try
                {
                    QpidByteBuffer.deinitialisePool();
                }
                finally
                {
                    deleteStoreIfExists();
                }
            }
        }
    }

    @Test
    public void persistCorruptRestartRecover_routesInvalidInstancesToCollectorWithoutFailing()
    {
        final UUID queueAId = new UUID(0L, 0L);
        final UUID queueBId = new UUID(0L, 1L);
        final UUID unknownQueueId = new UUID(0L, 2L);

        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        // Persist valid instances.
        final Map<Long, UUID> expectedQueueByMessageId = new HashMap<>();
        final Set<Long> expectedMessageIds = new HashSet<>();

        final long a1 = enqueueNewMessage(queueA);
        expectedQueueByMessageId.put(a1, queueAId);
        expectedMessageIds.add(a1);

        final long b1 = enqueueNewMessage(queueB);
        expectedQueueByMessageId.put(b1, queueBId);
        expectedMessageIds.add(b1);

        final long a2 = enqueueNewMessage(queueA);
        expectedQueueByMessageId.put(a2, queueAId);
        expectedMessageIds.add(a2);

        final long b2 = enqueueNewMessage(queueB);
        expectedQueueByMessageId.put(b2, queueBId);
        expectedMessageIds.add(b2);

        final long a3 = enqueueNewMessage(queueA);
        expectedQueueByMessageId.put(a3, queueAId);
        expectedMessageIds.add(a3);

        // Restart once to simulate a real persistence boundary.
        restartStore();

        // Inject semantically invalid but decodable delivery DB entries.
        // - Unknown queue id (will be filtered as invalid).
        // - Dangling message id for a known queue (message id is not part of the expected set).
        final long firstMessageId = Math.min(Math.min(a1, a2), Math.min(a3, Math.min(b1, b2)));
        final long danglingMessageId = firstMessageId - 1;

        insertQueueEntry(queueAId, danglingMessageId);
        insertQueueEntry(unknownQueueId, a1);

        // Restart again to ensure injected records are fully visible after reopen.
        restartStore();

        final Set<UUID> knownQueues = new HashSet<>();
        knownQueues.add(queueAId);
        knownQueues.add(queueBId);

        final List<MessageEnqueueRecord> visitedValid = new ArrayList<>();
        final List<MessageEnqueueRecord> visitedInvalid = new ArrayList<>();

        _storeReader.visitMessageInstances(record ->
                {
                    visitedValid.add(record);
                    return true;
                },
                record -> knownQueues.contains(record.getQueueId())
                        && expectedMessageIds.contains(record.getMessageNumber()),
                visitedInvalid::add);

        // Valid: exactly the originally enqueued instances.
        assertEquals(expectedMessageIds.size(), visitedValid.size(), "Unexpected number of valid instances");
        final Set<Long> visitedValidIds = new HashSet<>();
        for (MessageEnqueueRecord record : visitedValid)
        {
            visitedValidIds.add(record.getMessageNumber());
            assertEquals(expectedQueueByMessageId.get(record.getMessageNumber()), record.getQueueId(),
                    "Unexpected queue id for valid record");
        }
        assertEquals(expectedMessageIds, visitedValidIds, "Unexpected set of recovered valid message ids");

        // Invalid: exactly the injected invalid records.
        assertEquals(2, visitedInvalid.size(), "Unexpected number of invalid instances");

        boolean sawDangling = false;
        boolean sawUnknownQueue = false;
        for (MessageEnqueueRecord record : visitedInvalid)
        {
            if (queueAId.equals(record.getQueueId()) && danglingMessageId == record.getMessageNumber())
            {
                sawDangling = true;
            }
            if (unknownQueueId.equals(record.getQueueId()) && a1 == record.getMessageNumber())
            {
                sawUnknownQueue = true;
            }
        }

        assertTrue(sawDangling, "Dangling message instance was not reported to invalid collector");
        assertTrue(sawUnknownQueue, "Unknown queue instance was not reported to invalid collector");

        // Sanity: ensure scan continued past invalid records by observing that all expected valid instances were visited.
        assertEquals(expectedMessageIds.size(), visitedValidIds.size(), "Valid instance scan did not complete");
    }

    @Test
    public void persistCorruptRestartRecover_invalidRecordsDoNotAffectPerQueueOrder()
    {
        final UUID queueAId = new UUID(0x0102030405060708L, 0x0000000000000000L);
        final UUID queueBId = new UUID(0x0102030405060708L, 0x0000000000000001L);
        final UUID unknownQueueId = new UUID(0x0102030405060708L, 0x0000000000000002L);

        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        final List<Long> expectedA = new ArrayList<>();
        final List<Long> expectedB = new ArrayList<>();

        expectedA.add(enqueueNewMessage(queueA));
        expectedB.add(enqueueNewMessage(queueB));
        expectedA.add(enqueueNewMessage(queueA));
        expectedB.add(enqueueNewMessage(queueB));
        expectedA.add(enqueueNewMessage(queueA));

        restartStore();

        // Insert invalid record that falls between valid keys for queueA.
        // Using messageId = expectedA.get(1) - 1 ensures it is visited before some valid instances in queueA key space.
        final long invalidBetweenA = expectedA.get(1) - 1;
        insertQueueEntry(queueAId, invalidBetweenA);

        // Insert unknown queue record to ensure global scan sees foreign queue ids.
        insertQueueEntry(unknownQueueId, expectedB.get(0));

        restartStore();

        final Set<Long> expectedASet = new HashSet<>(expectedA);
        final Set<Long> expectedBSet = new HashSet<>(expectedB);

        final List<Long> recoveredA = new ArrayList<>();
        final List<Long> recoveredB = new ArrayList<>();

        // Only accept known queues and known message ids.
        _storeReader.visitMessageInstances(record ->
                {
                    if (queueAId.equals(record.getQueueId()))
                    {
                        recoveredA.add(record.getMessageNumber());
                    }
                    else if (queueBId.equals(record.getQueueId()))
                    {
                        recoveredB.add(record.getMessageNumber());
                    }
                    return true;
                },
                record -> (queueAId.equals(record.getQueueId()) && expectedASet.contains(record.getMessageNumber()))
                        || (queueBId.equals(record.getQueueId()) && expectedBSet.contains(record.getMessageNumber())),
                record -> { });

        assertEquals(expectedA, recoveredA, "Unexpected queueA recovered order");
        assertEquals(expectedB, recoveredB, "Unexpected queueB recovered order");
    }

    private void restartStore()
    {
        if (_storeReader != null)
        {
            _storeReader.close();
            _storeReader = null;
        }
        if (_store != null)
        {
            _store.closeMessageStore();
            _store = null;
        }

        _store = new BDBMessageStore();
        _store.openMessageStore(_virtualHost);
        _storeReader = _store.newMessageStoreReader();
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
                _store.addMessage((StorableMessageMetaData) new TestMessageMetaData(0L, 0)).allContentAdded();

        final Transaction txn = _store.newTransaction();
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
        final EnvironmentFacade environmentFacade = _store.getEnvironmentFacade();
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
