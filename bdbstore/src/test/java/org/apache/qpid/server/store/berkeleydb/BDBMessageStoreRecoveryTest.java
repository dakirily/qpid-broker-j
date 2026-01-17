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
import java.util.List;
import java.util.Objects;
import java.util.UUID;

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
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

/**
 * Integration-style tests for the BDB message store that exercise persistence
 * across close/open and validate instance ordering during recovery.
 */
public class BDBMessageStoreRecoveryTest extends org.apache.qpid.test.utils.UnitTestBase
{
    private static final int BUFFER_SIZE = 10;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 1.0;

    private String _storeLocation;
    private BDBMessageStore _store;
    private MessageStore.MessageStoreReader _storeReader;
    private VirtualHost<?> _virtualHost;

    @BeforeEach
    public void setUp() throws Exception
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
    public void persistRestartRecover_preservesInstanceOrderPerQueue() throws Exception
    {
        final UUID queueAId = new UUID(0L, 0L);
        final UUID queueBId = new UUID(0L, 1L);

        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        // Interleave enqueue operations across queues to ensure ordering is derived from the key and not from insertion.
        final long a1 = enqueueNewMessage(queueA).getMessageNumber();
        final long b1 = enqueueNewMessage(queueB).getMessageNumber();
        final long a2 = enqueueNewMessage(queueA).getMessageNumber();
        final long b2 = enqueueNewMessage(queueB).getMessageNumber();
        final long a3 = enqueueNewMessage(queueA).getMessageNumber();

        restartStore();

        assertEquals(List.of(a1, a2, a3), visitQueueMessageIds(queueA), "Unexpected recovered instances for queueA");
        assertEquals(List.of(b1, b2), visitQueueMessageIds(queueB), "Unexpected recovered instances for queueB");

        // Sanity: ensure recovered order is ascending.
        final List<Long> recoveredA = visitQueueMessageIds(queueA);
        assertTrue(isStrictlyAscending(recoveredA), "Recovered queueA instance order is not strictly ascending");
    }

    @Test
    public void persistRestartRecover_afterDequeue_excludesRemovedInstances() throws Exception
    {
        final UUID queueAId = new UUID(0x0102030405060708L, 0x0000000000000000L);
        final UUID queueBId = new UUID(0x0102030405060708L, 0x0000000000000001L);

        final TransactionLogResource queueA = createQueue(queueAId, "queueA");
        final TransactionLogResource queueB = createQueue(queueBId, "queueB");

        final MessageEnqueueRecord a1 = enqueueNewMessage(queueA);
        final MessageEnqueueRecord a2 = enqueueNewMessage(queueA);
        final MessageEnqueueRecord a3 = enqueueNewMessage(queueA);

        final MessageEnqueueRecord b1 = enqueueNewMessage(queueB);
        final MessageEnqueueRecord b2 = enqueueNewMessage(queueB);

        // Remove one record from the middle of queueA and one from queueB.
        dequeueMessage(a2);
        dequeueMessage(b1);

        restartStore();

        assertEquals(List.of(a1.getMessageNumber(), a3.getMessageNumber()),
                visitQueueMessageIds(queueA),
                "Recovered queueA instances must not include dequeued records");

        assertEquals(List.of(b2.getMessageNumber()),
                visitQueueMessageIds(queueB),
                "Recovered queueB instances must not include dequeued records");
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

    private MessageEnqueueRecord enqueueNewMessage(final TransactionLogResource queue)
    {
        final StoredMessage<StorableMessageMetaData> storedMessage =
                _store.addMessage((StorableMessageMetaData) new TestMessageMetaData(0L, 0)).allContentAdded();

        final Transaction txn = _store.newTransaction();
        final MessageEnqueueRecord record = txn.enqueueMessage(queue, new EnqueueableMessage<>()
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

        return record;
    }

    private void dequeueMessage(final MessageEnqueueRecord record)
    {
        final Transaction txn = _store.newTransaction();
        txn.dequeueMessage(record);
        txn.commitTran();
    }

    private List<Long> visitQueueMessageIds(final TransactionLogResource queue)
    {
        final List<Long> ids = new ArrayList<>();
        _storeReader.visitMessageInstances(queue, record ->
        {
            ids.add(record.getMessageNumber());
            return true;
        });
        return ids;
    }

    private boolean isStrictlyAscending(final List<Long> values)
    {
        long last = Long.MIN_VALUE;
        for (Long v : values)
        {
            if (v == null || v <= last)
            {
                return false;
            }
            last = v;
        }
        return true;
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
