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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreTestCase;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.rocksdb.RocksDBVirtualHost;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    /**
     * Creates a RocksDB virtual host stub for message store tests.
     *
     * @return the virtual host.
     */
    @Override
    protected VirtualHost<?> createVirtualHost()
    {
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
}
