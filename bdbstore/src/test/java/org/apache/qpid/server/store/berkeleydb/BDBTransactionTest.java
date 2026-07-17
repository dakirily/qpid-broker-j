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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;
import org.slf4j.Logger;

import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.SizeMonitoringSettings;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;

class BDBTransactionTest
{
    private static final UUID QUEUE_ID = UUID.fromString("1a12dc3a-cc99-4da8-bafe-0d2c1b8c24df");

    private EnvironmentFacade _environmentFacade;
    private com.sleepycat.je.Transaction _jeTransaction;
    private Database _metadataDatabase;
    private Database _contentDatabase;
    private Database _deliveryDatabase;
    private Database _xidDatabase;
    private TestBDBMessageStore _store;
    private TransactionLogResource _queue;
    private List<Long> _storedMessageIds;

    @BeforeEach
    void setUp()
    {
        _environmentFacade = mock(EnvironmentFacade.class);
        _jeTransaction = mock(com.sleepycat.je.Transaction.class);
        _metadataDatabase = mock(Database.class);
        _contentDatabase = mock(Database.class);
        _deliveryDatabase = mock(Database.class);
        _xidDatabase = mock(Database.class);
        _storedMessageIds = new ArrayList<>();

        when(_environmentFacade.beginTransaction(isNull())).thenReturn(_jeTransaction);
        when(_environmentFacade.openDatabase(anyString(), any())).thenAnswer(invocation ->
        {
            final String name = invocation.getArgument(0);
            return switch (name)
            {
                case "MESSAGE_METADATA" -> _metadataDatabase;
                case "MESSAGE_CONTENT" -> _contentDatabase;
                case "QUEUE_ENTRIES" -> _deliveryDatabase;
                case "XIDS" -> _xidDatabase;
                default -> mock(Database.class);
            };
        });
        when(_environmentFacade.handleDatabaseException(anyString(), any(RuntimeException.class)))
                .thenAnswer(invocation -> invocation.getArgument(1));
        when(_contentDatabase.put(any(), any(), any())).thenReturn(OperationStatus.SUCCESS);
        when(_deliveryDatabase.put(any(), any(), any())).thenReturn(OperationStatus.SUCCESS);
        when(_deliveryDatabase.delete(any(), any())).thenReturn(OperationStatus.SUCCESS);
        when(_xidDatabase.put(any(), any(), any())).thenReturn(OperationStatus.SUCCESS);
        when(_environmentFacade.commitAsync(same(_jeTransaction), any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(invocation.getArgument(1)));

        doAnswer(invocation ->
        {
            _storedMessageIds.add(LongBinding.entryToLong(invocation.getArgument(1)));
            return OperationStatus.SUCCESS;
        }).when(_metadataDatabase).put(same(_jeTransaction), any(), any());

        _store = new TestBDBMessageStore(_environmentFacade);
        _store.openMessageStore(createParent());

        _queue = mock(TransactionLogResource.class);
        when(_queue.getId()).thenReturn(QUEUE_ID);
        when(_queue.getName()).thenReturn("testQueue");
        when(_queue.getMessageDurability()).thenReturn(MessageDurability.DEFAULT);
    }

    @AfterEach
    void tearDown()
    {
        _store.closeMessageStore();
    }

    @ParameterizedTest
    @EnumSource(CommitMode.class)
    void singleMessageUsesFastFieldAndSameIdentityIsStoredOnce(final CommitMode mode) throws Exception
    {
        final AbstractBDBMessageStore.StoredBDBMessage<InternalMessageMetaData> message = createMessage(1L, 7);
        final Transaction transaction = _store.newTransaction();
        final TransactionLogResource secondQueue = mock(TransactionLogResource.class);
        when(secondQueue.getId()).thenReturn(UUID.fromString("cf0a4af0-e3e9-43a4-a4d1-86f0ae0f5601"));
        when(secondQueue.getName()).thenReturn("secondQueue");

        transaction.enqueueMessage(_queue, enqueueable(message));
        transaction.enqueueMessage(secondQueue, enqueueable(message));

        assertSame(message, getField(transaction, "_messageToStore"));
        assertNull(getField(transaction, "_messagesToStore"));
        assertNull(getField(transaction, "_messagesToStoreSet"));
        assertEquals(7, getField(transaction, "_storeSizeIncrease"));

        commit(transaction, mode);

        assertEquals(List.of(1L), _storedMessageIds);
        assertTrackedMessagesCleared(transaction);
        verify(_deliveryDatabase, times(2)).put(same(_jeTransaction), any(), any());
        verifyCommit(mode);
    }

    @ParameterizedTest
    @EnumSource(CommitMode.class)
    void secondDistinctMessagePromotesAndPreservesFirstEncounterOrder(final CommitMode mode) throws Exception
    {
        final AbstractBDBMessageStore.StoredBDBMessage<InternalMessageMetaData> first = createMessage(11L, 5);
        final AbstractBDBMessageStore.StoredBDBMessage<InternalMessageMetaData> second = createMessage(22L, 9);
        final Transaction transaction = _store.newTransaction();

        transaction.enqueueMessage(_queue, enqueueable(first));
        transaction.enqueueMessage(_queue, enqueueable(second));
        transaction.enqueueMessage(_queue, enqueueable(first));
        transaction.enqueueMessage(_queue, enqueueable(second));

        assertNull(getField(transaction, "_messageToStore"));
        assertEquals(List.of(first, second), getField(transaction, "_messagesToStore"));
        final IdentityHashMap<?, ?> identitySet = (IdentityHashMap<?, ?>) getField(transaction, "_messagesToStoreSet");
        assertEquals(2, identitySet.size());
        assertTrue(identitySet.containsKey(first));
        assertTrue(identitySet.containsKey(second));
        assertEquals(14, getField(transaction, "_storeSizeIncrease"));

        commit(transaction, mode);

        assertEquals(List.of(11L, 22L), _storedMessageIds);
        assertTrackedMessagesCleared(transaction);
        verifyCommit(mode);
    }

    @Test
    void abortDiscardsTrackedMessagesWithoutWritingTheirPayload() throws Exception
    {
        final Transaction transaction = _store.newTransaction();
        transaction.enqueueMessage(_queue, enqueueable(createMessage(31L, 3)));
        transaction.enqueueMessage(_queue, enqueueable(createMessage(32L, 4)));

        transaction.abortTran();

        verify(_metadataDatabase, never()).put(any(), any(), any());
        verify(_contentDatabase, never()).put(any(), any(), any());
        verify(_jeTransaction).abort();
        verify(_environmentFacade, never()).commit(any());
        verify(_environmentFacade, never()).commitAsync(any(), any());
        assertTrackedMessagesCleared(transaction);
    }

    @Test
    @SuppressWarnings("unchecked")
    void emptyDequeueOnlyAndNonBdbTransactionsDoNotAllocateMessageTracking() throws Exception
    {
        final Transaction empty = _store.newTransaction();
        assertTrackedMessagesCleared(empty);
        empty.abortTran();

        final Transaction dequeueOnly = _store.newTransaction();
        final MessageEnqueueRecord enqueueRecord = mock(MessageEnqueueRecord.class);
        when(enqueueRecord.getQueueId()).thenReturn(QUEUE_ID);
        when(enqueueRecord.getMessageNumber()).thenReturn(41L);
        dequeueOnly.dequeueMessage(enqueueRecord);
        assertTrackedMessagesCleared(dequeueOnly);
        dequeueOnly.abortTran();

        final Transaction nonBdb = _store.newTransaction();
        final StoredMessage<InternalMessageMetaData> externalStoredMessage = mock(StoredMessage.class);
        nonBdb.enqueueMessage(_queue, enqueueable(42L, externalStoredMessage));
        assertTrackedMessagesCleared(nonBdb);
        nonBdb.abortTran();

        verify(_jeTransaction, times(3)).abort();
        verify(_metadataDatabase, never()).put(any(), any(), any());
        verify(_contentDatabase, never()).put(any(), any(), any());
    }

    @ParameterizedTest
    @EnumSource(CommitMode.class)
    void preCommitFailureRetainsTrackingUntilAbort(final CommitMode mode) throws Exception
    {
        final RuntimeException failure = new RuntimeException("metadata write failed");
        final AtomicBoolean failSecondMessage = new AtomicBoolean(true);
        doAnswer(invocation ->
        {
            final long messageId = LongBinding.entryToLong(invocation.getArgument(1));
            _storedMessageIds.add(messageId);
            if (messageId == 52L && failSecondMessage.compareAndSet(true, false))
            {
                throw failure;
            }
            return OperationStatus.SUCCESS;
        }).when(_metadataDatabase).put(same(_jeTransaction), any(), any());

        final AbstractBDBMessageStore.StoredBDBMessage<InternalMessageMetaData> first = createMessage(51L, 3);
        final AbstractBDBMessageStore.StoredBDBMessage<InternalMessageMetaData> second = createMessage(52L, 4);
        final Transaction transaction = _store.newTransaction();
        transaction.enqueueMessage(_queue, enqueueable(first));
        transaction.enqueueMessage(_queue, enqueueable(second));

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> commit(transaction, mode));
        assertSame(failure, thrown);
        assertEquals(List.of(51L, 52L), _storedMessageIds);
        assertEquals(List.of(first, second), getField(transaction, "_messagesToStore"));
        assertEquals(2, ((IdentityHashMap<?, ?>) getField(transaction, "_messagesToStoreSet")).size());
        assertEquals(7, getField(transaction, "_storeSizeIncrease"));
        verify(_environmentFacade, never()).commit(any());
        verify(_environmentFacade, never()).commitAsync(any(), any());

        transaction.abortTran();

        assertTrackedMessagesCleared(transaction);
        verify(_jeTransaction).abort();
    }

    @Test
    void recordXidStoresBdbEnqueueMessagesBeforeWritingXid() throws Exception
    {
        final AbstractBDBMessageStore.StoredBDBMessage<InternalMessageMetaData> message = createMessage(61L, 8);
        final EnqueueableMessage<InternalMessageMetaData> enqueueableMessage = enqueueable(message);
        final Transaction.EnqueueRecord enqueueRecord = new Transaction.EnqueueRecord()
        {
            @Override
            public TransactionLogResource getResource()
            {
                return _queue;
            }

            @Override
            public EnqueueableMessage<?> getMessage()
            {
                return enqueueableMessage;
            }
        };
        final Transaction transaction = _store.newTransaction();

        transaction.recordXid(7L,
                              new byte[] { 1, 2 },
                              new byte[] { 3, 4 },
                              new Transaction.EnqueueRecord[] { enqueueRecord },
                              new Transaction.DequeueRecord[0]);

        final InOrder writeOrder = inOrder(_metadataDatabase, _contentDatabase, _xidDatabase);
        writeOrder.verify(_metadataDatabase).put(same(_jeTransaction), any(), any());
        writeOrder.verify(_contentDatabase).put(same(_jeTransaction), any(), any());
        writeOrder.verify(_xidDatabase).put(same(_jeTransaction), any(), any());
        assertEquals(List.of(61L), _storedMessageIds);
        assertEquals(0, getField(transaction, "_storeSizeIncrease"));
        assertTrackedMessagesCleared(transaction);

        transaction.abortTran();
        verify(_jeTransaction).abort();
    }

    private AbstractBDBMessageStore.StoredBDBMessage<InternalMessageMetaData> createMessage(final long id,
                                                                                            final int contentSize)
    {
        return _store.new StoredBDBMessage<>(id, new InternalMessageMetaData(true, null, contentSize), false);
    }

    private static EnqueueableMessage<InternalMessageMetaData> enqueueable(
            final AbstractBDBMessageStore.StoredBDBMessage<InternalMessageMetaData> message)
    {
        return enqueueable(message.getMessageNumber(), message);
    }

    private static EnqueueableMessage<InternalMessageMetaData> enqueueable(
            final long messageNumber, final StoredMessage<InternalMessageMetaData> storedMessage)
    {
        return new EnqueueableMessage<>()
        {
            @Override
            public long getMessageNumber()
            {
                return messageNumber;
            }

            @Override
            public boolean isPersistent()
            {
                return true;
            }

            @Override
            public StoredMessage<InternalMessageMetaData> getStoredMessage()
            {
                return storedMessage;
            }
        };
    }

    private void commit(final Transaction transaction, final CommitMode mode)
    {
        if (mode == CommitMode.SYNCHRONOUS)
        {
            transaction.commitTran();
        }
        else
        {
            final Object result = new Object();
            assertSame(result, transaction.commitTranAsync(result).join());
        }
    }

    private void verifyCommit(final CommitMode mode)
    {
        if (mode == CommitMode.SYNCHRONOUS)
        {
            verify(_environmentFacade).commit(same(_jeTransaction));
            verify(_environmentFacade, never()).commitAsync(any(), any());
        }
        else
        {
            verify(_environmentFacade).commitAsync(same(_jeTransaction), any());
            verify(_environmentFacade, never()).commit(any());
        }
    }

    private static void assertTrackedMessagesCleared(final Transaction transaction) throws Exception
    {
        assertNull(getField(transaction, "_messageToStore"));
        assertNull(getField(transaction, "_messagesToStore"));
        assertNull(getField(transaction, "_messagesToStoreSet"));
    }

    private static Object getField(final Transaction transaction, final String fieldName) throws Exception
    {
        final Field field = transaction.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(transaction);
    }

    private static ConfiguredObject<?> createParent()
    {
        final ConfiguredObject<?> parent = mock(ConfiguredObject.class,
                org.mockito.Mockito.withSettings().extraInterfaces(SizeMonitoringSettings.class));
        when(((SizeMonitoringSettings) parent).getStoreOverfullSize()).thenReturn(1024L);
        when(((SizeMonitoringSettings) parent).getStoreUnderfullSize()).thenReturn(512L);
        return parent;
    }

    private enum CommitMode
    {
        SYNCHRONOUS,
        ASYNCHRONOUS
    }

    private static final class TestBDBMessageStore extends AbstractBDBMessageStore
    {
        private final EnvironmentFacade _environmentFacade;
        private final Logger _logger = mock(Logger.class);

        private TestBDBMessageStore(final EnvironmentFacade environmentFacade)
        {
            _environmentFacade = environmentFacade;
        }

        @Override
        protected void doOpen(final ConfiguredObject<?> parent)
        {
        }

        @Override
        protected void doClose()
        {
        }

        @Override
        protected EnvironmentFacade getEnvironmentFacade()
        {
            return _environmentFacade;
        }

        @Override
        protected Logger getLogger()
        {
            return _logger;
        }

        @Override
        public String getStoreLocation()
        {
            return "";
        }

        @Override
        public File getStoreLocationAsFile()
        {
            return null;
        }

        @Override
        public void onDelete(final ConfiguredObject<?> parent)
        {
        }
    }
}
