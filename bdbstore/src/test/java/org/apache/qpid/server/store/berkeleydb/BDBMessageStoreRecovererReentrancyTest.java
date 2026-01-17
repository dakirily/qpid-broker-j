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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.TransactionConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.berkeleydb.tuple.QueueEntryBinding;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.SynchronousMessageStoreRecoverer;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

/**
 * Tests that deterministically detect whether recovery re-enters BDB JE from within the
 * {@link MessageStore.MessageStoreReader#visitMessageInstances} callback (while iterating a cursor).
 */
public class BDBMessageStoreRecovererReentrancyTest extends UnitTestBase
{
    private static final int BUFFER_SIZE = 10;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 1.0;

    private String _storeLocation;
    private BDBVirtualHost _storeParent;

    private InstrumentedBDBMessageStore _store;
    private ObservingEnvironmentFacade _observingFacade;

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(Objects.equals(getVirtualHostNodeStoreType(), VirtualHostNodeStoreType.BDB),
                "VirtualHostNodeStoreType should be BDB");

        _storeLocation = TMP_FOLDER + File.separator + getTestClassName() + File.separator + getTestName();
        deleteStoreIfExists();

        _storeParent = mock(BDBVirtualHost.class);
        when(_storeParent.getStorePath()).thenReturn(_storeLocation);
        when(_storeParent.getName()).thenReturn("bdbstore");
        when(_storeParent.getContextKeys(false)).thenReturn(Collections.emptySet());

        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE, SPARSITY_FRACTION);

        openStore();
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        try
        {
            closeStore();
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

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void recoverDoesNotBeginTransactionInsideCallbackOnValidRecoveryPath()
    {
        final UUID queueId = new UUID(0L, 0L);
        final TransactionLogResource queueResource = createQueueResource(queueId, "queue");
        enqueueNewMessage(queueResource);

        restartStore();

        final Queue queue = createMockQueue(queueId, "queue");
        final QueueManagingVirtualHost virtualHost = createVirtualHostForRecoverer(queue, queueId);

        _observingFacade.resetCounters();
        new SynchronousMessageStoreRecoverer().recover(virtualHost).join();

        assertEquals(0,
                _observingFacade.getBeginTransactionInCallback(),
                "Recovery must not start BDB transactions while iterating message instance cursor");
        assertEquals(0,
                _observingFacade.getBeginTransactionTotal(),
                "Recovery path without invalid instances must not start any BDB transactions");

        verify(queue).completeRecovery();
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void recoverFlowToDiskFromQueueRecoverDoesNotBeginTransactionInsideCallback()
    {
        final UUID queueId = new UUID(0x0102030405060708L, 0x0000000000000000L);
        final TransactionLogResource queueResource = createQueueResource(queueId, "queue");
        enqueueNewMessage(queueResource);

        restartStore();

        final Queue queue = createMockQueue(queueId, "queue");
        doAnswer(invocation ->
        {
            final ServerMessage<?> message = invocation.getArgument(0);
            message.getStoredMessage().flowToDisk();
            return null;
        }).when(queue).recover(any(ServerMessage.class), any(MessageEnqueueRecord.class));

        final QueueManagingVirtualHost virtualHost = createVirtualHostForRecoverer(queue, queueId);

        _observingFacade.resetCounters();
        new SynchronousMessageStoreRecoverer().recover(virtualHost).join();

        assertEquals(0,
                _observingFacade.getBeginTransactionInCallback(),
                "flowToDisk invoked from queue.recover must not start BDB transactions within cursor callback");
        assertEquals(0,
                _observingFacade.getBeginTransactionTotal(),
                "flowToDisk on recovered messages must not cause BDB transactions during recovery");
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void recoverInvalidInstancesAreDequeuedOutsideCallback()
    {
        final UUID queueId = new UUID(0x1111111111111111L, 0x0000000000000001L);
        final UUID unknownQueueId = new UUID(0x1111111111111111L, 0x0000000000000002L);

        final TransactionLogResource queueResource = createQueueResource(queueId, "queue");
        final long messageId = enqueueNewMessage(queueResource);

        restartStore();

        insertQueueEntry(unknownQueueId, messageId);

        restartStore();

        assertTrue(deliveryRecordExists(unknownQueueId, messageId),
                "Precondition failed: injected invalid record not found");

        final Queue queue = createMockQueue(queueId, "queue");
        final QueueManagingVirtualHost virtualHost = createVirtualHostForRecoverer(queue, queueId);

        _observingFacade.resetCounters();
        new SynchronousMessageStoreRecoverer().recover(virtualHost).join();

        assertEquals(0,
                _observingFacade.getBeginTransactionInCallback(),
                "Invalid-instance cleanup must not start BDB transactions within cursor callback");
        assertTrue(_observingFacade.getBeginTransactionTotal() > 0,
                "Invalid-instance cleanup must dequeue records using a BDB transaction");
        assertFalse(deliveryRecordExists(unknownQueueId, messageId),
                "Invalid record should be removed during recovery");
    }

    @Test
    public void probeDetectorDetectsBeginTransactionInsideCallback() throws Exception
    {
        final UUID queueId = new UUID(0x2222222222222222L, 0x0000000000000000L);
        final TransactionLogResource queueResource = createQueueResource(queueId, "queue");
        enqueueNewMessage(queueResource);

        restartStore();

        final MessageStore.MessageStoreReader reader = _store.newMessageStoreReader();
        try
        {
            _observingFacade.resetCounters();
            reader.visitMessageInstances(record ->
                    {
                        // This is a deliberate re-entrance used as a control to validate the detector.
                        final EnvironmentFacade environmentFacade = _store.getEnvironmentFacade();
                        final com.sleepycat.je.Transaction tx = environmentFacade.beginTransaction(null);
                        tx.abort();
                        return false;
                    },
                    record -> true,
                    record -> { });
        }
        finally
        {
            reader.close();
        }

        assertTrue(_observingFacade.getBeginTransactionInCallback() > 0,
                "Detector did not observe beginTransaction within callback as expected");
        assertEquals(_observingFacade.getBeginTransactionInCallback(),
                _observingFacade.getBeginTransactionTotal(),
                "All beginTransaction calls in this test must originate from inside callback");
    }

    private void openStore()
    {
        final ObservingEnvironmentFacadeFactory environmentFacadeFactory = new ObservingEnvironmentFacadeFactory();
        _store = new InstrumentedBDBMessageStore(environmentFacadeFactory);
        _store.openMessageStore(_storeParent);
        _observingFacade = (ObservingEnvironmentFacade) _store.getEnvironmentFacade();
    }

    private void closeStore()
    {
        if (_store != null)
        {
            _store.closeMessageStore();
            _store = null;
            _observingFacade = null;
        }
    }

    private void restartStore()
    {
        closeStore();
        openStore();
    }

    private TransactionLogResource createQueueResource(final UUID id, final String name)
    {
        final TransactionLogResource queue = mock(TransactionLogResource.class);
        when(queue.getId()).thenReturn(id);
        when(queue.getName()).thenReturn(name);
        when(queue.getMessageDurability()).thenReturn(MessageDurability.DEFAULT);
        return queue;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Queue createMockQueue(final UUID id, final String name)
    {
        final Queue queue = mock(Queue.class);
        when(queue.getId()).thenReturn(id);
        when(queue.getName()).thenReturn(name);
        when(queue.compareTo(any())).thenAnswer(invocation ->
        {
            final Object other = invocation.getArgument(0);
            if (other == null)
            {
                return 1;
            }
            if (other == queue)
            {
                return 0;
            }
            if (other instanceof Queue)
            {
                return name.compareTo(((Queue) other).getName());
            }
            return 0;
        });
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

    private QueueManagingVirtualHost<?> createVirtualHostForRecoverer(final Queue<?> queue, final UUID queueId)
    {
        final QueueManagingVirtualHost<?> virtualHost = mock(QueueManagingVirtualHost.class);
        when(virtualHost.getName()).thenReturn("vh");
        when(virtualHost.getEventLogger()).thenReturn(new EventLogger());
        when(virtualHost.getMessageStore()).thenReturn(_store);
        when(virtualHost.getChildren(Queue.class)).thenReturn(List.of(queue));
        when(virtualHost.getAttainedQueue(queueId)).thenReturn((Queue) queue);
        when(virtualHost.getDtxRegistry()).thenReturn(mock(DtxRegistry.class));
        return virtualHost;
    }

    private void insertQueueEntry(final UUID queueId, final long messageId)
    {
        final DatabaseEntry key = new DatabaseEntry();
        QueueEntryBinding.objectToEntry(queueId, messageId, key);

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

    private boolean deliveryRecordExists(final UUID queueId, final long messageId)
    {
        final DatabaseEntry key = new DatabaseEntry();
        QueueEntryBinding.objectToEntry(queueId, messageId, key);

        final EnvironmentFacade environmentFacade = _store.getEnvironmentFacade();
        final Database deliveryDb = environmentFacade.openDatabase("QUEUE_ENTRIES", BDBUtils.DEFAULT_DATABASE_CONFIG);

        final DatabaseEntry value = new DatabaseEntry();
        final OperationStatus status = deliveryDb.get(null, key, value, LockMode.READ_UNCOMMITTED);
        return OperationStatus.SUCCESS == status;
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

    private static final class CallbackContext
    {
        private static final ThreadLocal<Boolean> IN_CALLBACK = ThreadLocal.withInitial(() -> Boolean.FALSE);

        private static boolean isInCallback()
        {
            return Boolean.TRUE.equals(IN_CALLBACK.get());
        }

        private static void runInCallback(final Runnable action)
        {
            final boolean previous = isInCallback();
            IN_CALLBACK.set(Boolean.TRUE);
            try
            {
                action.run();
            }
            finally
            {
                IN_CALLBACK.set(previous);
            }
        }

        private static <T> T callInCallback(final java.util.concurrent.Callable<T> action)
        {
            final boolean previous = isInCallback();
            IN_CALLBACK.set(Boolean.TRUE);
            try
            {
                return action.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                IN_CALLBACK.set(previous);
            }
        }
    }

    private static class CallbackMarkingMessageStoreReader implements MessageStore.MessageStoreReader
    {
        private final MessageStore.MessageStoreReader _delegate;

        private CallbackMarkingMessageStoreReader(final MessageStore.MessageStoreReader delegate)
        {
            _delegate = delegate;
        }

        @Override
        public void visitMessages(final org.apache.qpid.server.store.handler.MessageHandler handler)
        {
            _delegate.visitMessages(handler);
        }

        @Override
        public void visitMessageInstances(final org.apache.qpid.server.store.handler.MessageInstanceHandler handler)
        {
            _delegate.visitMessageInstances(record -> CallbackContext.callInCallback(() -> handler.handle(record)));
        }

        @Override
        public void visitMessageInstances(final TransactionLogResource queue,
                                          final org.apache.qpid.server.store.handler.MessageInstanceHandler handler)
        {
            _delegate.visitMessageInstances(queue, record -> CallbackContext.callInCallback(() -> handler.handle(record)));
        }

        @Override
        public void visitMessageInstances(final org.apache.qpid.server.store.handler.MessageInstanceHandler validHandler,
                                          final java.util.function.Predicate<MessageEnqueueRecord> predicate,
                                          final java.util.function.Consumer<MessageEnqueueRecord> invalidCollector)
        {
            _delegate.visitMessageInstances(record -> CallbackContext.callInCallback(() -> validHandler.handle(record)),
                    record -> CallbackContext.callInCallback(() -> predicate.test(record)),
                    record -> CallbackContext.runInCallback(() -> invalidCollector.accept(record)));
        }

        @Override
        public void visitMessageInstances(final TransactionLogResource queue,
                                          final org.apache.qpid.server.store.handler.MessageInstanceHandler validHandler,
                                          final java.util.function.Predicate<MessageEnqueueRecord> predicate,
                                          final java.util.function.Consumer<MessageEnqueueRecord> invalidCollector)
        {
            _delegate.visitMessageInstances(queue,
                    record -> CallbackContext.callInCallback(() -> validHandler.handle(record)),
                    record -> CallbackContext.callInCallback(() -> predicate.test(record)),
                    record -> CallbackContext.runInCallback(() -> invalidCollector.accept(record)));
        }

        @Override
        public void visitDistributedTransactions(final org.apache.qpid.server.store.handler.DistributedTransactionHandler handler)
        {
            _delegate.visitDistributedTransactions(handler);
        }

        @Override
        public StoredMessage<?> getMessage(final long messageId)
        {
            return _delegate.getMessage(messageId);
        }

        @Override
        public void close()
        {
            _delegate.close();
        }
    }

    private static class InstrumentedBDBMessageStore extends BDBMessageStore
    {
        InstrumentedBDBMessageStore(final EnvironmentFacadeFactory environmentFacadeFactory)
        {
            super(environmentFacadeFactory);
        }

        @Override
        public MessageStoreReader newMessageStoreReader()
        {
            return new CallbackMarkingMessageStoreReader(super.newMessageStoreReader());
        }
    }

    private static class ObservingEnvironmentFacadeFactory implements EnvironmentFacadeFactory
    {
        private final EnvironmentFacadeFactory _delegate;

        private ObservingEnvironmentFacadeFactory()
        {
            _delegate = new StandardEnvironmentFacadeFactory();
        }

        @Override
        public EnvironmentFacade createEnvironmentFacade(final org.apache.qpid.server.model.ConfiguredObject<?> parent)
        {
            return new ObservingEnvironmentFacade(_delegate.createEnvironmentFacade(parent));
        }
    }

    private static class ObservingEnvironmentFacade implements EnvironmentFacade
    {
        private final EnvironmentFacade _delegate;

        private final AtomicInteger _beginTransactionInCallback = new AtomicInteger();
        private final AtomicInteger _beginTransactionTotal = new AtomicInteger();

        private final AtomicInteger _commitInCallback = new AtomicInteger();
        private final AtomicInteger _commitTotal = new AtomicInteger();

        private final AtomicInteger _commitNoSyncInCallback = new AtomicInteger();
        private final AtomicInteger _commitNoSyncTotal = new AtomicInteger();

        private final AtomicInteger _commitAsyncInCallback = new AtomicInteger();
        private final AtomicInteger _commitAsyncTotal = new AtomicInteger();

        private ObservingEnvironmentFacade(final EnvironmentFacade delegate)
        {
            _delegate = delegate;
        }

        int getBeginTransactionInCallback()
        {
            return _beginTransactionInCallback.get();
        }

        int getBeginTransactionTotal()
        {
            return _beginTransactionTotal.get();
        }

        void resetCounters()
        {
            _beginTransactionInCallback.set(0);
            _beginTransactionTotal.set(0);
            _commitInCallback.set(0);
            _commitTotal.set(0);
            _commitNoSyncInCallback.set(0);
            _commitNoSyncTotal.set(0);
            _commitAsyncInCallback.set(0);
            _commitAsyncTotal.set(0);
        }

        private void recordBeginTransaction()
        {
            _beginTransactionTotal.incrementAndGet();
            if (CallbackContext.isInCallback())
            {
                _beginTransactionInCallback.incrementAndGet();
            }
        }

        private void recordCommit()
        {
            _commitTotal.incrementAndGet();
            if (CallbackContext.isInCallback())
            {
                _commitInCallback.incrementAndGet();
            }
        }

        private void recordCommitNoSync()
        {
            _commitNoSyncTotal.incrementAndGet();
            if (CallbackContext.isInCallback())
            {
                _commitNoSyncInCallback.incrementAndGet();
            }
        }

        private void recordCommitAsync()
        {
            _commitAsyncTotal.incrementAndGet();
            if (CallbackContext.isInCallback())
            {
                _commitAsyncInCallback.incrementAndGet();
            }
        }

        @Override
        public void upgradeIfNecessary(final org.apache.qpid.server.model.ConfiguredObject<?> parent)
        {
            _delegate.upgradeIfNecessary(parent);
        }

        @Override
        public Database openDatabase(final String databaseName, final DatabaseConfig databaseConfig)
        {
            return _delegate.openDatabase(databaseName, databaseConfig);
        }

        @Override
        public Database clearDatabase(final com.sleepycat.je.Transaction txn,
                                      final String databaseName,
                                      final DatabaseConfig databaseConfig)
        {
            return _delegate.clearDatabase(txn, databaseName, databaseConfig);
        }

        @Override
        public Sequence openSequence(final Database database,
                                     final DatabaseEntry sequenceKey,
                                     final SequenceConfig sequenceConfig)
        {
            return _delegate.openSequence(database, sequenceKey, sequenceConfig);
        }

        @Override
        public com.sleepycat.je.Transaction beginTransaction(final TransactionConfig transactionConfig)
        {
            recordBeginTransaction();
            return _delegate.beginTransaction(transactionConfig);
        }

        @Override
        public void commit(final com.sleepycat.je.Transaction tx)
        {
            recordCommit();
            _delegate.commit(tx);
        }

        @Override
        public <X> CompletableFuture<X> commitAsync(final com.sleepycat.je.Transaction tx, final X val)
        {
            recordCommitAsync();
            return _delegate.commitAsync(tx, val);
        }

        @Override
        public void commitNoSync(final com.sleepycat.je.Transaction tx)
        {
            recordCommitNoSync();
            _delegate.commitNoSync(tx);
        }

        @Override
        public RuntimeException handleDatabaseException(final String contextMessage, final RuntimeException e)
        {
            return _delegate.handleDatabaseException(contextMessage, e);
        }

        @Override
        public void closeDatabase(final String name)
        {
            _delegate.closeDatabase(name);
        }

        @Override
        public void close()
        {
            _delegate.close();
        }

        @Override
        public long getTotalLogSize()
        {
            return _delegate.getTotalLogSize();
        }

        @Override
        public void reduceSizeOnDisk()
        {
            _delegate.reduceSizeOnDisk();
        }

        @Override
        public void flushLog()
        {
            _delegate.flushLog();
        }

        @Override
        public void setCacheSize(final long cacheSize)
        {
            _delegate.setCacheSize(cacheSize);
        }

        @Override
        public void flushLogFailed(final RuntimeException failure)
        {
            _delegate.flushLogFailed(failure);
        }

        @Override
        public void updateMutableConfig(final org.apache.qpid.server.model.ConfiguredObject<?> object)
        {
            _delegate.updateMutableConfig(object);
        }

        @Override
        public int cleanLog()
        {
            return _delegate.cleanLog();
        }

        @Override
        public void checkpoint(final boolean force)
        {
            _delegate.checkpoint(force);
        }

        @Override
        public java.util.Map<String, java.util.Map<String, Object>> getEnvironmentStatistics(final boolean reset)
        {
            return _delegate.getEnvironmentStatistics(reset);
        }

        @Override
        public java.util.Map<String, Object> getTransactionStatistics(final boolean reset)
        {
            return _delegate.getTransactionStatistics(reset);
        }

        @Override
        public java.util.Map<String, Object> getDatabaseStatistics(final String database, final boolean reset)
        {
            return _delegate.getDatabaseStatistics(database, reset);
        }

        @Override
        public void deleteDatabase(final String databaseName)
        {
            _delegate.deleteDatabase(databaseName);
        }
    }
}
