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
package org.apache.qpid.server.store.berkeleydb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.OperationStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.message.MessageDeletedException;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.protocol.v0_10.MessageMetaData_0_10;
import org.apache.qpid.server.protocol.v0_10.MessageTransferMessage;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore.MessageDeleteListener;
import org.apache.qpid.server.store.MessageStore.MessageStoreReader;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.berkeleydb.AbstractBDBMessageStore.StoredBDBMessage;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

class StoredBDBMessageTest extends UnitTestBase
{
    private static final int POOL_BUFFER_SIZE = 64;

    @TempDir
    private Path _storePath;

    private BDBVirtualHost _virtualHost;
    private BDBMessageStore _store;

    @BeforeEach
    void setUp()
    {
        QpidByteBuffer.deinitialisePool();
        QpidByteBuffer.initialisePool(POOL_BUFFER_SIZE, 16, 0.5);
        _virtualHost = mock(BDBVirtualHost.class);
        when(_virtualHost.getStorePath()).thenReturn(_storePath.toString());
        _store = new BDBMessageStore();
        _store.openMessageStore(_virtualHost);
    }

    @AfterEach
    void tearDown()
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
            QpidByteBuffer.deinitialisePool();
        }
    }

    @Test
    void testSingleFragmentKeepsFastPath() throws Exception
    {
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(3);
        try (final QpidByteBuffer empty = QpidByteBuffer.emptyQpidByteBuffer();
             final QpidByteBuffer src = QpidByteBuffer.wrap(new byte[] {0, 1, 2, 3}))
        {
            src.position(1);
            message.addContent(empty);
            message.addContent(src);
            message.addContent(empty);
            assertEquals(1, src.position());
        }

        assertNull(getContentFragments(message));
        message.allContentAdded();
        assertNull(getContentFragments(message));
        assertArrayEquals(new byte[] {1, 2, 3}, readContent(message, 0));
        assertEquals(message.getMetadataSize() + 3L, _store.getInMemorySize());
    }

    @Test
    void testEarlyReadConsolidatesFragmentsAndSubsequentAppendPreservesBody() throws Exception
    {
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(8);
        addFragment(message, new byte[] {1, 2});
        addFragment(message, new byte[] {3, 4});
        assertEquals(2, getContentFragments(message).size());

        try (final QpidByteBuffer view = message.getContent(1, Integer.MAX_VALUE))
        {
            assertNull(getContentFragments(message));
            assertArrayEquals(new byte[] {2, 3, 4}, copyBytes(view));

            addFragment(message, new byte[] {5, 6});
            addFragment(message, new byte[] {7, 8});
            assertEquals(3, getContentFragments(message).size());
            message.allContentAdded();
            message.allContentAdded();

            assertNull(getContentFragments(message));
            assertArrayEquals(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, readContent(message, 0));
            assertArrayEquals(new byte[] {2, 3, 4}, copyBytes(view));
            assertEquals(message.getMetadataSize() + 8L, _store.getInMemorySize());
        }
    }

    @Test
    void testReallocationConsolidatesPendingFragments() throws Exception
    {
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(4);
        addFragment(message, new byte[] {1, 2});
        addFragment(message, new byte[] {3, 4});
        assertEquals(2, getContentFragments(message).size());

        message.reallocate();

        assertNull(getContentFragments(message));
        assertArrayEquals(new byte[] {1, 2, 3, 4}, readContent(message, 0));
        message.allContentAdded();
        assertEquals(message.getMetadataSize() + 4L, _store.getInMemorySize());
    }

    @Test
    void testFlowToDiskPersistsPendingFragmentsAndAccountsForReload() throws Exception
    {
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(5);
        addFragment(message, new byte[] {1, 2});
        addFragment(message, new byte[] {3, 4, 5});
        assertEquals(message.getMetadataSize(), _store.getInMemorySize());

        assertTrue(message.flowToDisk());

        assertNull(getContentFragments(message));
        assertFalse(message.isInContentInMemory());
        assertEquals(0L, _store.getInMemorySize());
        assertEquals(message.getMetadataSize() + 5L, _store.getBytesEvacuatedFromMemory());
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, readContent(message, 0));
        assertEquals(5L, _store.getInMemorySize());
        assertNotNull(message.getMetaData());
        assertEquals(message.getMetadataSize() + 5L, _store.getInMemorySize());

        _store.closeMessageStore();
        _store.openMessageStore(_virtualHost);
        final MessageStoreReader reader = _store.newMessageStoreReader();
        try
        {
            final StoredMessage<?> recovered = reader.getMessage(message.getMessageNumber());
            assertNotNull(recovered);
            assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, readContent(recovered, 0));
            recovered.remove();
            assertEquals(0L, _store.getInMemorySize());
            assertNull(reader.getMessage(message.getMessageNumber()));
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    void testEmptyBodyRoundTrips()
    {
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(0);
        message.allContentAdded();
        assertArrayEquals(new byte[0], readContent(message, 0));
        assertEquals(message.getMetadataSize(), _store.getInMemorySize());

        assertTrue(message.flowToDisk());
        assertArrayEquals(new byte[0], readContent(message, 0));
        assertEquals(0L, _store.getInMemorySize());
    }

    @Test
    void testExplicitlyEmptyBodyStaysResidentAfterCommit()
    {
        _store.closeMessageStore();
        _store = spy(new BDBMessageStore());
        _store.openMessageStore(_virtualHost);
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        final Header header = new Header(deliveryProperties, new MessageProperties());
        final MessageMetaData_0_10 metadata = new MessageMetaData_0_10(header, 0, 1L);
        final MessageHandle<MessageMetaData_0_10> handle = _store.addMessage(metadata);
        final int buffersBefore = QpidByteBuffer.getNumberOfBuffersInUse();
        try (final QpidByteBuffer src = QpidByteBuffer.allocateDirect(POOL_BUFFER_SIZE))
        {
            src.position(src.limit());
            handle.addContent(src);
        }
        assertEquals(buffersBefore, QpidByteBuffer.getNumberOfBuffersInUse());
        final StoredBDBMessage<MessageMetaData_0_10> stored =
                (StoredBDBMessage<MessageMetaData_0_10>) handle.allContentAdded();
        stored.flushToStore();
        final MessageTransferMessage message = new MessageTransferMessage(stored, null);
        assertTrue(message.isPersistent());

        try (final QpidByteBuffer first = message.getBody();
             final QpidByteBuffer second = message.getBody())
        {
            assertFalse(first.hasRemaining());
            assertFalse(second.hasRemaining());
        }
        verify(_store, never()).getAllContent(stored.getMessageNumber());
        assertSame(metadata, stored.getMetaData());
        verify(_store, never()).getMessageMetaData(stored.getMessageNumber());
        assertTrue(stored.isInContentInMemory());
        assertEquals(stored.getMetadataSize(), _store.getInMemorySize());

        assertTrue(stored.flowToDisk());
        assertFalse(stored.isInContentInMemory());
        assertArrayEquals(new byte[0], readContent(stored, 0));
        verify(_store).getAllContent(stored.getMessageNumber());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReleasesUnfinishedFragments(final boolean closeStore) throws Exception
    {
        final int buffersBefore = QpidByteBuffer.getNumberOfBuffersInUse();
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(10);
        addFragment(message, new byte[] {1, 2});
        addFragment(message, new byte[] {3});
        assertEquals(buffersBefore + 2, QpidByteBuffer.getNumberOfBuffersInUse());
        assertEquals(message.getMetadataSize(), _store.getInMemorySize());

        if (closeStore)
        {
            _store.closeMessageStore();
        }
        else
        {
            message.remove();
        }

        assertNull(getContentFragments(message));
        assertEquals(buffersBefore, QpidByteBuffer.getNumberOfBuffersInUse());
        assertEquals(0L, _store.getInMemorySize());
    }

    @Test
    void testReleasesUnfinishedFragmentsWhenDeleteListenerFails() throws Exception
    {
        final int buffersBefore = QpidByteBuffer.getNumberOfBuffersInUse();
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(10);
        addFragment(message, new byte[] {1, 2});
        addFragment(message, new byte[] {3});
        final IllegalStateException failure = new IllegalStateException("Delete listener failed");
        _store.addMessageDeleteListener(deleted ->
        {
            throw failure;
        });

        assertSame(failure, assertThrows(IllegalStateException.class, message::remove));

        assertNull(getContentFragments(message));
        assertEquals(buffersBefore, QpidByteBuffer.getNumberOfBuffersInUse());
        assertEquals(0L, _store.getInMemorySize());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFailedDeleteReleasesPayloadAndCanBeRetried(final boolean listenerReadsMetadata) throws Exception
    {
        _store.closeMessageStore();
        when(_virtualHost.getContextKeys(false)).thenReturn(Set.of(EnvironmentConfig.LOCK_TIMEOUT));
        when(_virtualHost.getContextValue(String.class, EnvironmentConfig.LOCK_TIMEOUT)).thenReturn("50 ms");
        _store.openMessageStore(_virtualHost);

        final int buffersBefore = QpidByteBuffer.getNumberOfBuffersInUse();
        final int bodySize = 64 * 1024;
        final Header header = new Header(new DeliveryProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT), null);
        final MessageMetaData_0_10 metadata = new MessageMetaData_0_10(header, bodySize, 1L);
        final MessageHandle<MessageMetaData_0_10> handle = _store.addMessage(metadata);
        try (final QpidByteBuffer body = QpidByteBuffer.allocateDirect(bodySize))
        {
            handle.addContent(body);
        }
        final StoredMessage<MessageMetaData_0_10> stored = handle.allContentAdded();
        final MessageTransferMessage message = new MessageTransferMessage(stored, null);
        final TransactionLogResource queue = mock(TransactionLogResource.class);
        when(queue.getId()).thenReturn(UUID.randomUUID());
        final MessageReference<MessageTransferMessage> reference = message.newReference(queue);
        final Transaction enqueue = _store.newTransaction();
        final MessageEnqueueRecord record = enqueue.enqueueMessage(queue, message);
        enqueue.commitTran();
        final Transaction dequeue = _store.newTransaction();
        dequeue.dequeueMessage(record);
        dequeue.commitTran();

        final Set<?> tracked = getTrackedMessages();
        assertTrue(tracked.contains(stored));
        assertEquals(bodySize + (long) metadata.getStorableSize(), _store.getInMemorySize());
        assertTrue(QpidByteBuffer.getNumberOfBuffersInUse() > buffersBefore);

        final MessageDeleteListener listener = mock(MessageDeleteListener.class);
        _store.addMessageDeleteListener(listener);
        final EnvironmentFacade environment = _store.getEnvironmentFacade();
        final Database content = environment.openDatabase("MESSAGE_CONTENT", BDBUtils.DEFAULT_DATABASE_CONFIG);
        final Database metadataDb = environment.openDatabase("MESSAGE_METADATA", BDBUtils.DEFAULT_DATABASE_CONFIG);
        final DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(stored.getMessageNumber(), key);
        final DatabaseEntry value = new DatabaseEntry();
        value.setPartial(0, 0, true);
        final com.sleepycat.je.Transaction blocker = environment.beginTransaction(null);
        try
        {
            assertEquals(OperationStatus.SUCCESS, content.get(blocker, key, value, LockMode.RMW));
            final StoreException failure = assertThrows(StoreException.class, reference::release);
            assertTrue(failure.getCause() instanceof LockTimeoutException);
            assertEquals("Cannot remove messages", failure.getMessage());
        }
        finally
        {
            blocker.abort();
        }

        assertFalse(tracked.contains(stored));
        assertFalse(stored.isInContentInMemory());
        assertEquals(0L, stored.getInMemorySize());
        assertEquals(0L, _store.getInMemorySize());
        assertEquals(buffersBefore, QpidByteBuffer.getNumberOfBuffersInUse());
        assertThrows(MessageDeletedException.class, message::newReference);
        reference.release();
        verify(listener, never()).messageDeleted(stored);
        assertEquals(OperationStatus.SUCCESS, content.get(null, key, value, LockMode.READ_COMMITTED));
        assertEquals(OperationStatus.SUCCESS, metadataDb.get(null, key, value, LockMode.READ_COMMITTED));

        doAnswer(invocation ->
        {
            if (listenerReadsMetadata)
            {
                assertThrows(StoreException.class, stored::getMetaData);
            }
            else
            {
                assertThrows(StoreException.class, () -> readContent(stored, 0));
            }
            return null;
        }).when(listener).messageDeleted(stored);
        stored.remove();
        verify(listener).messageDeleted(stored);
        assertEquals(OperationStatus.NOTFOUND, content.get(null, key, value, LockMode.READ_COMMITTED));
        assertEquals(OperationStatus.NOTFOUND, metadataDb.get(null, key, value, LockMode.READ_COMMITTED));
        assertFalse(tracked.contains(stored));
        assertEquals(0L, _store.getInMemorySize());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTracksPayloadReloadedAfterFailedDelete(final boolean reloadMetadata) throws Exception
    {
        _store.closeMessageStore();
        _store = spy(new BDBMessageStore());
        _store.openMessageStore(_virtualHost);

        final byte[] content = {1, 2, 3};
        final StoredBDBMessage<TestMessageMetaData> stored = createMessage(content.length);
        addFragment(stored, content);
        stored.allContentAdded();
        stored.flushToStore();
        final StoreException failure = new StoreException("Delete failed");
        doThrow(failure).when(_store).removeMessage(stored.getMessageNumber());
        assertSame(failure, assertThrows(StoreException.class, stored::remove));
        final Set<?> tracked = getTrackedMessages();
        assertFalse(tracked.contains(stored));

        final StoreException readFailure = new StoreException("Reload failed");
        final long messageId = stored.getMessageNumber();
        if (reloadMetadata)
        {
            doThrow(readFailure).doCallRealMethod().when(_store).getMessageMetaData(messageId);
            assertSame(readFailure, assertThrows(StoreException.class, stored::getMetaData));
        }
        else
        {
            doThrow(readFailure).doCallRealMethod().when(_store).getAllContent(messageId);
            assertSame(readFailure, assertThrows(StoreException.class, () -> readContent(stored, 0)));
        }
        assertFalse(tracked.contains(stored));
        assertEquals(0L, _store.getInMemorySize());

        if (reloadMetadata)
        {
            assertNotNull(stored.getMetaData());
            doThrow(readFailure).when(_store).getAllContent(messageId);
            assertSame(readFailure, assertThrows(StoreException.class, () -> readContent(stored, 0)));
        }
        else
        {
            assertArrayEquals(content, readContent(stored, 0));
            doThrow(readFailure).when(_store).getMessageMetaData(messageId);
            assertSame(readFailure, assertThrows(StoreException.class, stored::getMetaData));
        }
        assertTrue(tracked.contains(stored));
        assertEquals(reloadMetadata ? stored.getMetadataSize() : content.length, _store.getInMemorySize());

        _store.closeMessageStore();
        assertEquals(0L, stored.getInMemorySize());
        assertEquals(0L, _store.getInMemorySize());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFinishesRemovalWhenQuotaCheckFails(final boolean listenerFails) throws Exception
    {
        _store.closeMessageStore();
        final int bodySize = 64 * 1024;
        when(_virtualHost.getStoreOverfullSize()).thenReturn(2L * bodySize - 1);
        when(_virtualHost.getStoreUnderfullSize()).thenReturn((long) bodySize);
        final EnvironmentFacade environment = new StandardEnvironmentFacadeFactory().createEnvironmentFacade(_virtualHost);
        final EnvironmentFacade facade = mock(EnvironmentFacade.class, delegatesTo(environment));
        doReturn(2L * bodySize).when(facade).getTotalLogSize();
        _store = new BDBMessageStore(parent -> facade);
        _store.openMessageStore(_virtualHost);

        final int buffersBefore = QpidByteBuffer.getNumberOfBuffersInUse();
        final TestMessageMetaData metadata = spy(new TestMessageMetaData(0L, bodySize));
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(metadata);
        try (final QpidByteBuffer body = QpidByteBuffer.allocateDirect(bodySize))
        {
            message.addContent(body);
        }
        message.allContentAdded();

        final TransactionLogResource queue = mock(TransactionLogResource.class);
        when(queue.getId()).thenReturn(UUID.randomUUID());
        final EnqueueableMessage<?> enqueued = mock(EnqueueableMessage.class);
        when(enqueued.getMessageNumber()).thenReturn(message.getMessageNumber());
        doReturn(message).when(enqueued).getStoredMessage();
        final Transaction enqueue = _store.newTransaction();
        final MessageEnqueueRecord delivery = enqueue.enqueueMessage(queue, enqueued);
        enqueue.commitTran();
        final Transaction dequeue = _store.newTransaction();
        dequeue.dequeueMessage(delivery);
        dequeue.commitTran();
        verify(facade).getTotalLogSize();

        final Set<?> tracked = getTrackedMessages();
        assertTrue(tracked.contains(message));
        assertEquals(bodySize + (long) message.getMetadataSize(), _store.getInMemorySize());
        assertTrue(QpidByteBuffer.getNumberOfBuffersInUse() > buffersBefore);

        final IllegalStateException quotaFailure = new IllegalStateException("Quota size lookup failed");
        doThrow(quotaFailure).when(facade).getTotalLogSize();
        final MessageDeleteListener listener = mock(MessageDeleteListener.class);
        final IllegalStateException listenerFailure = new IllegalStateException("Delete listener failed");
        if (listenerFails)
        {
            doThrow(listenerFailure).when(listener).messageDeleted(message);
        }
        _store.addMessageDeleteListener(listener);

        final StoreException failure = assertThrows(StoreException.class, message::remove);
        assertSame(quotaFailure, failure.getCause());
        assertFalse(tracked.contains(message));
        assertNull(message.getMetaData());
        assertEquals(0L, _store.getInMemorySize());
        assertEquals(buffersBefore, QpidByteBuffer.getNumberOfBuffersInUse());
        assertEquals(listenerFails ? List.of(listenerFailure) : List.of(), List.of(failure.getSuppressed()));

        message.remove();
        verify(listener).messageDeleted(message);
        verify(metadata).dispose();
        verify(facade, times(2)).getTotalLogSize();
        final MessageStoreReader reader = _store.newMessageStoreReader();
        try
        {
            assertNull(reader.getMessage(message.getMessageNumber()));
            assertThrows(StoreException.class, () -> _store.getAllContent(message.getMessageNumber()));
        }
        finally
        {
            reader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEvictedPayloadsReloadIndependently(final boolean metadataFirst)
    {
        final byte[] body = {1, 2, 3};
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(body.length);
        addFragment(message, body);
        assertTrue(message.flowToDisk());
        assertEquals(0L, message.getInMemorySize());

        if (metadataFirst)
        {
            final TestMessageMetaData metadata = message.getMetaData();
            assertNotNull(metadata);
            assertSame(metadata, message.getMetaData());
            assertEquals(message.getMetadataSize(), _store.getInMemorySize());
            assertFalse(message.isInContentInMemory());
            assertArrayEquals(body, readContent(message, 0));
        }
        else
        {
            assertArrayEquals(body, readContent(message, 0));
            assertEquals(body.length, _store.getInMemorySize());
            assertNotNull(message.getMetaData());
        }

        final long totalBytes = message.getMetadataSize() + (long) body.length;
        assertEquals(totalBytes, message.getInMemorySize());
        assertEquals(totalBytes, _store.getInMemorySize());
        assertArrayEquals(body, readContent(message, 0));
        assertEquals(totalBytes, _store.getInMemorySize());
        assertTrue(message.flowToDisk());
        assertTrue(message.flowToDisk());
        assertEquals(0L, _store.getInMemorySize());
        assertEquals(2 * totalBytes, _store.getBytesEvacuatedFromMemory());
    }

    @Test
    void testRemovalWhileEvictedPreventsReloadAndRejectsWrites()
    {
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(3);
        addFragment(message, new byte[] {1, 2, 3});
        assertTrue(message.flowToDisk());

        message.remove();
        message.remove();
        addFragment(message, new byte[] {4, 5, 6});
        message.allContentAdded();

        final TransactionLogResource queue = mock(TransactionLogResource.class);
        when(queue.getId()).thenReturn(UUID.randomUUID());
        final EnqueueableMessage<?> enqueued = mock(EnqueueableMessage.class);
        when(enqueued.getMessageNumber()).thenReturn(message.getMessageNumber());
        doReturn(message).when(enqueued).getStoredMessage();
        final Transaction transaction = _store.newTransaction();
        try
        {
            transaction.enqueueMessage(queue, enqueued);
            assertThrows(StoreException.class, transaction::commitTran);
        }
        finally
        {
            transaction.abortTran();
        }

        message.flushToStore();
        assertTrue(message.flowToDisk());
        message.reallocate();
        message.clear(true);

        assertNull(message.getMetaData());
        assertArrayEquals(new byte[0], readContent(message, 0));
        assertFalse(message.isInContentInMemory());
        assertEquals(0L, message.getInMemorySize());
        assertEquals(0L, _store.getInMemorySize());
        assertEquals(message.getMetadataSize() + 3L, _store.getBytesEvacuatedFromMemory());
        final MessageStoreReader reader = _store.newMessageStoreReader();
        try
        {
            assertNull(reader.getMessage(message.getMessageNumber()));
            reader.visitMessageInstances(queue, record -> fail("Unexpected delivery for removed message"));
        }
        finally
        {
            reader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testClearPreservesReloadEligibilityWhenMetadataCleanupThrows(final boolean close)
    {
        final byte[] body = {1, 2, 3};
        final TestMessageMetaData metadata = spy(new TestMessageMetaData(0L, body.length));
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(metadata);
        addFragment(message, body);
        message.flushToStore();

        final IllegalStateException failure = new IllegalStateException("Metadata cleanup failed");
        if (close)
        {
            doThrow(failure).when(metadata).dispose();
        }
        else
        {
            doThrow(failure).when(metadata).clearEncodedForm();
        }
        assertSame(failure, assertThrows(IllegalStateException.class, () -> message.clear(close)));
        message.clear(close);

        if (close)
        {
            verify(metadata).dispose();
        }
        else
        {
            verify(metadata).clearEncodedForm();
        }
        assertFalse(message.isInContentInMemory());
        assertEquals(0L, message.getInMemorySize());
        assertEquals(0L, _store.getInMemorySize());
        assertEquals(0L, _store.getBytesEvacuatedFromMemory());

        assertNotSame(metadata, message.getMetaData());
        assertArrayEquals(body, readContent(message, 0));
        assertEquals(message.getMetadataSize() + (long) body.length, _store.getInMemorySize());
        message.remove();
        assertEquals(0L, _store.getInMemorySize());
    }

    @Test
    void testConcurrentReloadAndEvictionPreservePayloadAndAccounting() throws Exception
    {
        final byte[] body = {1, 2, 3, 4};
        final StoredBDBMessage<TestMessageMetaData> message = createMessage(body.length);
        addFragment(message, body);
        assertTrue(message.flowToDisk());

        final ExecutorService executor = Executors.newFixedThreadPool(2,
                QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(Executors.defaultThreadFactory()));
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch start = new CountDownLatch(1);
        try
        {
            final Future<?> reloads = executor.submit(() ->
            {
                ready.countDown();
                start.await();
                for (int i = 0; i < 200; i++)
                {
                    assertEquals(body.length, message.getMetaData().getContentSize());
                    assertArrayEquals(body, readContent(message, 0));
                }
                return null;
            });
            final Future<?> evictions = executor.submit(() ->
            {
                ready.countDown();
                start.await();
                for (int i = 0; i < 200; i++)
                {
                    assertTrue(message.flowToDisk());
                }
                return null;
            });
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            reloads.get(10, TimeUnit.SECONDS);
            evictions.get(10, TimeUnit.SECONDS);
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertTrue(message.flowToDisk());
        assertEquals(0L, message.getInMemorySize());
        assertEquals(0L, _store.getInMemorySize());
        assertNotNull(message.getMetaData());
        assertArrayEquals(body, readContent(message, 0));
        assertEquals(message.getMetadataSize() + (long) body.length, _store.getInMemorySize());
        message.remove();
        assertEquals(0L, _store.getInMemorySize());
    }

    private StoredBDBMessage<TestMessageMetaData> createMessage(final int contentSize)
    {
        return createMessage(new TestMessageMetaData(0L, contentSize));
    }

    private StoredBDBMessage<TestMessageMetaData> createMessage(final TestMessageMetaData metadata)
    {
        return (StoredBDBMessage<TestMessageMetaData>) _store.addMessage(metadata);
    }

    private static void addFragment(final StoredBDBMessage<?> message, final byte[] bytes)
    {
        try (final QpidByteBuffer src = QpidByteBuffer.allocateDirect(POOL_BUFFER_SIZE))
        {
            src.put(bytes).flip();
            message.addContent(src);
        }
    }

    private static byte[] readContent(final StoredMessage<?> message, final int offset)
    {
        try (final QpidByteBuffer content = message.getContent(offset, Integer.MAX_VALUE))
        {
            return copyBytes(content);
        }
    }

    private static byte[] copyBytes(final QpidByteBuffer content)
    {
        final byte[] bytes = new byte[content.remaining()];
        content.copyTo(bytes);
        return bytes;
    }

    private static List<?> getContentFragments(final StoredBDBMessage<?> message) throws ReflectiveOperationException
    {
        final Field field = StoredBDBMessage.class.getDeclaredField("_contentFragments");
        field.setAccessible(true);
        return (List<?>) field.get(message);
    }

    private Set<?> getTrackedMessages() throws ReflectiveOperationException
    {
        final Field field = AbstractBDBMessageStore.class.getDeclaredField("_messages");
        field.setAccessible(true);
        return (Set<?>) field.get(_store);
    }
}
