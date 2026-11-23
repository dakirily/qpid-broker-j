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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.OperationStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.protocol.v0_8.AMQMessage;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.MessageMetaData;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreTestCase;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.berkeleydb.AbstractBDBMessageStore.BDBMessageStoreReader;
import org.apache.qpid.server.store.berkeleydb.tuple.MessageMetaDataBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.QueueEntryBinding;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;
import org.apache.qpid.test.utils.VirtualHostNodeStoreType;

/**
 * Subclass of MessageStoreTestCase which runs the standard tests from the superclass against
 * the BDB Store as well as additional tests specific to the BDB store-implementation.
 */
public class BDBMessageStoreTest extends MessageStoreTestCase
{
    private static final byte[] CONTENT_BYTES = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    // An 8 KiB block stores one header with the first ID, followed by one-byte deltas.
    private static final int SINGLE_BLOCK_DELIVERIES = 8 * 1024 - 3 * Long.BYTES - Integer.BYTES + 1;

    private String _storeLocation;

    @Override
    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(is(equalTo(VirtualHostNodeStoreType.BDB)).matches(VirtualHostNodeStoreType.BDB),
                "VirtualHostNodeStoreType should be BDB");
        super.setUp();
    }

    @Override
    @AfterEach
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

    private MessagePublishInfo createPublishInfoBody_0_8()
    {
        return new MessagePublishInfo(AMQShortString.createAMQShortString("exchange12345"), false, true,
                                      AMQShortString.createAMQShortString("routingKey12345"));

    }

    private ContentHeaderBody createContentHeaderBody_0_8(BasicContentHeaderProperties props, int length)
    {
        return new ContentHeaderBody(props, length);
    }

    private BasicContentHeaderProperties createContentHeaderProperties_0_8()
    {
        BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setDeliveryMode(Integer.valueOf(BasicContentHeaderProperties.PERSISTENT).byteValue());
        props.setContentType("text/html");
        props.setHeaders(FieldTableFactory.createFieldTable(Collections.singletonMap("Test", "MST")));
        return props;
    }

    /**
     * Tests that messages which are added to the store and then removed using the
     * public MessageStore interfaces are actually removed from the store by then
     * interrogating the store with its own implementation methods and verifying
     * expected exceptions are thrown to indicate the message is not present.
     */
    @Test
    public void testMessageCreationAndRemoval()
    {
        BDBMessageStore bdbStore = (BDBMessageStore) getStore();

        StoredMessage<MessageMetaData> storedMessage_0_8 = createAndStoreSingleChunkMessage_0_8(bdbStore);
        long messageid_0_8 = storedMessage_0_8.getMessageNumber();

        bdbStore.removeMessage(messageid_0_8);

        //verify the removal using the BDB store implementation methods directly
        try
        {
            // the next line should throw since the message id should not be found
            bdbStore.getMessageMetaData(messageid_0_8);
            fail("No exception thrown when message id not found getting metadata");
        }
        catch (StoreException e)
        {
            // pass since exception expected
        }

        try
        {
            bdbStore.getAllContent(messageid_0_8);
            fail("Expected exception not thrown");
        }
        catch (StoreException se)
        {
            // PASS
        }
    }

    @Test
    void testMetadataWrittenThroughDuplicateSurvivesReopen()
    {
        final MessagePublishInfo publishInfo = createPublishInfoBody_0_8();
        final ContentHeaderBody header = createContentHeaderBody_0_8(createContentHeaderProperties_0_8(),
                                                                    CONTENT_BYTES.length);
        final MessageMetaData metadata = new MessageMetaData(publishInfo, header)
        {
            @Override
            public void writeToBuffer(final QpidByteBuffer destination)
            {
                try (final QpidByteBuffer duplicate = destination.duplicate())
                {
                    super.writeToBuffer(duplicate);
                }
            }
        };
        final MessageHandle<MessageMetaData> handle = getStore().addMessage(metadata);
        try (final QpidByteBuffer content = QpidByteBuffer.wrap(CONTENT_BYTES))
        {
            handle.addContent(content);
        }
        final AMQMessage message = new AMQMessage(handle.allContentAdded());
        final Transaction transaction = getStore().newTransaction();
        try
        {
            transaction.enqueueMessage(createQueue(new UUID(0L, 1L)), message);
            transaction.commitTran();
        }
        catch (final StoreException e)
        {
            transaction.abortTran();
            throw e;
        }

        reopenStore();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final StoredMessage<?> recovered = reader.getMessage(message.getMessageNumber());
            assertArrayEquals(CONTENT_BYTES, copyContent(recovered));
            final MessageMetaData recoveredMetadata = (MessageMetaData) recovered.getMetaData();
            assertEquals(publishInfo.getRoutingKey(), recoveredMetadata.getMessagePublishInfo().getRoutingKey());
            assertEquals("text/html",
                         recoveredMetadata.getContentHeaderBody().getProperties().getContentTypeAsString());
            assertTrue(recoveredMetadata.isPersistent());
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testReopenAndDequeueLegacyEntries()
    {
        final TransactionLogResource queue = createQueue(new UUID(Long.MIN_VALUE, 1L));
        final BDBMessageStore store = (BDBMessageStore) getStore();
        final long messageId = store.getNextMessageId();
        writeLegacyEntries(store.getEnvironmentFacade(), queue.getId(), messageId);

        reopenStore();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            assertArrayEquals(CONTENT_BYTES, copyContent(reader.getMessage(messageId)));
            final List<MessageEnqueueRecord> records = readQueueEntries(reader, queue);
            assertEquals(1, records.size());
            final MessageEnqueueRecord record = records.get(0);
            assertEquals(queue.getId(), record.getQueueId());
            assertEquals(messageId, record.getMessageNumber());

            final Transaction transaction = getStore().newTransaction();
            transaction.dequeueMessage(record);
            transaction.commitTran();

            assertTrue(readQueueEntries(reader, queue).isEmpty());
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testReuseKeysAcrossWritesInOneTransaction()
    {
        final TransactionLogResource firstQueue = createQueue(new UUID(Long.MIN_VALUE, 1L));
        final TransactionLogResource secondQueue = createQueue(new UUID(Long.MIN_VALUE, 2L));
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final InternalMessage first = InternalMessage.createStringMessage(getStore(), header, "first", true);
        final InternalMessage second = InternalMessage.createStringMessage(getStore(), header, "second", true);
        final byte[] firstContent = copyContent(first.getStoredMessage());
        final byte[] secondContent = copyContent(second.getStoredMessage());

        final Transaction enqueue = getStore().newTransaction();
        final MessageEnqueueRecord firstEntry = enqueue.enqueueMessage(firstQueue, first);
        final MessageEnqueueRecord secondQueueEntry = enqueue.enqueueMessage(secondQueue, second);
        enqueue.enqueueMessage(firstQueue, second);
        enqueue.commitTran();

        reopenStore();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            assertArrayEquals(firstContent, copyContent(reader.getMessage(first.getMessageNumber())));
            assertArrayEquals(secondContent, copyContent(reader.getMessage(second.getMessageNumber())));
            assertEquals(List.of(first.getMessageNumber(), second.getMessageNumber()),
                    readQueueEntries(reader, firstQueue).stream()
                            .map(MessageEnqueueRecord::getMessageNumber).toList());
            assertEquals(List.of(second.getMessageNumber()),
                    readQueueEntries(reader, secondQueue).stream()
                            .map(MessageEnqueueRecord::getMessageNumber).toList());

            final Transaction dequeue = getStore().newTransaction();
            dequeue.dequeueMessage(firstEntry);
            dequeue.dequeueMessage(secondQueueEntry);
            dequeue.commitTran();

            final List<MessageEnqueueRecord> remaining = new ArrayList<>();
            reader.visitMessageInstances(remaining::add);
            assertEquals(1, remaining.size());
            assertEquals(firstQueue.getId(), remaining.get(0).getQueueId());
            assertEquals(second.getMessageNumber(), remaining.get(0).getMessageNumber());
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testQueueVisitorBuffersMessageIdsBeforeInvokingHandler()
    {
        final UUID queueId = new UUID(0L, 1L);
        final TransactionLogResource queue = createQueue(queueId);
        final int deliveryCount = 1025;
        final BDBMessageStore store = (BDBMessageStore) getStore();
        final EnvironmentFacade environment = store.getEnvironmentFacade();
        final Database deliveries = environment.openDatabase("QUEUE_ENTRIES", BDBUtils.DEFAULT_DATABASE_CONFIG);
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry value = new DatabaseEntry(new byte[0]);
        final com.sleepycat.je.Transaction writeTransaction = environment.beginTransaction(null);
        try
        {
            for (int messageId = deliveryCount; messageId > 0; messageId--)
            {
                QueueEntryBinding.objectToEntry(queueId, messageId, key);
                assertEquals(OperationStatus.SUCCESS, deliveries.put(writeTransaction, key, value));
            }
            QueueEntryBinding.objectToEntry(new UUID(0L, 2L), deliveryCount + 1L, key);
            assertEquals(OperationStatus.SUCCESS, deliveries.put(writeTransaction, key, value));
            writeTransaction.commit();
        }
        finally
        {
            if (writeTransaction.isValid())
            {
                writeTransaction.abort();
            }
        }

        reopenStore();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final List<MessageEnqueueRecord> records = new ArrayList<>();
            final List<MessageEnqueueRecord> nestedRecords = new ArrayList<>();
            reader.visitMessageInstances(queue, record ->
            {
                records.add(record);
                if (records.size() == 1)
                {
                    final Transaction dequeue = getStore().newTransaction();
                    dequeue.dequeueMessage(record);
                    dequeue.commitTran();
                    reader.visitMessageInstances(queue, nestedRecord ->
                    {
                        nestedRecords.add(nestedRecord);
                        return false;
                    });
                }
                return true;
            });

            assertEquals(deliveryCount, records.size());
            for (int i = 0; i < deliveryCount; i++)
            {
                assertSame(queueId, records.get(i).getQueueId());
                assertEquals(i + 1L, records.get(i).getMessageNumber());
            }
            assertEquals(1, nestedRecords.size());
            assertSame(queueId, nestedRecords.get(0).getQueueId());
            assertEquals(2L, nestedRecords.get(0).getMessageNumber());
            assertNotSame(records.get(1), nestedRecords.get(0));
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testQueueVisitorIncludesSignedMessageIdsInOrder()
    {
        final UUID queueId = new UUID(Long.MIN_VALUE, -2L);
        final UUID precedingQueueId = new UUID(Long.MIN_VALUE, -3L);
        final UUID followingQueueId = new UUID(Long.MIN_VALUE, 0L);
        final TransactionLogResource queue = createQueue(queueId);
        final TransactionLogResource emptyQueue = createQueue(new UUID(Long.MIN_VALUE, -1L));
        final DatabaseEntry extendedKey = legacyDeliveryKey(queueId, 1L);
        extendedKey.setData(Arrays.copyOf(extendedKey.getData(), QueueEntryBinding.KEY_SIZE + 1));
        writeDeliveryKeys(legacyDeliveryKey(followingQueueId, Long.MIN_VALUE), legacyDeliveryKey(queueId, 0L),
                          legacyDeliveryKey(queueId, Long.MAX_VALUE), legacyDeliveryKey(queueId, -1L),
                          legacyDeliveryKey(precedingQueueId, Long.MAX_VALUE), extendedKey,
                          legacyDeliveryKey(queueId, Long.MIN_VALUE));

        reopenStore();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final List<MessageEnqueueRecord> records = readQueueEntries(reader, queue);
            assertEquals(List.of(Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE),
                         records.stream().map(MessageEnqueueRecord::getMessageNumber).toList());
            assertTrue(readQueueEntries(reader, emptyQueue).isEmpty());
        }
        finally
        {
            reader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {15, 16, 23})
    void testQueueVisitorStopsAtMalformedNeighbor(final int keySize)
    {
        final UUID queueId = new UUID(0L, 0L);
        final UUID neighborId = new UUID(0L, 256L);
        final TransactionLogResource queue = createQueue(queueId);
        final TransactionLogResource emptyQueue = createQueue(new UUID(0L, 128L));
        final DatabaseEntry malformed = legacyDeliveryKey(neighborId, 0L);
        malformed.setSize(keySize);
        writeDeliveryKeys(legacyDeliveryKey(queueId, 1L), malformed, legacyDeliveryKey(neighborId, 2L));

        reopenStore();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            assertEquals(List.of(1L), readQueueEntries(reader, queue).stream()
                    .map(MessageEnqueueRecord::getMessageNumber).toList());
            assertTrue(readQueueEntries(reader, emptyQueue).isEmpty());
        }
        finally
        {
            reader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {16, 23})
    void testQueueVisitorRejectsTruncatedKeyForItsQueue(final int keySize)
    {
        final UUID queueId = new UUID(0L, 256L);
        final TransactionLogResource queue = createQueue(queueId);
        final DatabaseEntry malformed = legacyDeliveryKey(queueId, 0L);
        malformed.setSize(keySize);
        writeDeliveryKeys(legacyDeliveryKey(queueId, -1L), malformed);

        reopenStore();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final List<MessageEnqueueRecord> visited = new ArrayList<>();
            final StoreException failure = assertThrows(StoreException.class,
                    () -> reader.visitMessageInstances(queue, visited::add));
            assertTrue(failure.getCause() instanceof IllegalArgumentException);
            assertTrue(visited.isEmpty());
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    public void testGlobalVisitorReusesQueueUuidAfterReopen()
    {
        final TransactionLogResource firstQueue = createQueue(new UUID(0L, 0L));
        final TransactionLogResource secondQueue = createQueue(new UUID(0L, 1L));
        final TransactionLogResource thirdQueue = createQueue(new UUID(1L, 1L));
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final InternalMessage first = InternalMessage.createStringMessage(getStore(), header, "first", true);
        final InternalMessage second = InternalMessage.createStringMessage(getStore(), header, "second", true);
        final Transaction transaction = getStore().newTransaction();
        transaction.enqueueMessage(thirdQueue, first);
        transaction.enqueueMessage(secondQueue, first);
        transaction.enqueueMessage(firstQueue, second);
        transaction.enqueueMessage(firstQueue, first);
        transaction.commitTran();

        reopenStore();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final List<MessageEnqueueRecord> records = new ArrayList<>();
            final List<MessageEnqueueRecord> nestedRecords = new ArrayList<>();
            reader.visitMessageInstances(record ->
            {
                records.add(record);
                if (records.size() == 1)
                {
                    reader.visitMessageInstances(nestedRecord ->
                    {
                        nestedRecords.add(nestedRecord);
                        return false;
                    });
                }
                return true;
            });

            assertEquals(List.of(firstQueue.getId(), firstQueue.getId(), secondQueue.getId(), thirdQueue.getId()),
                    records.stream().map(MessageEnqueueRecord::getQueueId).toList());
            assertEquals(List.of(first.getMessageNumber(), second.getMessageNumber(), first.getMessageNumber(),
                                 first.getMessageNumber()),
                    records.stream().map(MessageEnqueueRecord::getMessageNumber).toList());
            assertSame(records.get(0).getQueueId(), records.get(1).getQueueId());
            assertNotSame(records.get(0), records.get(1));
            assertEquals(1, nestedRecords.size());
            assertEquals(firstQueue.getId(), nestedRecords.get(0).getQueueId());
            assertNotSame(records.get(0).getQueueId(), nestedRecords.get(0).getQueueId());
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    void testRecoveryVisitorUsesOneCursorWhenSnapshotFits()
    {
        final EnvironmentFacade environment = mock(EnvironmentFacade.class);
        final Database deliveries = mock(Database.class);
        final Cursor cursor = mock(Cursor.class);
        when(environment.openDatabase(eq("QUEUE_ENTRIES"), any())).thenReturn(deliveries);
        when(deliveries.openCursor(null, null)).thenReturn(cursor);

        final UUID firstQueue = new UUID(Long.MIN_VALUE, 1L);
        final UUID secondQueue = new UUID(1L, Long.MIN_VALUE);
        final int firstQueueCount = SINGLE_BLOCK_DELIVERIES + 1;
        final int count = firstQueueCount + 2;
        final AtomicInteger next = new AtomicInteger();
        when(cursor.getNext(any(), isNull(), eq(LockMode.READ_UNCOMMITTED))).thenAnswer(invocation ->
        {
            final int index = next.getAndIncrement();
            if (index >= count)
            {
                return OperationStatus.NOTFOUND;
            }
            final DatabaseEntry found = index < firstQueueCount ?
                    legacyDeliveryKey(firstQueue, index - 500L) :
                    legacyDeliveryKey(secondQueue, index - firstQueueCount - 1L);
            final DatabaseEntry key = invocation.getArgument(0);
            key.setData(found.getData(), found.getOffset(), found.getSize());
            return OperationStatus.SUCCESS;
        });

        final BDBMessageStore store = new BDBMessageStore(parent -> environment);
        store.openMessageStore(mock(BDBVirtualHost.class));
        final BDBMessageStoreReader reader = (BDBMessageStoreReader) store.newMessageStoreReader();
        final List<MessageEnqueueRecord> visited = new ArrayList<>();
        try
        {
            reader.visitMessageInstancesForRecovery(record ->
            {
                if (visited.isEmpty())
                {
                    verify(cursor).close();
                }
                return visited.add(record);
            }, 2);
        }
        finally
        {
            reader.close();
            store.closeMessageStore();
        }

        verify(deliveries).openCursor(null, null);
        assertEquals(count, visited.size());
        for (int index = 0; index < firstQueueCount; index++)
        {
            assertEquals(firstQueue, visited.get(index).getQueueId());
            assertEquals(index - 500L, visited.get(index).getMessageNumber());
        }
        assertSame(visited.get(0).getQueueId(), visited.get(firstQueueCount - 1).getQueueId());
        assertEquals(secondQueue, visited.get(firstQueueCount).getQueueId());
        assertEquals(secondQueue, visited.get(firstQueueCount + 1).getQueueId());
        assertEquals(-1L, visited.get(firstQueueCount).getMessageNumber());
        assertEquals(0L, visited.get(firstQueueCount + 1).getMessageNumber());
    }

    @Test
    void testRecoveryVisitorPreservesDeltasAndFullWidthMessageIds()
    {
        final UUID queueId = new UUID(Long.MIN_VALUE, -1L);
        final List<Long> expected = new ArrayList<>();
        long messageId = Long.MIN_VALUE;
        expected.add(messageId);
        for (int bits = 7; bits <= 56; bits += 7)
        {
            // Exercise both sides of each encoded length boundary for delta + 1.
            messageId += (1L << bits) - 2;
            expected.add(messageId);
            messageId += (1L << bits) - 1;
            expected.add(messageId);
        }
        // This gap also exceeds the range of a signed subtraction.
        expected.add(Long.MAX_VALUE - 1);
        expected.add(Long.MAX_VALUE);

        final DatabaseEntry[] keys = new DatabaseEntry[expected.size() + 1];
        for (int index = 0; index < expected.size(); index++)
        {
            keys[index] = legacyDeliveryKey(queueId, expected.get(index));
        }
        keys[expected.size()] = new DatabaseEntry(Arrays.copyOf(keys[0].getData(), QueueEntryBinding.KEY_SIZE + 1));
        expected.add(1, Long.MIN_VALUE);
        writeDeliveryKeys(keys);
        reopenStore();

        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final List<Long> visited = new ArrayList<>();
            reader.visitMessageInstancesForRecovery(record ->
            {
                assertEquals(queueId, record.getQueueId());
                return visited.add(record.getMessageNumber());
            });
            assertEquals(expected, visited);
        }
        finally
        {
            reader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, SINGLE_BLOCK_DELIVERIES - 1, SINGLE_BLOCK_DELIVERIES, SINGLE_BLOCK_DELIVERIES + 1})
    void testRecoveryVisitorPreservesSignedOrderAtSnapshotBoundaries(final int count)
    {
        final UUID queueId = new UUID(Long.MIN_VALUE, 0L);
        final long firstId = -500L;
        writeDeliveryRange(queueId, firstId, count);
        reopenStore();

        final BDBMessageStoreReader reader = (BDBMessageStoreReader) getStore().newMessageStoreReader();
        try
        {
            final List<Long> visited = new ArrayList<>();
            reader.visitMessageInstancesForRecovery(record ->
            {
                assertTrue(visited.size() < count);
                assertEquals(queueId, record.getQueueId());
                return visited.add(record.getMessageNumber());
            }, 1);
            assertEquals(LongStream.range(firstId, firstId + count).boxed().toList(), visited);
        }
        finally
        {
            reader.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"1, false", "2, true"})
    void testRecoveryVisitorResumesWhenCallbacksDeleteDeliveryKeys(final int blockLimit,
                                                                  final boolean laterKeyWasBuffered)
    {
        final UUID queueId = new UUID(0L, 1L);
        final long removedFromNextPage = SINGLE_BLOCK_DELIVERIES + 501L;
        // One deleted delivery shifts the next page boundary by one ID.
        final long pageResumeId = SINGLE_BLOCK_DELIVERIES + 1001L;
        final int count = SINGLE_BLOCK_DELIVERIES + 1002;
        writeDeliveryRange(queueId, 1L, count);
        reopenStore();

        final BDBMessageStoreReader reader = (BDBMessageStoreReader) getStore().newMessageStoreReader();
        try
        {
            final List<Long> visited = new ArrayList<>();
            reader.visitMessageInstancesForRecovery(record ->
            {
                final long messageId = record.getMessageNumber();
                assertTrue(visited.size() < count);
                visited.add(messageId);
                if (messageId == 1L)
                {
                    final Transaction transaction = getStore().newTransaction();
                    transaction.dequeueMessage(
                            new AbstractBDBMessageStore.BDBEnqueueRecord(queueId, removedFromNextPage));
                    transaction.commitTran();
                }
                else if (messageId == SINGLE_BLOCK_DELIVERIES || messageId == pageResumeId)
                {
                    final Transaction transaction = getStore().newTransaction();
                    transaction.dequeueMessage(record);
                    transaction.commitTran();
                }
                return true;
            }, blockLimit);
            // A buffered delivery has the same snapshot semantics as the legacy bulk reader.
            final List<Long> expected = LongStream.rangeClosed(1L, count)
                    .filter(messageId -> laterKeyWasBuffered || messageId != removedFromNextPage).boxed().toList();
            assertEquals(expected, visited);
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    void testRecoveryVisitorResumesWithCompleteRawKey()
    {
        final UUID queueId = new UUID(0L, 1L);
        writeDeliveryRange(queueId, 1L, SINGLE_BLOCK_DELIVERIES - 1);
        final DatabaseEntry resumeKey = legacyDeliveryKey(queueId, SINGLE_BLOCK_DELIVERIES);
        final byte[] firstExtendedKey = Arrays.copyOf(resumeKey.getData(), QueueEntryBinding.KEY_SIZE + 1);
        final byte[] secondExtendedKey = firstExtendedKey.clone();
        secondExtendedKey[QueueEntryBinding.KEY_SIZE] = 1;
        writeDeliveryKeys(new DatabaseEntry(firstExtendedKey), new DatabaseEntry(secondExtendedKey),
                          legacyDeliveryKey(queueId, SINGLE_BLOCK_DELIVERIES + 1L),
                          legacyDeliveryKey(new UUID(0L, 2L), Long.MIN_VALUE));
        reopenStore();

        final BDBMessageStoreReader reader = (BDBMessageStoreReader) getStore().newMessageStoreReader();
        try
        {
            final List<MessageEnqueueRecord> legacy = new ArrayList<>();
            reader.visitMessageInstances(legacy::add);
            final List<MessageEnqueueRecord> paged = new ArrayList<>();
            reader.visitMessageInstancesForRecovery(record ->
            {
                assertTrue(paged.size() < legacy.size());
                return paged.add(record);
            }, 1);
            assertEquals(SINGLE_BLOCK_DELIVERIES + 3, legacy.size());
            assertEquals(legacy.stream().map(MessageEnqueueRecord::getMessageNumber).toList(),
                         paged.stream().map(MessageEnqueueRecord::getMessageNumber).toList());
            assertEquals(legacy.stream().map(MessageEnqueueRecord::getQueueId).toList(),
                         paged.stream().map(MessageEnqueueRecord::getQueueId).toList());
            assertSame(paged.get(SINGLE_BLOCK_DELIVERIES - 1).getQueueId(),
                       paged.get(SINGLE_BLOCK_DELIVERIES).getQueueId());
        }
        finally
        {
            reader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testRecoveryVisitorValidatesLaterKeysBeforeCallbacks(final int blockLimit)
    {
        final UUID queueId = new UUID(0L, 1L);
        writeDeliveryRange(queueId, 1L, SINGLE_BLOCK_DELIVERIES + 1);
        final DatabaseEntry corrupt = legacyDeliveryKey(queueId, Long.MAX_VALUE);
        corrupt.setSize(QueueEntryBinding.KEY_SIZE - 1);
        writeDeliveryKeys(corrupt);

        final BDBMessageStoreReader reader = (BDBMessageStoreReader) getStore().newMessageStoreReader();
        try
        {
            final List<MessageEnqueueRecord> visited = new ArrayList<>();
            final StoreException failure = assertThrows(StoreException.class,
                    () -> reader.visitMessageInstancesForRecovery(record ->
                    {
                        visited.add(record);
                        return false;
                    }, blockLimit));
            assertTrue(failure.getCause() instanceof IllegalArgumentException);
            assertTrue(visited.isEmpty());
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    void testRecoveryVisitorHonorsEarlyStop()
    {
        writeDeliveryRange(new UUID(0L, 1L), 1L, 1001);
        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            final List<Long> visited = new ArrayList<>();
            reader.visitMessageInstancesForRecovery(record ->
            {
                visited.add(record.getMessageNumber());
                return false;
            });
            assertEquals(List.of(1L), visited);
        }
        finally
        {
            reader.close();
        }
    }

    @Test
    void testRecoveryVisitorPreservesCallbackFailure()
    {
        writeDeliveryRange(new UUID(0L, 1L), 1L, 1);
        final RuntimeException failure = new IllegalArgumentException("Recovery callback failed");
        final MessageStore.MessageStoreReader reader = getStore().newMessageStoreReader();
        try
        {
            assertSame(failure, assertThrows(RuntimeException.class,
                    () -> reader.visitMessageInstancesForRecovery(record ->
                    {
                        throw failure;
                    })));
        }
        finally
        {
            reader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRecoveryVisitorStopsAfterStoreCloses(final boolean reopenWithSharedEnvironment)
    {
        writeDeliveryRange(new UUID(0L, 1L), 1L, 1001);
        final BDBMessageStore backingStore = (BDBMessageStore) getStore();
        final ConfiguredObject<?> parent = backingStore.getParent();
        final MessageStore store;
        if (reopenWithSharedEnvironment)
        {
            final BDBConfigurationStore configurationStore = new BDBConfigurationStore(VirtualHost.class);
            store = configurationStore.new ProvidedBDBMessageStore()
            {
                @Override
                public EnvironmentFacade getEnvironmentFacade()
                {
                    return backingStore.getEnvironmentFacade();
                }
            };
            store.openMessageStore(parent);
        }
        else
        {
            store = backingStore;
        }

        final MessageStore.MessageStoreReader reader = store.newMessageStoreReader();
        try
        {
            final AtomicInteger callbacks = new AtomicInteger();
            final IllegalStateException failure =
                    assertThrows(IllegalStateException.class, () -> reader.visitMessageInstancesForRecovery(record ->
                    {
                        if (callbacks.incrementAndGet() == 1000)
                        {
                            store.closeMessageStore();
                            if (reopenWithSharedEnvironment)
                            {
                                store.openMessageStore(parent);
                            }
                        }
                        return true;
                    }));
            assertEquals(1000, callbacks.get());
            assertEquals(reopenWithSharedEnvironment ? "Message store changed during delivery recovery"
                                                    : "Message store is not open", failure.getMessage());
        }
        finally
        {
            reader.close();
            store.closeMessageStore();
        }
    }

    private void writeDeliveryRange(final UUID queueId, final long firstMessageId, final int count)
    {
        final DatabaseEntry[] keys = new DatabaseEntry[count];
        for (int i = 0; i < keys.length; i++)
        {
            keys[i] = legacyDeliveryKey(queueId, firstMessageId + i);
        }
        writeDeliveryKeys(keys);
    }

    @Test
    public void testReuseMessageKeyAfterNestedMetadataLookup()
    {
        final MessageStore store = getStore();
        final MessageStore.MessageStoreReader reader = store.newMessageStoreReader();
        try
        {
            final TestMessageMetaData metaData = new TestMessageMetaData(0L, 0)
            {
                @Override
                public void writeToBuffer(final QpidByteBuffer destination)
                {
                    assertNull(reader.getMessage(-1L));
                    super.writeToBuffer(destination);
                }
            };
            final StoredMessage<TestMessageMetaData> message = store.addMessage(metaData).allContentAdded();

            assertTrue(message.flowToDisk());

            assertNotNull(reader.getMessage(message.getMessageNumber()));
            assertNull(reader.getMessage(-1L));
        }
        finally
        {
            reader.close();
        }
    }

    private void writeDeliveryKeys(final DatabaseEntry... keys)
    {
        final EnvironmentFacade environment = ((BDBMessageStore) getStore()).getEnvironmentFacade();
        final Database deliveries = environment.openDatabase("QUEUE_ENTRIES", BDBUtils.DEFAULT_DATABASE_CONFIG);
        final DatabaseEntry value = new DatabaseEntry(new byte[0]);
        final com.sleepycat.je.Transaction transaction = environment.beginTransaction(null);
        try
        {
            for (final DatabaseEntry key : keys)
            {
                assertEquals(OperationStatus.SUCCESS, deliveries.put(transaction, key, value));
            }
            transaction.commit();
        }
        finally
        {
            if (transaction.isValid())
            {
                transaction.abort();
            }
        }
    }

    private static DatabaseEntry legacyDeliveryKey(final UUID queueId, final long messageId)
    {
        final TupleOutput output = new TupleOutput();
        output.writeLong(queueId.getMostSignificantBits());
        output.writeLong(queueId.getLeastSignificantBits());
        output.writeLong(messageId);
        return new DatabaseEntry(output.toByteArray());
    }

    private static void writeLegacyEntries(final EnvironmentFacade environment, final UUID queueId,
                                           final long messageId)
    {
        final DatabaseEntry messageKey = new DatabaseEntry();
        LongBinding.longToEntry(messageId, messageKey);
        final DatabaseEntry deliveryKey = legacyDeliveryKey(queueId, messageId);
        final DatabaseEntry metaData = new DatabaseEntry();
        MessageMetaDataBinding.getInstance()
                .objectToEntry(new TestMessageMetaData(messageId, CONTENT_BYTES.length), metaData);

        final com.sleepycat.je.Transaction transaction = environment.beginTransaction(null);
        try
        {
            assertEquals(OperationStatus.SUCCESS,
                    environment.openDatabase("MESSAGE_METADATA", BDBUtils.DEFAULT_DATABASE_CONFIG)
                            .put(transaction, messageKey, metaData));
            assertEquals(OperationStatus.SUCCESS,
                    environment.openDatabase("MESSAGE_CONTENT", BDBUtils.DEFAULT_DATABASE_CONFIG)
                            .put(transaction, messageKey, new DatabaseEntry(CONTENT_BYTES)));
            assertEquals(OperationStatus.SUCCESS,
                    environment.openDatabase("QUEUE_ENTRIES", BDBUtils.DEFAULT_DATABASE_CONFIG)
                            .put(transaction, deliveryKey, new DatabaseEntry(new byte[0])));
            transaction.commit();
        }
        finally
        {
            if (transaction.isValid())
            {
                transaction.abort();
            }
        }
    }

    private static TransactionLogResource createQueue(final UUID queueId)
    {
        final TransactionLogResource queue = mock(TransactionLogResource.class);
        when(queue.getId()).thenReturn(queueId);
        return queue;
    }

    private static List<MessageEnqueueRecord> readQueueEntries(final MessageStore.MessageStoreReader reader,
                                                              final TransactionLogResource queue)
    {
        final List<MessageEnqueueRecord> records = new ArrayList<>();
        reader.visitMessageInstances(queue, records::add);
        return records;
    }

    private static byte[] copyContent(final StoredMessage<?> message)
    {
        assertNotNull(message);
        try (final QpidByteBuffer content = message.getContent(0, message.getContentSize()))
        {
            final byte[] bytes = new byte[content.remaining()];
            content.copyTo(bytes);
            return bytes;
        }
    }

    private StoredMessage<MessageMetaData> createAndStoreSingleChunkMessage_0_8(MessageStore store)
    {
        QpidByteBuffer chunk1 = QpidByteBuffer.wrap(CONTENT_BYTES);

        int bodySize = CONTENT_BYTES.length;

        //create and store the message using the MessageStore interface
        MessagePublishInfo pubInfoBody_0_8 = createPublishInfoBody_0_8();
        BasicContentHeaderProperties props_0_8 = createContentHeaderProperties_0_8();

        ContentHeaderBody chb_0_8 = createContentHeaderBody_0_8(props_0_8, bodySize);

        MessageMetaData messageMetaData_0_8 = new MessageMetaData(pubInfoBody_0_8, chb_0_8);
        MessageHandle<MessageMetaData> storedMessage_0_8 = store.addMessage(messageMetaData_0_8);

        storedMessage_0_8.addContent(chunk1);
        ((AbstractBDBMessageStore.StoredBDBMessage)storedMessage_0_8).flushToStore();

        return storedMessage_0_8.allContentAdded();
    }

    @Test
    public void testOnDelete()
    {
        String storeLocation = getStore().getStoreLocation();

        File location = new File(storeLocation);
        assertTrue(location.exists(), "Store does not exist at " + storeLocation);

        getStore().closeMessageStore();
        assertTrue(location.exists(), "Store does not exist at " + storeLocation);

        BDBVirtualHost mockVH = mock(BDBVirtualHost.class);
        String testLocation = getStore().getStoreLocation();
        when(mockVH.getStorePath()).thenReturn(testLocation);

        getStore().onDelete(mockVH);

        assertFalse(location.exists(), "Store exists at " + storeLocation);
    }


    @Override
    protected VirtualHost createVirtualHost()
    {
        _storeLocation = TMP_FOLDER + File.separator + getTestName();
        deleteStoreIfExists();

        final BDBVirtualHost parent = mock(BDBVirtualHost.class);
        when(parent.getStorePath()).thenReturn(_storeLocation);
        return parent;
    }

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
    public void testGetNextMessgaeIdWithLockException(){
        EnvironmentFacadeFactory eff = mock(EnvironmentFacadeFactory.class);
        EnvironmentFacade ef = mock(EnvironmentFacade.class);
        doThrow(LockTimeoutException.class).when(ef).openSequence(any(),any(),any());
        when(eff.createEnvironmentFacade(any())).thenReturn(ef);

        BDBMessageStore store = new BDBMessageStore (eff);
        store.openMessageStore(getVirtualHost());

        try
        {
            store.addMessage(new InternalMessageMetaData(false, null, 0));
            fail("exception is expected");
        }
        catch(ConnectionScopedRuntimeException e)
        {
            assertEquals("Unexpected exception on BDB sequence", e.getMessage());
        }

    }
}
