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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.OperationStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.model.VirtualHost;
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
import org.apache.qpid.server.store.berkeleydb.tuple.MessageMetaDataBinding;
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

    private static void writeLegacyEntries(final EnvironmentFacade environment, final UUID queueId,
                                           final long messageId)
    {
        final DatabaseEntry messageKey = new DatabaseEntry();
        LongBinding.longToEntry(messageId, messageKey);
        final TupleOutput deliveryKey = new TupleOutput();
        deliveryKey.writeLong(queueId.getMostSignificantBits());
        deliveryKey.writeLong(queueId.getLeastSignificantBits());
        deliveryKey.writeLong(messageId);
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
                            .put(transaction, new DatabaseEntry(deliveryKey.toByteArray()),
                                    new DatabaseEntry(new byte[0])));
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
