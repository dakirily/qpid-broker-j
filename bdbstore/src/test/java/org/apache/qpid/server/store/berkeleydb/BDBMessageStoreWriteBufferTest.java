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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.store.berkeleydb.AbstractBDBMessageStore.StoredBDBMessage;
import org.apache.qpid.server.store.berkeleydb.tuple.MessageMetaDataBinding;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

class BDBMessageStoreWriteBufferTest extends UnitTestBase
{
    private static final String CONTENT_DATABASE_NAME = "MESSAGE_CONTENT";
    private static final String METADATA_DATABASE_NAME = "MESSAGE_METADATA";
    private static final int METADATA_HEADER_SIZE = Integer.BYTES + Byte.BYTES;

    private BDBMessageStore _store;

    @AfterEach
    void tearDown()
    {
        if (_store != null)
        {
            _store.closeMessageStore();
        }
    }

    @Test
    void testJeCopiesReusedBodiesAndPersistsMetadata(@TempDir final Path directory)
    {
        final int capacity = BDBWriteBuffer.MAX_RETAINED_CAPACITY;
        final List<byte[]> bodies = List.of(bytes(capacity, (byte) 11), bytes(capacity - 1, (byte) 12),
                                            new byte[] {4, 5, 6}, new byte[0], bytes(capacity + 1, (byte) 13),
                                            new byte[] {7, 8});
        final int metadataCapacity = capacity - METADATA_HEADER_SIZE;
        final List<byte[]> payloads = List.of(bytes(metadataCapacity, (byte) 21),
                bytes(metadataCapacity - 1, (byte) 22), new byte[] {9, 10}, new byte[0],
                bytes(metadataCapacity + 1, (byte) 23), new byte[] {6});
        final List<byte[]> metadataEntries = new ArrayList<>();
        final EnvironmentConfig environmentConfig = new EnvironmentConfig().setAllowCreate(true).setTransactional(true);
        environmentConfig.setCacheSize(8 * 1024 * 1024);
        final DatabaseConfig databaseConfig = new DatabaseConfig().setAllowCreate(true).setTransactional(true);
        try (final Environment environment = new Environment(directory.toFile(), environmentConfig);
             final Database metadata = environment.openDatabase(null, METADATA_DATABASE_NAME, databaseConfig);
             final Database content = environment.openDatabase(null, CONTENT_DATABASE_NAME, databaseConfig))
        {
            openStore(metadata, content);
            final Transaction transaction = environment.beginTransaction(null, null);
            try
            {
                for (int index = 0; index < bodies.size(); index++)
                {
                    final byte[] body = bodies.get(index);
                    final TestMetaData fixture = new TestMetaData(body.length, payloads.get(index));
                    final DatabaseEntry ordinary = new DatabaseEntry();
                    MessageMetaDataBinding.getInstance().objectToEntry(fixture, ordinary);
                    metadataEntries.add(entryBytes(ordinary));
                    writeMessage(transaction, index + 1L, fixture, body);
                }
                // Reuse the same buffer while all of the previous writes are still uncommitted.
                try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
                {
                    buffer.prepare(-1L, capacity);
                    Arrays.fill(buffer.getValue().getData(), (byte) 99);
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

        environmentConfig.setAllowCreate(false);
        databaseConfig.setAllowCreate(false);
        try (final Environment environment = new Environment(directory.toFile(), environmentConfig);
             final Database metadata = environment.openDatabase(null, METADATA_DATABASE_NAME, databaseConfig);
             final Database content = environment.openDatabase(null, CONTENT_DATABASE_NAME, databaseConfig))
        {
            for (int index = 0; index < bodies.size(); index++)
            {
                assertArrayEquals(bodies.get(index), readValue(content, index + 1L));
                assertArrayEquals(metadataEntries.get(index), readValue(metadata, index + 1L));
            }
        }
    }

    @Test
    void testRetainedMetadataOutputSurvivesFollowingWrites()
    {
        final Database metadata = mock(Database.class);
        final Database content = mock(Database.class);
        openStore(metadata, content);
        final Transaction transaction = mock(Transaction.class);
        when(metadata.put(same(transaction), any(), any())).thenReturn(OperationStatus.SUCCESS);
        when(content.put(same(transaction), any(), any())).thenReturn(OperationStatus.SUCCESS);
        final byte[] payload = bytes(8, (byte) 1);
        final byte[] body = bytes(10, (byte) 2);
        final AtomicReference<QpidByteBuffer> retained = new AtomicReference<>();
        final TestMetaData fixture = new TestMetaData(body.length, payload)
        {
            @Override
            public void writeToBuffer(final QpidByteBuffer destination)
            {
                retained.set(destination.duplicate());
                super.writeToBuffer(destination);
            }
        };

        final StoredBDBMessage<InternalMessageMetaData> message = createMessage(10L, fixture, body);
        try
        {
            message.store(transaction);
            final byte[] encoded = new byte[payload.length];
            retained.get().copyTo(encoded);
            assertArrayEquals(payload, encoded, "The message body write must preserve the retained encoding");

            writeMessage(transaction, 20L, new TestMetaData(0, bytes(payload.length, (byte) 3)), new byte[0]);
            retained.get().copyTo(encoded);
            assertArrayEquals(payload, encoded, "The next metadata write must preserve the retained encoding");
        }
        finally
        {
            try (final QpidByteBuffer encoding = retained.get())
            {
                message.clear(true);
            }
        }
    }

    @Test
    void testNestedContentPutKeepsOuterKeyAndBody()
    {
        final Database content = mock(Database.class);
        openStore(mock(Database.class), content);
        final Transaction transaction = mock(Transaction.class);
        final byte[] outerBody = {1, 2, 3};
        final byte[] innerBody = {4, 5, 6};
        final List<Long> writeOrder = new ArrayList<>();
        when(content.put(same(transaction), any(), any())).thenAnswer(invocation ->
        {
            final DatabaseEntry key = invocation.getArgument(1);
            final DatabaseEntry value = invocation.getArgument(2);
            final long messageId = LongBinding.entryToLong(key);
            if (messageId == 10L)
            {
                writeBody(transaction, 20L, innerBody);
                assertEquals(10L, LongBinding.entryToLong(key));
                assertArrayEquals(outerBody, entryBytes(value));
            }
            else
            {
                assertEquals(20L, messageId);
                assertArrayEquals(innerBody, entryBytes(value));
            }
            writeOrder.add(messageId);
            return OperationStatus.SUCCESS;
        });

        writeBody(transaction, 10L, outerBody);

        assertEquals(List.of(20L, 10L), writeOrder);
    }

    @Test
    void testNestedMetadataSerializationKeepsOuterKeyAndPayload()
    {
        final Database metadata = mock(Database.class);
        final Database content = mock(Database.class);
        openStore(metadata, content);
        final Transaction transaction = mock(Transaction.class);
        when(content.put(same(transaction), any(), any())).thenReturn(OperationStatus.SUCCESS);
        final List<MetadataWrite> writes = new ArrayList<>();
        when(metadata.put(same(transaction), any(), any())).thenAnswer(invocation ->
        {
            writes.add(captureMetadata(invocation.getArgument(1), invocation.getArgument(2)));
            return OperationStatus.SUCCESS;
        });
        final TestMetaData outer = new TestMetaData(1, new byte[] {1, 2})
        {
            @Override
            public void writeToBuffer(final QpidByteBuffer destination)
            {
                destination.put((byte) 1);
                writeMessage(transaction, 20L, new TestMetaData(1, new byte[] {3, 4}), new byte[] {5});
                destination.put((byte) 2);
            }
        };

        writeMessage(transaction, 10L, outer, new byte[] {6});

        assertEquals(List.of(20L, 10L), writes.stream().map(MetadataWrite::messageId).toList());
        assertNotSame(writes.get(0).array(), writes.get(1).array());
        assertArrayEquals(new byte[] {3, 4}, metadataPayload(writes.get(0)));
        assertArrayEquals(new byte[] {1, 2}, metadataPayload(writes.get(1)));
    }

    @Test
    void testRetryKeepsSerializedMetadataWhileNestedWritesComplete()
    {
        final Database metadata = mock(Database.class);
        final Database content = mock(Database.class);
        openStore(metadata, content);
        final Transaction transaction = mock(Transaction.class);
        when(content.put(same(transaction), any(), any())).thenReturn(OperationStatus.SUCCESS);
        final AtomicInteger outerAttempts = new AtomicInteger();
        final List<MetadataWrite> writes = new ArrayList<>();
        when(metadata.put(same(transaction), any(), any())).thenAnswer(invocation ->
        {
            final DatabaseEntry key = invocation.getArgument(1);
            final DatabaseEntry value = invocation.getArgument(2);
            final MetadataWrite write = captureMetadata(key, value);
            writes.add(write);
            if (write.messageId() == 10L && outerAttempts.getAndIncrement() == 0)
            {
                writeMessage(transaction, 20L, new TestMetaData(1, new byte[] {9}), new byte[] {8});
                assertEquals(10L, LongBinding.entryToLong(key));
                assertArrayEquals(write.bytes(), entryBytes(value));
                throw mock(LockTimeoutException.class);
            }
            return OperationStatus.SUCCESS;
        });
        final AtomicInteger serializations = new AtomicInteger();
        final TestMetaData outer = new TestMetaData(1, new byte[] {1, 2})
        {
            @Override
            public void writeToBuffer(final QpidByteBuffer destination)
            {
                serializations.incrementAndGet();
                super.writeToBuffer(destination);
            }
        };

        writeMessage(transaction, 10L, outer, new byte[] {3});

        assertEquals(1, serializations.get());
        assertEquals(List.of(10L, 20L, 10L), writes.stream().map(MetadataWrite::messageId).toList());
        final MetadataWrite first = writes.get(0);
        final MetadataWrite retry = writes.get(2);
        assertSame(first.entry(), retry.entry());
        assertSame(first.array(), retry.array());
        assertNotSame(first.array(), writes.get(1).array());
        assertArrayEquals(first.bytes(), retry.bytes());
        assertArrayEquals(new byte[] {1, 2}, metadataPayload(retry));
    }

    @Test
    void testOversizedMetadataSerializationFailureReleasesBuffer()
    {
        final Database metadata = mock(Database.class);
        openStore(metadata, mock(Database.class));
        final Transaction transaction = mock(Transaction.class);
        final BDBWriteBuffer cached = cachedWriteBuffer();
        final IllegalStateException failure = new IllegalStateException("Synthetic serialization failure");
        final int payloadSize = BDBWriteBuffer.MAX_RETAINED_CAPACITY;
        final TestMetaData fixture = new TestMetaData(0, new byte[payloadSize])
        {
            @Override
            public void writeToBuffer(final QpidByteBuffer destination)
            {
                assertEquals(payloadSize, destination.remaining());
                throw failure;
            }
        };

        assertSame(failure, assertThrows(IllegalStateException.class, () ->
                writeMessage(transaction, 10L, fixture, new byte[0])));
        verify(metadata, never()).put(any(), any(), any());
        assertWriteBufferReleased(cached);
    }

    @Test
    void testOversizedMetadataPutFailureReleasesBufferForNextWrite()
    {
        final Database metadata = mock(Database.class);
        final Database content = mock(Database.class);
        openStore(metadata, content);
        final Transaction failedTransaction = mock(Transaction.class);
        final Transaction nextTransaction = mock(Transaction.class);
        final BDBWriteBuffer cached = cachedWriteBuffer();
        final IllegalStateException failure = new IllegalStateException("Synthetic metadata put failure");
        final byte[] oversized = bytes(BDBWriteBuffer.MAX_RETAINED_CAPACITY, (byte) 7);
        when(metadata.put(same(failedTransaction), any(), any())).thenAnswer(invocation ->
        {
            final MetadataWrite write = captureMetadata(invocation.getArgument(1), invocation.getArgument(2));
            assertSame(cached.getValue(), write.entry());
            assertArrayEquals(oversized, metadataPayload(write));
            throw failure;
        });
        when(metadata.put(same(nextTransaction), any(), any())).thenAnswer(invocation ->
        {
            final MetadataWrite write = captureMetadata(invocation.getArgument(1), invocation.getArgument(2));
            assertEquals(20L, write.messageId());
            assertSame(cached.getValue(), write.entry());
            assertArrayEquals(new byte[] {9}, metadataPayload(write));
            return OperationStatus.SUCCESS;
        });
        when(content.put(same(nextTransaction), any(), any())).thenReturn(OperationStatus.SUCCESS);

        assertSame(failure, assertThrows(IllegalStateException.class, () ->
                writeMessage(failedTransaction, 10L, new TestMetaData(0, oversized), new byte[0])));
        assertWriteBufferReleased(cached);
        writeMessage(nextTransaction, 20L, new TestMetaData(1, new byte[] {9}), new byte[] {8});
        verify(metadata).put(same(nextTransaction), any(), any());
    }

    @Test
    void testFailedOversizedContentPutReleasesBufferForRetry()
    {
        final Database content = mock(Database.class);
        openStore(mock(Database.class), content);
        final Transaction failedTransaction = mock(Transaction.class);
        final Transaction retryTransaction = mock(Transaction.class);
        final byte[] body = bytes(BDBWriteBuffer.MAX_RETAINED_CAPACITY + 1, (byte) 14);
        final BDBWriteBuffer cached = cachedWriteBuffer();
        final RuntimeException writeFailure = new IllegalStateException("Synthetic content put failure");
        doAnswer(invocation ->
        {
            final DatabaseEntry value = invocation.getArgument(2);
            assertSame(cached.getValue(), value);
            assertArrayEquals(body, entryBytes(value));
            throw writeFailure;
        }).when(content).put(same(failedTransaction), any(), any());
        when(content.put(same(retryTransaction), any(), any())).thenAnswer(invocation ->
        {
            assertEquals(10L, LongBinding.entryToLong(invocation.getArgument(1)));
            final DatabaseEntry value = invocation.getArgument(2);
            assertSame(cached.getValue(), value);
            assertArrayEquals(body, entryBytes(value));
            return OperationStatus.SUCCESS;
        });

        final InternalMessageMetaData metadata = new InternalMessageMetaData(true, null, body.length);
        final StoredBDBMessage<InternalMessageMetaData> message = createMessage(10L, metadata, body);
        try
        {
            assertSame(writeFailure, assertThrows(RuntimeException.class, () -> message.store(failedTransaction)));
            assertWriteBufferReleased(cached);
            message.store(retryTransaction);
            verify(content).put(same(retryTransaction), any(), any());
        }
        finally
        {
            message.clear(true);
        }
    }

    private void openStore(final Database metadata, final Database content)
    {
        final EnvironmentFacade facade = mock(EnvironmentFacade.class);
        final Map<String, Database> databases = Map.of(METADATA_DATABASE_NAME, metadata,
                                                      CONTENT_DATABASE_NAME, content);
        when(facade.openDatabase(anyString(), any()))
                .thenAnswer(invocation -> databases.get(invocation.getArgument(0)));
        when(facade.handleDatabaseException(anyString(), any(RuntimeException.class)))
                .thenAnswer(invocation -> invocation.getArgument(1));
        _store = new BDBMessageStore(parent -> facade);
        _store.openMessageStore(mock(BDBVirtualHost.class));
    }

    private void writeBody(final Transaction transaction, final long messageId, final byte[] body)
    {
        writeMessage(transaction, messageId, new InternalMessageMetaData(true, null, body.length), body);
    }

    private void writeMessage(final Transaction transaction, final long messageId,
                              final InternalMessageMetaData metadata, final byte[] body)
    {
        final StoredBDBMessage<InternalMessageMetaData> message = createMessage(messageId, metadata, body);
        try
        {
            message.store(transaction);
        }
        finally
        {
            message.clear(true);
        }
    }

    private StoredBDBMessage<InternalMessageMetaData> createMessage(final long messageId,
                                                                  final InternalMessageMetaData metadata,
                                                                  final byte[] body)
    {
        final StoredBDBMessage<InternalMessageMetaData> message = _store.new StoredBDBMessage<>(messageId, metadata,
                                                                                               false);
        try (final QpidByteBuffer input = QpidByteBuffer.wrap(body))
        {
            message.addContent(input);
            message.allContentAdded();
        }
        return message;
    }

    private byte[] readValue(final Database database, final long messageId)
    {
        final DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(messageId, key);
        final DatabaseEntry value = new DatabaseEntry();
        assertEquals(OperationStatus.SUCCESS, database.get(null, key, value, LockMode.DEFAULT));
        return entryBytes(value);
    }

    private byte[] entryBytes(final DatabaseEntry entry)
    {
        return Arrays.copyOfRange(entry.getData(), entry.getOffset(), entry.getOffset() + entry.getSize());
    }

    private MetadataWrite captureMetadata(final DatabaseEntry key, final DatabaseEntry value)
    {
        assertFalse(key.getPartial());
        assertFalse(value.getPartial());
        return new MetadataWrite(LongBinding.entryToLong(key), value, value.getData(), entryBytes(value));
    }

    private static byte[] metadataPayload(final MetadataWrite write)
    {
        return Arrays.copyOfRange(write.bytes(), METADATA_HEADER_SIZE, write.bytes().length);
    }

    private static BDBWriteBuffer cachedWriteBuffer()
    {
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            return buffer;
        }
    }

    private static void assertWriteBufferReleased(final BDBWriteBuffer cached)
    {
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            assertSame(cached, buffer);
            assertEquals(0, buffer.getValue().getData().length);
        }
    }

    private byte[] bytes(final int size, final byte value)
    {
        final byte[] bytes = new byte[size];
        Arrays.fill(bytes, value);
        return bytes;
    }

    private record MetadataWrite(long messageId, DatabaseEntry entry, byte[] array, byte[] bytes)
    {
    }

    private static class TestMetaData extends InternalMessageMetaData
    {
        private final byte[] _payload;

        private TestMetaData(final int contentSize, final byte[] payload)
        {
            super(true, null, contentSize);
            _payload = payload;
        }

        @Override
        public int getStorableSize()
        {
            return _payload.length;
        }

        @Override
        public void writeToBuffer(final QpidByteBuffer destination)
        {
            destination.put(_payload);
        }
    }
}
