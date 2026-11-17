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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.berkeleydb.tuple.MessageMetaDataBinding;
import org.apache.qpid.test.utils.UnitTestBase;

class BDBMessageStoreWriteBufferTest extends UnitTestBase
{
    private final List<Write> _metadataWrites = new ArrayList<>();
    private final List<Write> _contentWrites = new ArrayList<>();

    @TempDir
    private Path _directory;
    private Database _metadataDatabase;
    private Database _contentDatabase;
    private Transaction _transaction;
    private ProbeStore _store;

    @BeforeEach
    void setUp()
    {
        _metadataWrites.clear();
        _contentWrites.clear();
        _metadataDatabase = mock(Database.class);
        _contentDatabase = mock(Database.class);
        _transaction = mock(Transaction.class);
        final EnvironmentFacade facade = mock(EnvironmentFacade.class);
        when(facade.openDatabase(anyString(), any())).thenAnswer(invocation ->
                "MESSAGE_METADATA".equals(invocation.getArgument(0)) ? _metadataDatabase : _contentDatabase);
        when(facade.handleDatabaseException(anyString(), any(RuntimeException.class)))
                .thenAnswer(invocation -> invocation.getArgument(1));
        when(_metadataDatabase.put(any(), any(), any())).thenAnswer(invocation ->
        {
            _metadataWrites.add(capture(invocation.getArgument(1), invocation.getArgument(2)));
            return OperationStatus.SUCCESS;
        });
        when(_contentDatabase.put(any(), any(), any())).thenAnswer(invocation ->
        {
            _contentWrites.add(capture(invocation.getArgument(1), invocation.getArgument(2)));
            return OperationStatus.SUCCESS;
        });
        _store = new ProbeStore(facade);
    }

    @ParameterizedTest
    @CsvSource({"0,64", "7,64", "65536,128", "65537,128", "3,65531", "3,65532"})
    void testWritesUseExactLengthsAcrossCapacityBoundary(final int contentSize, final int metadataSize)
    {
        final byte[] content = bytes(contentSize, (byte) 11);
        final TestMetaData metadata = new TestMetaData(contentSize, bytes(metadataSize, (byte) 12));
        write(_store, _transaction, 100L, metadata, content);
        assertEquals(100L, _metadataWrites.get(0).messageId());
        assertEquals(100L, _contentWrites.get(0).messageId());
        assertArrayEquals(content, _contentWrites.get(0).bytes());
        final DatabaseEntry expected = new DatabaseEntry();
        MessageMetaDataBinding.getInstance().objectToEntry(metadata, expected);
        assertArrayEquals(expected.getData(), _metadataWrites.get(0).bytes());

        write(_store, _transaction, 101L, new TestMetaData(1, new byte[]{3}), new byte[]{4});
        assertArrayEquals(new byte[]{4}, _contentWrites.get(1).bytes());
        assertEquals(6, _metadataWrites.get(1).bytes().length);
    }

    @Test
    void testNestedSerializationKeepsOuterKeyAndPayload()
    {
        final TestMetaData outer = new TestMetaData(2, new byte[]{1, 2})
        {
            @Override
            public void writeToBuffer(final QpidByteBuffer destination)
            {
                write(_store, _transaction, 20L, new TestMetaData(1, new byte[]{3}), new byte[]{4});
                super.writeToBuffer(destination);
            }
        };
        write(_store, _transaction, 10L, outer, new byte[]{5, 6});
        assertEquals(List.of(20L, 10L), _metadataWrites.stream().map(Write::messageId).toList());
        assertEquals(List.of(20L, 10L), _contentWrites.stream().map(Write::messageId).toList());
        assertNotSame(_metadataWrites.get(0).array(), _metadataWrites.get(1).array());
        assertArrayEquals(new byte[]{1, 2}, Arrays.copyOfRange(_metadataWrites.get(1).bytes(), 5, 7));
        assertArrayEquals(new byte[]{5, 6}, _contentWrites.get(1).bytes());
    }

    @Test
    void testRetryKeepsBorrowedMetadataWhileNestedWritesComplete()
    {
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger serializations = new AtomicInteger();
        doAnswer(invocation ->
        {
            final Write value = capture(invocation.getArgument(1), invocation.getArgument(2));
            _metadataWrites.add(value);
            if (value.messageId() == 10L && attempts.getAndIncrement() == 0)
            {
                write(_store, _transaction, 20L, new TestMetaData(1, new byte[]{9}), new byte[]{8});
                throw mock(LockTimeoutException.class);
            }
            return OperationStatus.SUCCESS;
        }).when(_metadataDatabase).put(any(), any(), any());
        final TestMetaData metadata = new TestMetaData(2, new byte[]{1, 2})
        {
            @Override
            public void writeToBuffer(final QpidByteBuffer destination)
            {
                serializations.incrementAndGet();
                super.writeToBuffer(destination);
            }
        };
        write(_store, _transaction, 10L, metadata, new byte[]{3, 4});
        assertEquals(1, serializations.get());
        assertEquals(List.of(10L, 20L, 10L), _metadataWrites.stream().map(Write::messageId).toList());
        final Write first = _metadataWrites.get(0);
        final Write retry = _metadataWrites.get(2);
        assertSame(first.value(), retry.value());
        assertSame(first.array(), retry.array());
        assertArrayEquals(first.bytes(), retry.bytes());
        assertArrayEquals(new byte[]{3, 4}, _contentWrites.get(1).bytes());
    }

    @Test
    void testSerializationFailureReleasesBorrowAndOversizedArray()
    {
        final BDBWriteBuffer cached;
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            cached = buffer;
        }
        final TestMetaData metadata = new TestMetaData(0, new byte[BDBWriteBuffer.MAX_RETAINED_CAPACITY + 1])
        {
            @Override
            public void writeToBuffer(final QpidByteBuffer destination)
            {
                throw new IllegalStateException("Synthetic serialization failure");
            }
        };
        assertThrows(IllegalStateException.class, () -> write(_store, _transaction, 1L, metadata, new byte[0]));
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            assertSame(cached, buffer);
            assertEquals(0, buffer.getValue().getData().length);
        }
        write(_store, _transaction, 2L, new TestMetaData(1, new byte[]{2}), new byte[]{3});
        assertEquals(List.of(2L), _metadataWrites.stream().map(Write::messageId).toList());
    }

    @Test
    void testDatabaseFailureReleasesBorrow()
    {
        final IllegalStateException failure = new IllegalStateException("Synthetic database failure");
        doThrow(failure).when(_contentDatabase).put(any(), any(), any());
        assertSame(failure, assertThrows(IllegalStateException.class, () ->
                write(_store, _transaction, 1L, new TestMetaData(1, new byte[]{2}), new byte[]{3})));
        final DatabaseEntry value = _metadataWrites.get(0).value();
        assertEquals(0, value.getData().length);
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            assertSame(value, buffer.getValue());
        }
    }

    @Test
    void testJeCopiesReusedBuffersBeforeCommit()
    {
        final EnvironmentConfig environmentConfig = new EnvironmentConfig().setAllowCreate(true).setTransactional(true);
        environmentConfig.setCacheSize(8 * 1024 * 1024);
        final DatabaseConfig databaseConfig = new DatabaseConfig().setAllowCreate(true).setTransactional(true);
        try (final Environment environment = new Environment(_directory.toFile(), environmentConfig);
             final Database metadata = environment.openDatabase(null, "MESSAGE_METADATA", databaseConfig);
             final Database content = environment.openDatabase(null, "MESSAGE_CONTENT", databaseConfig))
        {
            final EnvironmentFacade facade = mock(EnvironmentFacade.class);
            final Map<String, Database> databases = Map.of("MESSAGE_METADATA", metadata, "MESSAGE_CONTENT", content);
            when(facade.openDatabase(anyString(), any())).thenAnswer(invocation ->
                    databases.get(invocation.getArgument(0)));
            final ProbeStore store = new ProbeStore(facade);
            final Transaction transaction = environment.beginTransaction(null, null);
            try
            {
                write(store, transaction, 1L, new InternalMessageMetaData(true, null, 4096), bytes(4096, (byte) 11));
                write(store, transaction, 2L, new InternalMessageMetaData(true, null, 3), new byte[]{4, 5, 6});
                try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
                {
                    buffer.prepare(999L, BDBWriteBuffer.MAX_RETAINED_CAPACITY);
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
            assertArrayEquals(bytes(4096, (byte) 11), read(content, 1L));
            assertArrayEquals(new byte[]{4, 5, 6}, read(content, 2L));
            final StorableMessageMetaData recovered = MessageMetaDataBinding.getInstance()
                    .entryToObject(new DatabaseEntry(read(metadata, 1L)));
            try
            {
                assertEquals(4096, recovered.getContentSize());
            }
            finally
            {
                recovered.dispose();
            }
        }
    }

    private void write(final ProbeStore store,
                       final Transaction transaction,
                       final long id,
                       final StorableMessageMetaData metadata,
                       final byte[] content)
    {
        final AbstractBDBMessageStore.StoredBDBMessage<?> message = store.new StoredBDBMessage<>(id, metadata, false);
        try (final QpidByteBuffer input = QpidByteBuffer.wrap(content))
        {
            message.addContent(input);
            message.allContentAdded();
            message.store(transaction);
        }
        finally
        {
            message.clear(true);
        }
    }

    private Write capture(final DatabaseEntry key, final DatabaseEntry value)
    {
        assertFalse(key.getPartial());
        assertFalse(value.getPartial());
        return new Write(LongBinding.entryToLong(key), value, value.getData(),
                Arrays.copyOfRange(value.getData(), value.getOffset(), value.getOffset() + value.getSize()));
    }

    private byte[] read(final Database database, final long id)
    {
        final DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(id, key);
        final DatabaseEntry value = new DatabaseEntry();
        assertEquals(OperationStatus.SUCCESS, database.get(null, key, value, LockMode.DEFAULT));
        return Arrays.copyOfRange(value.getData(), value.getOffset(), value.getOffset() + value.getSize());
    }

    private byte[] bytes(final int size, final byte value)
    {
        final byte[] bytes = new byte[size];
        Arrays.fill(bytes, value);
        return bytes;
    }

    private record Write(long messageId, DatabaseEntry value, byte[] array, byte[] bytes)
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

    private static final class ProbeStore extends AbstractBDBMessageStore
    {
        private final EnvironmentFacade _facade;

        private ProbeStore(final EnvironmentFacade facade)
        {
            _facade = facade;
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
            return _facade;
        }

        @Override
        protected Logger getLogger()
        {
            return NOPLogger.NOP_LOGGER;
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
