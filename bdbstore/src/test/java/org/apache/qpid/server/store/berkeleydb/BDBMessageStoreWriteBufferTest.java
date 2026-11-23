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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.store.berkeleydb.AbstractBDBMessageStore.StoredBDBMessage;
import org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

class BDBMessageStoreWriteBufferTest extends UnitTestBase
{
    private static final String CONTENT_DATABASE_NAME = "MESSAGE_CONTENT";
    private static final String METADATA_DATABASE_NAME = "MESSAGE_METADATA";

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
    void testJeCopiesReusedBodiesBeforeCommit(@TempDir final Path directory)
    {
        final int capacity = BDBWriteBuffer.MAX_RETAINED_CAPACITY;
        final List<byte[]> bodies = List.of(bytes(capacity, (byte) 11), bytes(capacity - 1, (byte) 12),
                                            new byte[] {4, 5, 6}, new byte[0], bytes(capacity + 1, (byte) 13),
                                            new byte[] {7, 8});
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
                    writeBody(transaction, index + 1L, bodies.get(index));
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
             final Database content = environment.openDatabase(null, CONTENT_DATABASE_NAME, databaseConfig))
        {
            for (int index = 0; index < bodies.size(); index++)
            {
                assertArrayEquals(bodies.get(index), readBody(content, index + 1L));
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
    void testFailedOversizedContentPutReleasesBufferForRetry()
    {
        final Database content = mock(Database.class);
        openStore(mock(Database.class), content);
        final Transaction failedTransaction = mock(Transaction.class);
        final Transaction retryTransaction = mock(Transaction.class);
        final byte[] body = bytes(BDBWriteBuffer.MAX_RETAINED_CAPACITY + 1, (byte) 14);
        final BDBWriteBuffer cached;
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            cached = buffer;
        }
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

        final StoredBDBMessage<InternalMessageMetaData> message = createMessage(10L, body);
        try
        {
            assertSame(writeFailure, assertThrows(RuntimeException.class, () -> message.store(failedTransaction)));
            try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
            {
                assertSame(cached, buffer);
                assertEquals(0, buffer.getValue().getData().length);
            }
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
        final StoredBDBMessage<InternalMessageMetaData> message = createMessage(messageId, body);
        try
        {
            message.store(transaction);
        }
        finally
        {
            message.clear(true);
        }
    }

    private StoredBDBMessage<InternalMessageMetaData> createMessage(final long messageId, final byte[] body)
    {
        final InternalMessageMetaData metadata = new InternalMessageMetaData(true, null, body.length);
        final StoredBDBMessage<InternalMessageMetaData> message = _store.new StoredBDBMessage<>(messageId, metadata,
                                                                                               false);
        try (final QpidByteBuffer input = QpidByteBuffer.wrap(body))
        {
            message.addContent(input);
            message.allContentAdded();
        }
        return message;
    }

    private byte[] readBody(final Database database, final long messageId)
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

    private byte[] bytes(final int size, final byte value)
    {
        final byte[] bytes = new byte[size];
        Arrays.fill(bytes, value);
        return bytes;
    }
}
