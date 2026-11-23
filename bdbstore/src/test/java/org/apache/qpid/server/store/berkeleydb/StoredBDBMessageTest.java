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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.store.MessageStore.MessageStoreReader;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;
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

    private StoredBDBMessage<TestMessageMetaData> createMessage(final int contentSize)
    {
        return (StoredBDBMessage<TestMessageMetaData>) _store.addMessage(new TestMessageMetaData(0L, contentSize));
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
}
