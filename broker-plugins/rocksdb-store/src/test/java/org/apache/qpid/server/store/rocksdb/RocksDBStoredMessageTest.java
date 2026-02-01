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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TestMessageMetaData;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RocksDBStoredMessageTest
{
    @Test
    void flowToDiskStoresContentAndClearsMemory()
    {
        FakeAccess access = new FakeAccess();
        byte[] content = "abc".getBytes(StandardCharsets.UTF_8);
        TestMessageMetaData metaData = new TestMessageMetaData(1L, content.length);

        RocksDBStoredMessage<TestMessageMetaData> message =
                new RocksDBStoredMessage<>(access, 1L, metaData);
        message.addContent(QpidByteBuffer.wrap(content));
        message.allContentAdded();

        long expectedInMemory = metaData.getStorableSize() + metaData.getContentSize();
        assertEquals(expectedInMemory, access.inMemory.get(), "Unexpected in-memory size before flowToDisk");

        assertTrue(message.flowToDisk(), "Expected flowToDisk to return true");
        assertEquals(0L, access.inMemory.get(), "Expected in-memory size to be cleared");
        assertEquals(expectedInMemory, access.bytesEvacuated.get(), "Unexpected evacuated byte count");
        assertArrayEquals(content, access.storedContent, "Stored content mismatch");
    }

    @Test
    void getContentLoadsFromStoreOnce()
    {
        FakeAccess access = new FakeAccess();
        byte[] content = "payload".getBytes(StandardCharsets.UTF_8);
        TestMessageMetaData metaData = new TestMessageMetaData(1L, content.length);

        RocksDBStoredMessage<TestMessageMetaData> message =
                new RocksDBStoredMessage<>(access, 1L, metaData);
        message.addContent(QpidByteBuffer.wrap(content));
        message.allContentAdded();
        message.flowToDisk();

        try (QpidByteBuffer buffer = message.getContent(0, Integer.MAX_VALUE))
        {
            byte[] read = new byte[content.length];
            buffer.get(read);
            assertArrayEquals(content, read);
        }
        try (QpidByteBuffer buffer = message.getContent(0, Integer.MAX_VALUE))
        {
            byte[] read = new byte[content.length];
            buffer.get(read);
            assertArrayEquals(content, read);
        }

        assertEquals(1, access.loadContentCalls.get(), "Expected loadContent to be called once");
    }

    @Test
    void removeDeletesAndNotifies()
    {
        FakeAccess access = new FakeAccess();
        byte[] content = "remove".getBytes(StandardCharsets.UTF_8);
        TestMessageMetaData metaData = new TestMessageMetaData(1L, content.length);

        RocksDBStoredMessage<TestMessageMetaData> message =
                new RocksDBStoredMessage<>(access, 1L, metaData);
        message.addContent(QpidByteBuffer.wrap(content));
        message.allContentAdded();
        message.flowToDisk();

        message.remove();

        assertEquals(1, access.deleteCalls.get(), "Expected deleteMessage to be called");
        assertEquals(-content.length, access.storedSizeDelta.get(), "Unexpected stored size delta");
        assertSame(message, access.deletedMessage.get(), "Expected deleted message to be notified");
    }

    private static final class FakeAccess implements RocksDBStoredMessage.StoreAccess
    {
        private final AtomicLong inMemory = new AtomicLong();
        private final AtomicLong bytesEvacuated = new AtomicLong();
        private final AtomicInteger loadContentCalls = new AtomicInteger();
        private final AtomicInteger deleteCalls = new AtomicInteger();
        private final AtomicInteger storedSizeDelta = new AtomicInteger();
        private final AtomicReference<StoredMessage<?>> deletedMessage = new AtomicReference<>();
        private byte[] storedContent;
        private MetaDataRecord storedMeta;

        @Override
        public int getMessageChunkSize()
        {
            return 0;
        }

        @Override
        public int getDefaultChunkSize()
        {
            return 0;
        }

        @Override
        public boolean shouldChunk(final int contentSize)
        {
            return false;
        }

        @Override
        public byte[] loadContent(final long messageId)
        {
            loadContentCalls.incrementAndGet();
            return storedContent == null ? new byte[0] : Arrays.copyOf(storedContent, storedContent.length);
        }

        @Override
        public MetaDataRecord loadMetaDataRecord(final long messageId)
        {
            return storedMeta;
        }

        @Override
        public byte[] loadChunkedContentSlice(final long messageId,
                                              final int contentSize,
                                              final int chunkSize,
                                              final int offset,
                                              final int length)
        {
            throw new IllegalStateException("Chunked content not expected in test");
        }

        @Override
        public void storeChunk(final org.rocksdb.Transaction txn,
                               final long messageId,
                               final int chunkIndex,
                               final byte[] data)
        {
            throw new IllegalStateException("Chunked content not expected in test");
        }

        @Override
        public void storeChunkedMessage(final org.rocksdb.Transaction txn,
                                        final long messageId,
                                        final org.apache.qpid.server.store.StorableMessageMetaData metaData,
                                        final int chunkSize,
                                        final java.util.List<byte[]> chunks,
                                        final byte[] trailingChunk)
        {
            storedMeta = new MetaDataRecord(metaData, true, chunkSize);
            int total = 0;
            if (chunks != null)
            {
                for (byte[] chunk : chunks)
                {
                    total += chunk.length;
                }
            }
            if (trailingChunk != null)
            {
                total += trailingChunk.length;
            }
            byte[] combined = new byte[total];
            int offset = 0;
            if (chunks != null)
            {
                for (byte[] chunk : chunks)
                {
                    System.arraycopy(chunk, 0, combined, offset, chunk.length);
                    offset += chunk.length;
                }
            }
            if (trailingChunk != null)
            {
                System.arraycopy(trailingChunk, 0, combined, offset, trailingChunk.length);
            }
            storedContent = combined;
        }

        @Override
        public void storeMessageMetadata(final org.rocksdb.Transaction txn,
                                         final long messageId,
                                         final org.apache.qpid.server.store.StorableMessageMetaData metaData,
                                         final boolean chunked,
                                         final int chunkSize)
        {
            storedMeta = new MetaDataRecord(metaData, chunked, chunkSize);
        }

        @Override
        public void storeMessage(final org.rocksdb.Transaction txn,
                                 final long messageId,
                                 final org.apache.qpid.server.store.StorableMessageMetaData metaData,
                                 final byte[] content)
        {
            storedMeta = new MetaDataRecord(metaData, false, 0);
            storedContent = content == null ? new byte[0] : Arrays.copyOf(content, content.length);
        }

        @Override
        public void deleteMessage(final long messageId)
        {
            deleteCalls.incrementAndGet();
        }

        @Override
        public void storedSizeChangeOccurred(final int delta)
        {
            storedSizeDelta.addAndGet(delta);
        }

        @Override
        public void notifyMessageDeleted(final StoredMessage<?> message)
        {
            deletedMessage.set(message);
        }

        @Override
        public AtomicLong getInMemorySize()
        {
            return inMemory;
        }

        @Override
        public AtomicLong getBytesEvacuatedFromMemory()
        {
            return bytesEvacuated;
        }

        @Override
        public byte[] emptyValue()
        {
            return new byte[0];
        }
    }
}
