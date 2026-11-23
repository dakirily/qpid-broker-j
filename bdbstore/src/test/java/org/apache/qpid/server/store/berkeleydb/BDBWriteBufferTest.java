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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.DatabaseEntry;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.test.utils.UnitTestBase;

class BDBWriteBufferTest extends UnitTestBase
{
    @Test
    void testReuseResetsEntryStateAndLogicalSize()
    {
        final BDBWriteBuffer cached;
        final byte[] retained;
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            cached = buffer;
            buffer.prepare(1L, 1024);
            retained = buffer.getValue().getData();
            buffer.getKey().setPartial(2, 3, true);
            buffer.getKey().setOffset(1);
            buffer.getKey().setSize(2);
            buffer.getValue().setPartial(3, 4, true);
        }

        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            assertSame(cached, buffer);
            buffer.prepare(2L, 3);
            assertSame(retained, buffer.getValue().getData());
            assertEquals(0, buffer.getValue().getOffset());
            assertEquals(3, buffer.getValue().getSize());
            assertFalse(buffer.getValue().getPartial());
            assertFalse(buffer.getKey().getPartial());
            assertEquals(2L, LongBinding.entryToLong(buffer.getKey()));

            buffer.prepare(3L, 0);
            assertEquals(0, buffer.getValue().getSize());
            assertSame(retained, buffer.getValue().getData());
        }
    }

    @Test
    void testOversizedArrayIsNotRetained()
    {
        final DatabaseEntry entry;
        final byte[] retained;
        final byte[] oversized;
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            buffer.prepare(1L, BDBWriteBuffer.MAX_RETAINED_CAPACITY);
            retained = buffer.getValue().getData();
            assertEquals(BDBWriteBuffer.MAX_RETAINED_CAPACITY, retained.length);
            buffer.prepare(2L, BDBWriteBuffer.MAX_RETAINED_CAPACITY + 1);
            entry = buffer.getValue();
            oversized = entry.getData();
            assertNotSame(retained, oversized);
        }
        assertEquals(0, entry.getData().length);

        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            buffer.prepare(3L, 8);
            assertSame(retained, buffer.getValue().getData());
            assertNotSame(oversized, buffer.getValue().getData());
        }
    }

    @Test
    void testNestedBorrowDoesNotChangeOuterKeyOrPayload()
    {
        try (final BDBWriteBuffer outer = BDBWriteBuffer.acquire())
        {
            outer.prepare(11L, 16);
            Arrays.fill(outer.getValue().getData(), (byte) 42);
            final byte[] expected = Arrays.copyOf(outer.getValue().getData(), 16);
            try (final BDBWriteBuffer inner = BDBWriteBuffer.acquire())
            {
                inner.prepare(22L, 16);
                assertNotSame(outer, inner);
                assertNotSame(outer.getKey(), inner.getKey());
                assertNotSame(outer.getValue(), inner.getValue());
                assertNotSame(outer.getValue().getData(), inner.getValue().getData());
                Arrays.fill(inner.getValue().getData(), (byte) 43);
            }
            assertEquals(11L, LongBinding.entryToLong(outer.getKey()));
            assertArrayEquals(expected, Arrays.copyOf(outer.getValue().getData(), 16));
        }
    }

    @Test
    void testCopyPreservesSingleAndCompositeSourcePositions()
    {
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire();
             final QpidByteBuffer first = QpidByteBuffer.wrap(new byte[]{0, 1, 2});
             final QpidByteBuffer second = QpidByteBuffer.wrap(new byte[]{3, 4}))
        {
            buffer.prepare(1L, 100);
            first.position(1);
            buffer.prepare(2L, first.remaining());
            buffer.copyContent(first);
            assertEquals(1, first.position());
            assertArrayEquals(new byte[]{1, 2}, Arrays.copyOf(buffer.getValue().getData(), 2));

            try (final QpidByteBuffer composite = QpidByteBuffer.concatenate(first, second))
            {
                buffer.prepare(3L, composite.remaining());
                buffer.copyContent(composite);
                assertEquals(0, composite.position());
                assertArrayEquals(new byte[]{1, 2, 3, 4}, Arrays.copyOf(buffer.getValue().getData(), 4));
            }
        }
    }

    @Test
    void testFailedPreparationReleasesBorrow()
    {
        final BDBWriteBuffer cached;
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            cached = buffer;
            assertThrows(IllegalArgumentException.class, () -> buffer.prepare(1L, -1));
        }
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            assertSame(cached, buffer);
        }
    }

    @Test
    void testExactCapacityCopyPreservesCompositeSource()
    {
        final byte[] firstBytes = new byte[BDBWriteBuffer.MAX_RETAINED_CAPACITY / 2];
        final byte[] secondBytes = new byte[firstBytes.length];
        Arrays.fill(firstBytes, (byte) 1);
        Arrays.fill(secondBytes, (byte) 2);
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire();
             final QpidByteBuffer first = QpidByteBuffer.wrap(firstBytes);
             final QpidByteBuffer second = QpidByteBuffer.wrap(secondBytes);
             final QpidByteBuffer content = QpidByteBuffer.concatenate(first, second))
        {
            buffer.prepare(1L, content.remaining());
            assertEquals(content.remaining(), buffer.getValue().getData().length);
            buffer.copyContent(content);
            assertEquals(0, content.position());
            assertArrayEquals(firstBytes, Arrays.copyOfRange(buffer.getValue().getData(), 0, firstBytes.length));
            assertArrayEquals(secondBytes, Arrays.copyOfRange(buffer.getValue().getData(),
                    firstBytes.length, firstBytes.length + secondBytes.length));
        }
    }

    @Test
    void testConcurrentWritersUseSeparateContexts() throws Exception
    {
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        try
        {
            final List<Future<BDBWriteBuffer>> writers = List.of(executor.submit(() ->
                    borrowOnWriterThread(1L, ready, release)), executor.submit(() ->
                    borrowOnWriterThread(2L, ready, release)));
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            release.countDown();
            final BDBWriteBuffer first = writers.get(0).get(10, TimeUnit.SECONDS);
            final BDBWriteBuffer second = writers.get(1).get(10, TimeUnit.SECONDS);
            assertNotSame(first, second);
            assertNotSame(first.getKey(), second.getKey());
        }
        finally
        {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private BDBWriteBuffer borrowOnWriterThread(final long id,
                                                final CountDownLatch ready,
                                                final CountDownLatch release) throws InterruptedException
    {
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            buffer.prepare(id, 16);
            Arrays.fill(buffer.getValue().getData(), (byte) id);
            ready.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            assertEquals(id, LongBinding.entryToLong(buffer.getKey()));
            for (int i = 0; i < buffer.getValue().getSize(); i++)
            {
                assertEquals((byte) id, buffer.getValue().getData()[i]);
            }
            return buffer;
        }
    }
}
