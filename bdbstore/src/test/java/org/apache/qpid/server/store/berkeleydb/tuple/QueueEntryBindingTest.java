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
package org.apache.qpid.server.store.berkeleydb.tuple;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.UUID;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.util.CachingUUIDFactory;
import org.apache.qpid.test.utils.UnitTestBase;

class QueueEntryBindingTest extends UnitTestBase
{
    private static final UUID QUEUE_ID = new UUID(Long.MIN_VALUE, Long.MAX_VALUE);

    @ParameterizedTest
    @ValueSource(longs = {Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE})
    void testWriteMessageIdMatchesLongBinding(final long messageId)
    {
        final DatabaseEntry expected = new DatabaseEntry();
        LongBinding.longToEntry(messageId, expected);

        final byte[] backing = new byte[2 * Long.BYTES];
        final DatabaseEntry actual = new DatabaseEntry(backing, 3, Long.BYTES);
        QueueEntryBinding.writeMessageId(messageId, actual);

        assertSame(backing, actual.getData());
        assertEquals(0, actual.getOffset());
        assertEquals(Long.BYTES, actual.getSize());
        assertArrayEquals(expected.getData(), Arrays.copyOf(actual.getData(), actual.getSize()));
    }

    @ParameterizedTest
    @ValueSource(longs = {Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE})
    void testDeliveryKeyMatchesLegacyTupleEncoding(final long messageId)
    {
        final TupleOutput expected = new TupleOutput();
        expected.writeLong(QUEUE_ID.getMostSignificantBits());
        expected.writeLong(QUEUE_ID.getLeastSignificantBits());
        expected.writeLong(messageId);

        final byte[] backing = new byte[32];
        final DatabaseEntry entry = new DatabaseEntry(backing, 3, 24);
        QueueEntryBinding.objectToEntry(QUEUE_ID, messageId, entry);

        assertSame(backing, entry.getData());
        assertEquals(0, entry.getOffset());
        assertEquals(24, entry.getSize());
        assertArrayEquals(expected.toByteArray(), Arrays.copyOf(entry.getData(), entry.getSize()));
    }

    @Test
    void testDeliveryKeyReplacesUndersizedBuffer()
    {
        final DatabaseEntry entry = new DatabaseEntry(new byte[23]);
        QueueEntryBinding.objectToEntry(QUEUE_ID, 42L, entry);

        assertEquals(24, entry.getSize());
        assertEquals(42L, QueueEntryBinding.readMessageId(entry));
    }

    @Test
    void testReadDeliveryKeyAtNonZeroOffset()
    {
        final TupleOutput output = new TupleOutput();
        output.writeByte(0);
        output.writeLong(QUEUE_ID.getMostSignificantBits());
        output.writeLong(QUEUE_ID.getLeastSignificantBits());
        output.writeLong(-42L);
        final DatabaseEntry entry = new DatabaseEntry(output.toByteArray(), 1, 24);

        final MessageEnqueueRecord record = QueueEntryBinding.readIds(new CachingUUIDFactory(), entry);
        assertEquals(QUEUE_ID, record.getQueueId());
        assertEquals(-42L, record.getMessageNumber());
        assertEquals(-42L, QueueEntryBinding.readMessageId(entry));
        assertTrue(QueueEntryBinding.matchesQueueId(entry, QUEUE_ID));
        assertFalse(QueueEntryBinding.matchesQueueId(entry, new UUID(0L, QUEUE_ID.getLeastSignificantBits())));
        assertFalse(QueueEntryBinding.matchesQueueId(entry, new UUID(QUEUE_ID.getMostSignificantBits(), 0L)));
    }

    @Test
    void testTruncatedDeliveryKeyFailsValidation()
    {
        final DatabaseEntry entry = new DatabaseEntry(new byte[24], 0, 23);
        assertThrows(IllegalArgumentException.class, () -> QueueEntryBinding.matchesQueueId(entry, QUEUE_ID));
        assertThrows(IllegalArgumentException.class, () -> QueueEntryBinding.readMessageId(entry));
        assertThrows(IllegalArgumentException.class, () -> QueueEntryBinding.readIds(new CachingUUIDFactory(), entry));
    }

    @Test
    void testOrderedDecoderReusesZeroUuidWithMutableKeyAtNonZeroOffset()
    {
        final UUID queueId = new UUID(0L, 0L);
        final int offset = 3;
        final int keySize = QueueEntryBinding.KEY_SIZE;
        final byte[] backing = new byte[offset + keySize + 1];
        final DatabaseEntry key = new DatabaseEntry(backing, offset, keySize + 1);
        final DatabaseEntry encoded = new DatabaseEntry();
        final QueueEntryBinding.OrderedDecoder decoder = new QueueEntryBinding.OrderedDecoder();

        QueueEntryBinding.objectToEntry(queueId, Long.MIN_VALUE, encoded);
        System.arraycopy(encoded.getData(), 0, backing, offset, keySize);
        final MessageEnqueueRecord first = decoder.readIds(key);

        QueueEntryBinding.objectToEntry(queueId, Long.MAX_VALUE, encoded);
        System.arraycopy(encoded.getData(), 0, backing, offset, keySize);
        final MessageEnqueueRecord second = decoder.readIds(key);
        Arrays.fill(backing, (byte) 0);

        assertEquals(queueId, first.getQueueId());
        assertSame(first.getQueueId(), second.getQueueId());
        assertNotSame(first, second);
        assertEquals(Long.MIN_VALUE, first.getMessageNumber());
        assertEquals(Long.MAX_VALUE, second.getMessageNumber());
    }

    @Test
    void testOrderedDecoderValidatesKeyForCachedQueue()
    {
        final DatabaseEntry entry = new DatabaseEntry();
        QueueEntryBinding.objectToEntry(QUEUE_ID, 1L, entry);
        final QueueEntryBinding.OrderedDecoder decoder = new QueueEntryBinding.OrderedDecoder();
        decoder.readIds(entry);

        entry.setSize(QueueEntryBinding.KEY_SIZE - 1);
        assertThrows(IllegalArgumentException.class, () -> decoder.readIds(entry));
    }
}
