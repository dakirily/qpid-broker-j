/*
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
 */

package org.apache.qpid.server.store.berkeleydb.tuple;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import org.apache.qpid.server.store.berkeleydb.AbstractBDBMessageStore;
import org.apache.qpid.server.util.CachingUUIDFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sleepycat.je.DatabaseEntry;

class QueueEntryBindingTest
{
    private static final int KEY_SIZE = 24;

    private final long _messageId = 12345L;
    private CachingUUIDFactory _uuidFactory;
    private UUID _uuid;

    @BeforeEach
    void beforeEach()
    {
        _uuidFactory = new CachingUUIDFactory();
        _uuid = new UUID(0x8123456789ABCDEFL, 0xFEDCBA9876543210L);
    }

    @Test
    void entryToObject()
    {
        final DatabaseEntry entry = encode(_uuid, _messageId);
        final AbstractBDBMessageStore.BDBEnqueueRecord record = QueueEntryBinding.readIds(_uuidFactory, entry);

        assertEquals(_uuid, record.getQueueId());
        assertEquals(_messageId, record.messageNumber());
    }

    @Test
    void objectToEntry()
    {
        final DatabaseEntry entry = encode(_uuid, _messageId);
        final byte[] data = entry.getData();

        final UUID expectedQueueId = _uuid;

        final UUID actualQueueId = _uuidFactory.createUuidFromBits(
                ByteBuffer.wrap(data, 0, 8).getLong() ^ 0x8000000000000000L,
                ByteBuffer.wrap(data, 8, 8).getLong() ^ 0x8000000000000000L);
        final long actualMessageId = ByteBuffer.wrap(data, 16, 8).getLong() ^ 0x8000000000000000L;

        assertEquals(expectedQueueId, actualQueueId);
        assertEquals(_messageId, actualMessageId);
    }

    @Test
    void matchesQueueIdReturnsTrueForMatchingQueueIdAndFalseForDifferentQueueId()
    {
        final DatabaseEntry entry = encodeWithOffset(_uuid, _messageId, 7);

        assertTrue(QueueEntryBinding.matchesQueueId(entry, _uuid));
        assertFalse(QueueEntryBinding.matchesQueueId(entry,
                new UUID(_uuid.getMostSignificantBits(), _uuid.getLeastSignificantBits() + 1)));
    }

    @Test
    void matchesQueueIdReturnsFalseForSameMsbDifferentLsb()
    {
        final UUID queueId = new UUID(0x0123456789ABCDEFL, 0x1122334455667788L);
        final DatabaseEntry entry = encode(queueId, _messageId);

        assertFalse(QueueEntryBinding.matchesQueueId(entry,
                new UUID(queueId.getMostSignificantBits(), queueId.getLeastSignificantBits() + 1)));
    }

    @Test
    void matchesQueueIdReturnsFalseForSameLsbDifferentMsb()
    {
        final UUID queueId = new UUID(0x0123456789ABCDEFL, 0x1122334455667788L);
        final DatabaseEntry entry = encode(queueId, _messageId);

        assertFalse(QueueEntryBinding.matchesQueueId(entry,
                new UUID(queueId.getMostSignificantBits() + 1, queueId.getLeastSignificantBits())));
    }

    @Test
    void matchesQueueIdReturnsFalseForNullEntryOrQueueIdOrData()
    {
        assertFalse(QueueEntryBinding.matchesQueueId(null, _uuid));
        assertFalse(QueueEntryBinding.matchesQueueId(new DatabaseEntry(), null));

        final DatabaseEntry nullData = new DatabaseEntry();
        nullData.setData(null);
        assertFalse(QueueEntryBinding.matchesQueueId(nullData, _uuid));
    }

    @Test
    void matchesQueueIdReturnsFalseForInvalidOffsetOrSize()
    {
        final UUID queueId = _uuid;

        final DatabaseEntry sizeTooSmall = entryWithOffsetAndSize(new byte[KEY_SIZE], 0, KEY_SIZE - 1);
        assertFalse(QueueEntryBinding.matchesQueueId(sizeTooSmall, queueId));

        final DatabaseEntry offsetTooLarge = entryWithOffsetAndSize(new byte[KEY_SIZE], 1, KEY_SIZE);
        assertFalse(QueueEntryBinding.matchesQueueId(offsetTooLarge, queueId));

        final DatabaseEntry negativeOffset = entryWithOffsetAndSize(new byte[KEY_SIZE], -1, KEY_SIZE);
        assertFalse(QueueEntryBinding.matchesQueueId(negativeOffset, queueId));
    }

    @Test
    void readMessageIdReturnsDecodedMessageId()
    {
        final DatabaseEntry entry = encodeWithOffset(_uuid, _messageId, 5);
        assertEquals(_messageId, QueueEntryBinding.readMessageId(entry));
    }

    @Test
    void readMessageIdThrowsIllegalArgumentExceptionForInvalidEntry()
    {
        final DatabaseEntry invalid = entryWithOffsetAndSize(new byte[KEY_SIZE], 1, KEY_SIZE);
        assertThrows(IllegalArgumentException.class, () -> QueueEntryBinding.readMessageId(invalid));
    }

    @Test
    void readIdsReturnsMsbLsbMessageIdAndHonoursEntryOffset()
    {
        final DatabaseEntry entry = encodeWithOffset(_uuid, _messageId, 11);
        final AbstractBDBMessageStore.BDBEnqueueRecord record = QueueEntryBinding.readIds(_uuidFactory, entry);

        assertEquals(_uuid, record.getQueueId());
        assertEquals(_messageId, record.getMessageNumber());
    }

    @Test
    void entryToObjectHonorsEntryOffset()
    {
        final DatabaseEntry entry = encodeWithOffset(_uuid, _messageId, 9);
        final AbstractBDBMessageStore.BDBEnqueueRecord record = QueueEntryBinding.readIds(_uuidFactory, entry);

        assertEquals(_uuid, record.getQueueId());
        assertEquals(_messageId, record.getMessageNumber());
    }

    @Test
    void readIdsThrowsIllegalArgumentExceptionWhenEntryIsNull()
    {
        assertThrows(IllegalArgumentException.class, () -> QueueEntryBinding.readIds(_uuidFactory, null));
    }

    @Test
    void readIdsThrowsIllegalArgumentExceptionWhenDataIsNull()
    {
        final DatabaseEntry entry = new DatabaseEntry();
        entry.setData(null);
        assertThrows(IllegalArgumentException.class, () -> QueueEntryBinding.readIds(_uuidFactory, entry));
    }

    @Test
    void readIdsThrowsIllegalArgumentExceptionWhenSizeLessThanKeySize()
    {
        final DatabaseEntry entry = entryWithOffsetAndSize(new byte[KEY_SIZE], 0, KEY_SIZE - 1);
        assertThrows(IllegalArgumentException.class, () -> QueueEntryBinding.readIds(_uuidFactory, entry));
    }

    @Test
    void readIdsThrowsIllegalArgumentExceptionWhenOffsetNegative()
    {
        final DatabaseEntry entry = entryWithOffsetAndSize(new byte[KEY_SIZE], -1, KEY_SIZE);
        assertThrows(IllegalArgumentException.class, () -> QueueEntryBinding.readIds(_uuidFactory, entry));
    }

    @Test
    void readIdsThrowsIllegalArgumentExceptionWhenOffsetPlusKeySizeBeyondArrayLength()
    {
        final DatabaseEntry entry = entryWithOffsetAndSize(new byte[KEY_SIZE], 1, KEY_SIZE);
        assertThrows(IllegalArgumentException.class, () -> QueueEntryBinding.readIds(_uuidFactory, entry));
    }

    @Test
    void objectToEntryPreservesLexicographicOrderingForSameQueueIdAcrossSignedMessageIdRange()
    {
        final UUID queueId = new UUID(0L, 0L);
        final long[] messageIds = { Long.MIN_VALUE, -2L, -1L, 0L, 1L, 2L, 12345L, Long.MAX_VALUE };

        final DatabaseEntry[] keys = new DatabaseEntry[messageIds.length];
        for (int i = 0; i < messageIds.length; i++)
        {
            keys[i] = encode(queueId, messageIds[i]);
        }

        Arrays.sort(keys, (left, right) -> compareUnsignedLexicographically(left, right, KEY_SIZE));

        final long[] actualOrder = new long[keys.length];
        for (int i = 0; i < keys.length; i++)
        {
            actualOrder[i] = QueueEntryBinding.readMessageId(keys[i]);
        }

        final long[] expectedOrder = messageIds.clone();
        Arrays.sort(expectedOrder);

        assertArrayEquals(expectedOrder, actualOrder);
    }

    @Test
    void objectToEntryPreservesLexicographicOrderingAcrossQueueIdsForSameMessageId()
    {
        final long messageId = 0L;
        final UUID[] queueIds =
                {
                        new UUID(Long.MIN_VALUE, 0L),
                        new UUID(-1L, -1L),
                        new UUID(-1L, 0L),
                        new UUID(-1L, 1L),
                        new UUID(0L, Long.MIN_VALUE),
                        new UUID(0L, 0L),
                        new UUID(0L, 1L),
                        new UUID(1L, 0L),
                        new UUID(Long.MAX_VALUE, Long.MAX_VALUE)
                };

        final DatabaseEntry[] keys = new DatabaseEntry[queueIds.length];
        for (int i = 0; i < queueIds.length; i++)
        {
            keys[i] = encode(queueIds[i], messageId);
        }

        Arrays.sort(keys, (left, right) -> compareUnsignedLexicographically(left, right, KEY_SIZE));

        final UUID[] actualOrder = new UUID[keys.length];
        for (int i = 0; i < keys.length; i++)
        {
            final AbstractBDBMessageStore.BDBEnqueueRecord record = QueueEntryBinding.readIds(_uuidFactory, keys[i]);
            actualOrder[i] = record.getQueueId();
        }

        final UUID[] expectedOrder = queueIds.clone();
        Arrays.sort(expectedOrder, QueueEntryBindingTest::compareUuidSigned);

        assertArrayEquals(expectedOrder, actualOrder);
    }

    private static DatabaseEntry encode(final UUID queueId, final long messageId)
    {
        final DatabaseEntry entry = new DatabaseEntry();
        QueueEntryBinding.objectToEntry(queueId, messageId, entry);
        return entry;
    }

    private static DatabaseEntry encodeWithOffset(final UUID queueId, final long messageId, final int offset)
    {
        final DatabaseEntry encoded = encode(queueId, messageId);
        final byte[] raw = encoded.getData();

        final byte[] backing = new byte[offset + raw.length + 3];
        Arrays.fill(backing, (byte) 0x5A);
        System.arraycopy(raw, 0, backing, offset, raw.length);

        final DatabaseEntry entry = new DatabaseEntry();
        entry.setData(backing, offset, raw.length);
        return entry;
    }

    private static DatabaseEntry entryWithOffsetAndSize(final byte[] data, final int offset, final int size)
    {
        final DatabaseEntry entry = new DatabaseEntry();
        entry.setData(data);
        entry.setOffset(offset);
        entry.setSize(size);
        return entry;
    }

    private static int compareUnsignedLexicographically(final DatabaseEntry left,
                                                        final DatabaseEntry right,
                                                        final int length)
    {
        final byte[] leftData = left.getData();
        final byte[] rightData = right.getData();
        final int leftOffset = left.getOffset();
        final int rightOffset = right.getOffset();

        for (int i = 0; i < length; i++)
        {
            final int leftByte = leftData[leftOffset + i] & 0xFF;
            final int rightByte = rightData[rightOffset + i] & 0xFF;
            if (leftByte != rightByte)
            {
                return leftByte < rightByte ? -1 : 1;
            }
        }
        return 0;
    }

    private static int compareUuidSigned(final UUID left, final UUID right)
    {
        int comparison = Long.compare(left.getMostSignificantBits(), right.getMostSignificantBits());
        if (comparison == 0)
        {
            comparison = Long.compare(left.getLeastSignificantBits(), right.getLeastSignificantBits());
        }
        return comparison;
    }
}
