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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import org.apache.qpid.server.util.CachingUUIDFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sleepycat.je.DatabaseEntry;

class QueueEntryBindingTest
{
    private final long _messageId = 12345L;
    private CachingUUIDFactory _uuidFactory;
    private UUID _uuid;

    @BeforeEach
    void beforeEach()
    {
        _uuidFactory = new CachingUUIDFactory();
        _uuid = UUID.randomUUID();
    }

    @Test
    void entryToObject()
    {
        final DatabaseEntry entry = encode(_uuid, _messageId);
        final long[] result = QueueEntryBinding.readIds(entry);

        assertEquals(_uuid, new UUID(result[0], result[1]));
        assertEquals(_messageId, result[2]);
    }

    @Test
    void objectToEntry()
    {
        final DatabaseEntry entry = encode(_uuid, _messageId);
        final byte[] data = entry.getData();

        final UUID expectedQueueId = _uuid;
        final long expectedMessageId = _messageId;

        final UUID actualQueueId = _uuidFactory.createUuidFromBits(
                ByteBuffer.wrap(data, 0, 8).getLong() ^ 0x8000000000000000L,
                ByteBuffer.wrap(data, 8, 8).getLong() ^ 0x8000000000000000L);
        final long actualMessageId = ByteBuffer.wrap(data, 16, 8).getLong() ^ 0x8000000000000000L;

        assertEquals(expectedQueueId, actualQueueId);
        assertEquals(expectedMessageId, actualMessageId);
    }

    @Test
    void matchesQueueId_returnsTrueForMatchingQueueId_andFalseForDifferentQueueId()
    {
        final DatabaseEntry entry = encodeWithOffset(_uuid, _messageId, 7);

        assertTrue(QueueEntryBinding.matchesQueueId(entry, _uuid));
        assertFalse(QueueEntryBinding.matchesQueueId(entry, UUID.randomUUID()));
    }

    @Test
    void readMessageId_returnsDecodedMessageId()
    {
        final DatabaseEntry entry = encodeWithOffset(_uuid, _messageId, 5);
        assertEquals(_messageId, QueueEntryBinding.readMessageId(entry));
    }

    @Test
    void readIds_returnsMsbLsbMessageId_andHonoursEntryOffset()
    {
        final DatabaseEntry entry = encodeWithOffset(_uuid, _messageId, 11);
        final long[] ids = QueueEntryBinding.readIds(entry);

        assertArrayEquals(new long[]
                {
                        _uuid.getMostSignificantBits(),
                        _uuid.getLeastSignificantBits(),
                        _messageId
                }, ids);
    }

    @Test
    void entryToObject_honorsEntryOffset()
    {
        final DatabaseEntry entry = encodeWithOffset(_uuid, _messageId, 9);
        final long[] key = QueueEntryBinding.readIds(entry);

        assertEquals(_uuid, new UUID(key[0], key[1]));
        assertEquals(_messageId, key[2]);
    }

    @Test
    void objectToEntry_preservesLexicographicOrderingForSameQueueId()
    {
        final UUID queueId = UUID.randomUUID();
        final DatabaseEntry key0 = encode(queueId, 0L);
        final DatabaseEntry key1 = encode(queueId, 1L);

        final int comparison = compareUnsignedLexicographically(key0, key1, 24);
        assertTrue(comparison < 0, "Expected messageId=0 key to sort before messageId=1 key");
    }

    @Test
    void readIdsInto_populatesTargetArray_andHonoursEntryOffset()
    {
        final DatabaseEntry entry = encodeWithOffset(_uuid, _messageId, 13);
        final long[] target = new long[3];
        QueueEntryBinding.readIdsInto(entry, target);

        assertArrayEquals(new long[]
                {
                        _uuid.getMostSignificantBits(),
                        _uuid.getLeastSignificantBits(),
                        _messageId
                }, target);
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
}
