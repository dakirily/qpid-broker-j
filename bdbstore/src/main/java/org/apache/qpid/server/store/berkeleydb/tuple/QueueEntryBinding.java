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

import java.util.UUID;

import com.sleepycat.je.DatabaseEntry;

import org.apache.qpid.server.store.berkeleydb.AbstractBDBMessageStore;
import org.apache.qpid.server.util.CachingUUIDFactory;


/**
 * Delivery DB key binding.
 *
 * <p>Key layout: {@code [queueId UUID (16 bytes)][messageId (8 bytes)]}. Values are encoded as unsigned longs
 * to preserve lexicographic ordering. {@link #validateKey(DatabaseEntry)} performs bounds checks to surface
 * corrupted keys early.
 */
public class QueueEntryBinding
{
    private static final int QUEUE_ID_SIZE = 2 * Long.BYTES;
    public static final int KEY_SIZE = QUEUE_ID_SIZE + Long.BYTES;

    private QueueEntryBinding()
    {
    }

    public static void objectToEntry(final UUID queueId, final long messageId, final DatabaseEntry entry)
    {
        final byte[] existingData = entry.getData();
        final byte[] output = existingData != null && existingData.length >= KEY_SIZE
                ? existingData
                : new byte[KEY_SIZE];
        writeQueueId(queueId, output);
        writeUnsignedLong(messageId ^ 0x8000000000000000L, output, QUEUE_ID_SIZE);
        entry.setData(output, 0, KEY_SIZE);
    }

    public static byte[] queuePrefix(final UUID queueId)
    {
        final byte[] prefix = new byte[QUEUE_ID_SIZE];
        writeQueueId(queueId, prefix);
        return prefix;
    }

    private static void writeQueueId(final UUID queueId, final byte[] data)
    {
        writeUnsignedLong(queueId.getMostSignificantBits() ^ 0x8000000000000000L, data, 0);
        writeUnsignedLong(queueId.getLeastSignificantBits() ^ 0x8000000000000000L, data, Long.BYTES);
    }

    private static void writeUnsignedLong(final long val, final byte[] data, int offset)
    {
        data[offset++] = (byte) (val >>> 56);
        data[offset++] = (byte) (val >>> 48);
        data[offset++] = (byte) (val >>> 40);
        data[offset++] = (byte) (val >>> 32);
        data[offset++] = (byte) (val >>> 24);
        data[offset++] = (byte) (val >>> 16);
        data[offset++] = (byte) (val >>> 8);
        data[offset] = (byte) val;
    }

    private static long readUnsignedLong(final byte[] data, int offset)
    {
        return (((long) data[offset++] & 0xffL) << 56)
               | (((long) data[offset++] & 0xffL) << 48)
               | (((long) data[offset++] & 0xffL) << 40)
               | (((long) data[offset++] & 0xffL) << 32)
               | (((long) data[offset++] & 0xffL) << 24)
               | (((long) data[offset++] & 0xffL) << 16)
               | (((long) data[offset++] & 0xffL) << 8)
               | ((long) data[offset] & 0xffL) ;
    }

    public static long readMessageId(final DatabaseEntry entry)
    {
        validateKey(entry);
        final byte[] data = entry.getData();
        final int offset = entry.getOffset();
        return readUnsignedLong(data, offset + 16) ^ 0x8000000000000000L;
    }

    public static void writeMessageId(final long messageId, final DatabaseEntry entry)
    {
        writeUnsignedLong(messageId ^ 0x8000000000000000L, entry.getData(), 0);
    }

    public static boolean matchesQueueId(final DatabaseEntry entry, final UUID queueId)
    {
        if (queueId == null)
        {
            throw new IllegalArgumentException("Queue id is null");
        }

        validateKey(entry);
        final byte[] data = entry.getData();
        final int offset = entry.getOffset();

        // compare encoded values directly to avoid creating UUID objects
        return readUnsignedLong(data, offset) == (queueId.getMostSignificantBits() ^ 0x8000000000000000L)
                && readUnsignedLong(data, offset + 8) == (queueId.getLeastSignificantBits() ^ 0x8000000000000000L);
    }

    public static AbstractBDBMessageStore.BDBEnqueueRecord readIds(final CachingUUIDFactory uuidFactory, final DatabaseEntry entry)
    {
        validateKey(entry);
        final byte[] data = entry.getData();
        final int offset = entry.getOffset();
        final long msb = readUnsignedLong(data, offset) ^ 0x8000000000000000L;
        final long lsb = readUnsignedLong(data, offset + 8) ^ 0x8000000000000000L;
        final long messageId = readUnsignedLong(data, offset + 16) ^ 0x8000000000000000L;
        return new AbstractBDBMessageStore.BDBEnqueueRecord(uuidFactory.createUuidFromBits(msb, lsb), messageId);
    }

    public static void validateKey(final DatabaseEntry entry)
    {
        if (entry == null)
        {
            throw new IllegalArgumentException("Entry is null");
        }

        final byte[] data = entry.getData();
        if (data == null)
        {
            throw new IllegalArgumentException("Entry data is null");
        }

        final int offset = entry.getOffset();
        final int size = entry.getSize();

        if (offset < 0 || size != KEY_SIZE || offset > data.length - size)
        {
            throw new IllegalArgumentException("Invalid deliveryDb key: expected exactly " + KEY_SIZE
                    + " bytes from offset. offset = " + offset + ", size = " + size
                    + ", data.length = " + data.length);
        }
    }

    /**
     * Decodes one sorted DELIVERY scan, reusing the UUID for adjacent records of the same queue.
     * Each scan invocation must own its decoder and retain it across pages and cursor reopenings.
     */
    public static final class OrderedDecoder
    {
        private long _mostSignificantBits;
        private long _leastSignificantBits;
        private UUID _queueId;

        public AbstractBDBMessageStore.BDBEnqueueRecord readIds(final DatabaseEntry entry)
        {
            validateKey(entry);
            final byte[] data = entry.getData();
            final int offset = entry.getOffset();
            final long msb = readUnsignedLong(data, offset) ^ 0x8000000000000000L;
            final long lsb = readUnsignedLong(data, offset + 8) ^ 0x8000000000000000L;
            final long messageId = readUnsignedLong(data, offset + 16) ^ 0x8000000000000000L;
            if (_queueId == null || msb != _mostSignificantBits || lsb != _leastSignificantBits)
            {
                _queueId = new UUID(msb, lsb);
                _mostSignificantBits = msb;
                _leastSignificantBits = lsb;
            }
            return new AbstractBDBMessageStore.BDBEnqueueRecord(_queueId, messageId);
        }
    }
}
