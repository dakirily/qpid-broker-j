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

import org.apache.qpid.server.store.berkeleydb.AbstractBDBMessageStore.BDBEnqueueRecord;
import org.apache.qpid.server.util.CachingUUIDFactory;

public class QueueEntryBinding
{
    private static final int QUEUE_ID_SIZE = 2 * Long.BYTES;
    public static final int KEY_SIZE = QUEUE_ID_SIZE + Long.BYTES;

    private QueueEntryBinding()
    {
    }

    public static BDBEnqueueRecord readIds(final CachingUUIDFactory uuidFactory, final DatabaseEntry entry)
    {
        final byte[] data = getDeliveryKeyData(entry);
        final int offset = entry.getOffset();
        final UUID queueId = uuidFactory.createUuidFromBits(readUnsignedLong(data, offset) ^ Long.MIN_VALUE,
                readUnsignedLong(data, offset + Long.BYTES) ^ Long.MIN_VALUE);
        final long messageId = readUnsignedLong(data, offset + QUEUE_ID_SIZE) ^ Long.MIN_VALUE;
        return new BDBEnqueueRecord(queueId, messageId);
    }

    public static boolean matchesQueueId(final DatabaseEntry entry, final UUID queueId)
    {
        final byte[] data = getDeliveryKeyData(entry);
        final int offset = entry.getOffset();
        return readUnsignedLong(data, offset) == (queueId.getMostSignificantBits() ^ Long.MIN_VALUE)
                && readUnsignedLong(data, offset + Long.BYTES) == (queueId.getLeastSignificantBits() ^ Long.MIN_VALUE);
    }

    public static long readMessageId(final DatabaseEntry entry)
    {
        final byte[] data = getDeliveryKeyData(entry);
        return readUnsignedLong(data, entry.getOffset() + QUEUE_ID_SIZE) ^ Long.MIN_VALUE;
    }

    public static void objectToEntry(final UUID queueId, final long messageId, final DatabaseEntry entry)
    {
        final byte[] output = getWriteBuffer(entry, KEY_SIZE);
        writeUnsignedLong(queueId.getMostSignificantBits() ^ Long.MIN_VALUE, output, 0);
        writeUnsignedLong(queueId.getLeastSignificantBits() ^ Long.MIN_VALUE, output, Long.BYTES);
        writeUnsignedLong(messageId ^ Long.MIN_VALUE, output, QUEUE_ID_SIZE);
        entry.setData(output, 0, KEY_SIZE);
    }

    /** Writes a standalone message ID in the same tuple format as JE's {@code LongBinding}. */
    public static void writeMessageId(final long messageId, final DatabaseEntry entry)
    {
        final byte[] output = getWriteBuffer(entry, Long.BYTES);
        writeUnsignedLong(messageId ^ Long.MIN_VALUE, output, 0);
        entry.setData(output, 0, Long.BYTES);
    }

    private static byte[] getWriteBuffer(final DatabaseEntry entry, final int size)
    {
        final byte[] data = entry.getData();
        return data != null && data.length >= size ? data : new byte[size];
    }

    private static byte[] getDeliveryKeyData(final DatabaseEntry entry)
    {
        final byte[] data = entry.getData();
        final int offset = entry.getOffset();
        final int size = entry.getSize();
        if (data == null || offset < 0 || size < KEY_SIZE || offset > data.length - size)
        {
            throw new IllegalArgumentException("Invalid delivery database key range");
        }
        return data;
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
        return (((long) data[offset++] & 0xffL) << 56) |
                (((long) data[offset++] & 0xffL) << 48) |
                (((long) data[offset++] & 0xffL) << 40) |
                (((long) data[offset++] & 0xffL) << 32) |
                (((long) data[offset++] & 0xffL) << 24) |
                (((long) data[offset++] & 0xffL) << 16) |
                (((long) data[offset++] & 0xffL) << 8) |
                ((long) data[offset] & 0xffL);
    }
}
