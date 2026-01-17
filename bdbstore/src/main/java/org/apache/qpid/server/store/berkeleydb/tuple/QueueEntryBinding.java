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
    public static final int KEY_SIZE = 24;

    private QueueEntryBinding()
    {
    }

    public static void objectToEntry(final UUID queueId, long messageId, final DatabaseEntry entry)
    {
        byte[] output = entry.getData() != null ? entry.getData() : new byte[KEY_SIZE];
        writeUnsignedLong(queueId.getMostSignificantBits() ^ 0x8000000000000000L, output, 0);
        writeUnsignedLong(queueId.getLeastSignificantBits() ^ 0x8000000000000000L, output, 8);
        writeUnsignedLong(messageId ^ 0x8000000000000000L, output, 16);
        entry.setData(output);
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
        if (entry == null || queueId == null)
        {
            return false;
        }

        final byte[] data = entry.getData();
        if (data == null)
        {
            return false;
        }

        final int offset = entry.getOffset();
        final int size = entry.getSize();
        if (offset < 0 || size < KEY_SIZE || offset + KEY_SIZE > data.length)
        {
            return false;
        }

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

    private static void validateKey(final DatabaseEntry entry)
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

        if (offset < 0 || size < KEY_SIZE || offset + KEY_SIZE > data.length)
        {
            throw new IllegalArgumentException("Invalid deliveryDb key: expected at least " + KEY_SIZE +
                    " bytes from offset. offset = " + offset +", size = " + size + ", data.length = " + data.length);
        }
    }
}
