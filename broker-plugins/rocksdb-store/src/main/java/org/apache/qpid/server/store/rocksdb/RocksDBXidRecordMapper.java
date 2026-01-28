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

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.Transaction.StoredXidRecord;

final class RocksDBXidRecordMapper
{
    private static final byte XID_FORMAT_VERSION = 1;

    interface RecordFactory
    {
        Transaction.EnqueueRecord createEnqueue(UUID queueId, long messageId);

        Transaction.DequeueRecord createDequeue(UUID queueId, long messageId);
    }

    private RocksDBXidRecordMapper()
    {
    }

    static byte[] encodeXidKey(final long format, final byte[] globalId, final byte[] branchId)
    {
        try
        {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] longBytes = new byte[Long.BYTES];
            writeLong(longBytes, 0, format);
            digest.update(longBytes);
            byte[] intBytes = new byte[Integer.BYTES];
            writeInt(intBytes, 0, globalId == null ? 0 : globalId.length);
            digest.update(intBytes);
            if (globalId != null)
            {
                digest.update(globalId);
            }
            writeInt(intBytes, 0, branchId == null ? 0 : branchId.length);
            digest.update(intBytes);
            if (branchId != null)
            {
                digest.update(branchId);
            }
            byte[] hash = digest.digest();
            return Arrays.copyOf(hash, 16);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new StoreException("Failed to hash XID key", e);
        }
    }

    static byte[] encodeXidValue(final long format,
                                 final byte[] globalId,
                                 final byte[] branchId,
                                 final Transaction.EnqueueRecord[] enqueues,
                                 final Transaction.DequeueRecord[] dequeues)
    {
        int enqueueCount = enqueues == null ? 0 : enqueues.length;
        int dequeueCount = dequeues == null ? 0 : dequeues.length;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(XID_FORMAT_VERSION);
        RocksDBCodec.writeVarLong(stream, format);
        RocksDBCodec.writeVarInt(stream, globalId == null ? 0 : globalId.length);
        if (globalId != null && globalId.length > 0)
        {
            stream.write(globalId, 0, globalId.length);
        }
        RocksDBCodec.writeVarInt(stream, branchId == null ? 0 : branchId.length);
        if (branchId != null && branchId.length > 0)
        {
            stream.write(branchId, 0, branchId.length);
        }
        RocksDBCodec.writeVarInt(stream, enqueueCount);
        if (enqueueCount > 0)
        {
            for (Transaction.EnqueueRecord record : enqueues)
            {
                writeXidRecord(stream, record.getResource().getId(),
                               record.getMessage().getMessageNumber());
            }
        }
        RocksDBCodec.writeVarInt(stream, dequeueCount);
        if (dequeueCount > 0)
        {
            for (Transaction.DequeueRecord record : dequeues)
            {
                MessageEnqueueRecord enqueueRecord = record.getEnqueueRecord();
                writeXidRecord(stream, enqueueRecord.getQueueId(),
                               enqueueRecord.getMessageNumber());
            }
        }
        return stream.toByteArray();
    }

    static XidRecordData decodeXidValue(final byte[] data, final RecordFactory recordFactory)
    {
        RocksDBCodec.Cursor cursor = new RocksDBCodec.Cursor();
        if (data.length == 0)
        {
            throw new StoreException("Empty XID value");
        }
        byte version = data[cursor._position++];
        if (version != XID_FORMAT_VERSION)
        {
            throw new StoreException("Unsupported XID value version: " + version);
        }
        long format = RocksDBCodec.readVarLong(data, cursor);
        byte[] globalId = readVarBytes(data, cursor);
        byte[] branchId = readVarBytes(data, cursor);
        StoredXidRecord xid = new RocksDBStoredXidRecord(format, globalId, branchId);

        int enqueueCount = RocksDBCodec.readVarInt(data, cursor);
        List<Transaction.EnqueueRecord> enqueues = new ArrayList<>(enqueueCount);
        for (int i = 0; i < enqueueCount; i++)
        {
            UUID queueId = readQueueId(data, cursor);
            long messageId = RocksDBCodec.readVarLong(data, cursor);
            enqueues.add(recordFactory.createEnqueue(queueId, messageId));
        }
        int dequeueCount = RocksDBCodec.readVarInt(data, cursor);
        List<Transaction.DequeueRecord> dequeues = new ArrayList<>(dequeueCount);
        for (int i = 0; i < dequeueCount; i++)
        {
            UUID queueId = readQueueId(data, cursor);
            long messageId = RocksDBCodec.readVarLong(data, cursor);
            dequeues.add(recordFactory.createDequeue(queueId, messageId));
        }
        return new XidRecordData(xid, new XidActions(enqueues, dequeues));
    }

    private static void writeXidRecord(final ByteArrayOutputStream output,
                                       final UUID queueId,
                                       final long messageId)
    {
        writeLong(output, queueId.getMostSignificantBits());
        writeLong(output, queueId.getLeastSignificantBits());
        RocksDBCodec.writeVarLong(output, messageId);
    }

    private static UUID readQueueId(final byte[] data, final RocksDBCodec.Cursor cursor)
    {
        long most = readLong(data, cursor);
        long least = readLong(data, cursor);
        return new UUID(most, least);
    }

    private static void writeLong(final ByteArrayOutputStream output, final long value)
    {
        output.write((byte) (value >>> 56));
        output.write((byte) (value >>> 48));
        output.write((byte) (value >>> 40));
        output.write((byte) (value >>> 32));
        output.write((byte) (value >>> 24));
        output.write((byte) (value >>> 16));
        output.write((byte) (value >>> 8));
        output.write((byte) value);
    }

    private static void writeLong(final byte[] data, final int offset, final long value)
    {
        data[offset] = (byte) (value >>> 56);
        data[offset + 1] = (byte) (value >>> 48);
        data[offset + 2] = (byte) (value >>> 40);
        data[offset + 3] = (byte) (value >>> 32);
        data[offset + 4] = (byte) (value >>> 24);
        data[offset + 5] = (byte) (value >>> 16);
        data[offset + 6] = (byte) (value >>> 8);
        data[offset + 7] = (byte) value;
    }

    private static void writeInt(final byte[] data, final int offset, final int value)
    {
        data[offset] = (byte) (value >>> 24);
        data[offset + 1] = (byte) (value >>> 16);
        data[offset + 2] = (byte) (value >>> 8);
        data[offset + 3] = (byte) value;
    }

    private static long readLong(final byte[] data, final RocksDBCodec.Cursor cursor)
    {
        int position = cursor._position;
        if (position + Long.BYTES > data.length)
        {
            throw new StoreException("Insufficient data for long");
        }
        long value = ((long) (data[position] & 0xFF) << 56)
                     | ((long) (data[position + 1] & 0xFF) << 48)
                     | ((long) (data[position + 2] & 0xFF) << 40)
                     | ((long) (data[position + 3] & 0xFF) << 32)
                     | ((long) (data[position + 4] & 0xFF) << 24)
                     | ((long) (data[position + 5] & 0xFF) << 16)
                     | ((long) (data[position + 6] & 0xFF) << 8)
                     | ((long) (data[position + 7] & 0xFF));
        cursor._position += Long.BYTES;
        return value;
    }

    private static byte[] readVarBytes(final byte[] data, final RocksDBCodec.Cursor cursor)
    {
        int length = RocksDBCodec.readVarInt(data, cursor);
        if (length == 0)
        {
            return new byte[0];
        }
        if (cursor._position + length > data.length)
        {
            throw new StoreException("Insufficient data for bytes");
        }
        byte[] bytes = Arrays.copyOfRange(data, cursor._position, cursor._position + length);
        cursor._position += length;
        return bytes;
    }
}

final class RocksDBStoredXidRecord implements Transaction.StoredXidRecord
{
    private final long _format;
    private final byte[] _globalId;
    private final byte[] _branchId;

    RocksDBStoredXidRecord(final long format, final byte[] globalId, final byte[] branchId)
    {
        _format = format;
        _globalId = globalId;
        _branchId = branchId;
    }

    @Override
    public long getFormat()
    {
        return _format;
    }

    @Override
    public byte[] getGlobalId()
    {
        return _globalId;
    }

    @Override
    public byte[] getBranchId()
    {
        return _branchId;
    }

    @Override
    public boolean equals(final Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }

        RocksDBStoredXidRecord that = (RocksDBStoredXidRecord) object;
        return _format == that._format
               && Arrays.equals(_globalId, that._globalId)
               && Arrays.equals(_branchId, that._branchId);
    }

    @Override
    public int hashCode()
    {
        int result = (int) (_format ^ (_format >>> 32));
        result = 31 * result + Arrays.hashCode(_globalId);
        result = 31 * result + Arrays.hashCode(_branchId);
        return result;
    }
}

final class XidRecord
{
    private final byte[] _key;
    private final byte[] _value;

    XidRecord(final byte[] key, final byte[] value)
    {
        _key = key;
        _value = value;
    }

    byte[] getKey()
    {
        return _key;
    }

    byte[] getValue()
    {
        return _value;
    }
}

final class XidRecordData
{
    private final StoredXidRecord _xid;
    private final XidActions _actions;

    XidRecordData(final StoredXidRecord xid, final XidActions actions)
    {
        _xid = xid;
        _actions = actions;
    }

    StoredXidRecord getXid()
    {
        return _xid;
    }

    XidActions getActions()
    {
        return _actions;
    }
}

final class XidActions
{
    private final List<Transaction.EnqueueRecord> _enqueues;
    private final List<Transaction.DequeueRecord> _dequeues;

    XidActions(final List<Transaction.EnqueueRecord> enqueues,
               final List<Transaction.DequeueRecord> dequeues)
    {
        _enqueues = enqueues;
        _dequeues = dequeues;
    }

    List<Transaction.EnqueueRecord> getEnqueues()
    {
        return _enqueues;
    }

    List<Transaction.DequeueRecord> getDequeues()
    {
        return _dequeues;
    }
}
