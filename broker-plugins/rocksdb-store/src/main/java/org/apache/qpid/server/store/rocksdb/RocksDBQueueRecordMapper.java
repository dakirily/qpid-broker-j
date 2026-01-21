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
package org.apache.qpid.server.store.rocksdb;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.BitSet;
import java.util.UUID;

import org.apache.qpid.server.store.StoreException;

final class RocksDBQueueRecordMapper
{
    private static final byte QUEUE_SEGMENT_VERSION = 1;
    private static final byte QUEUE_STATE_VERSION = 1;

    private RocksDBQueueRecordMapper()
    {
    }

    static byte[] encodeQueueState(final long headSegment, final long tailSegment)
    {
        byte[] data = new byte[1 + Long.BYTES + Long.BYTES];
        data[0] = QUEUE_STATE_VERSION;
        writeLong(data, 1, headSegment);
        writeLong(data, 1 + Long.BYTES, tailSegment);
        return data;
    }

    static QueueState decodeQueueState(final byte[] data)
    {
        if (data.length != 1 + Long.BYTES + Long.BYTES)
        {
            throw new StoreException("Invalid queue state length: " + data.length);
        }
        byte version = data[0];
        if (version != QUEUE_STATE_VERSION)
        {
            throw new StoreException("Unsupported queue state version " + version);
        }
        long head = readLong(data, 1);
        long tail = readLong(data, 1 + Long.BYTES);
        return new QueueState(head, tail);
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

    private static long readLong(final byte[] data, final int offset)
    {
        return ((long) (data[offset] & 0xFF) << 56)
               | ((long) (data[offset + 1] & 0xFF) << 48)
               | ((long) (data[offset + 2] & 0xFF) << 40)
               | ((long) (data[offset + 3] & 0xFF) << 32)
               | ((long) (data[offset + 4] & 0xFF) << 24)
               | ((long) (data[offset + 5] & 0xFF) << 16)
               | ((long) (data[offset + 6] & 0xFF) << 8)
               | ((long) (data[offset + 7] & 0xFF));
    }

    static byte[] encodeQueueSegment(final QueueSegment segment)
    {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(QUEUE_SEGMENT_VERSION);
        RocksDBCodec.writeVarInt(stream, segment._entryCount);
        RocksDBCodec.writeVarInt(stream, segment._ackedCount);
        RocksDBCodec.writeVarLong(stream, segment._messageIds[0]);
        for (int i = 1; i < segment._entryCount; i++)
        {
            long delta = segment._messageIds[i] - segment._messageIds[i - 1];
            RocksDBCodec.writeVarLong(stream, delta);
        }
        byte[] ackBytes = segment._acked.toByteArray();
        RocksDBCodec.writeVarInt(stream, ackBytes.length);
        stream.write(ackBytes, 0, ackBytes.length);
        return stream.toByteArray();
    }

    static QueueSegment decodeQueueSegment(final byte[] data)
    {
        if (data.length == 0)
        {
            throw new StoreException("Empty queue segment");
        }
        RocksDBCodec.Cursor cursor = new RocksDBCodec.Cursor();
        byte version = data[cursor._position++];
        if (version != QUEUE_SEGMENT_VERSION)
        {
            throw new StoreException("Unsupported queue segment version " + version);
        }
        int entryCount = RocksDBCodec.readVarInt(data, cursor);
        int ackedCount = RocksDBCodec.readVarInt(data, cursor);
        if (entryCount <= 0)
        {
            throw new StoreException("Invalid queue segment entry count: " + entryCount);
        }
        long[] messageIds = new long[entryCount];
        long value = RocksDBCodec.readVarLong(data, cursor);
        messageIds[0] = value;
        for (int i = 1; i < entryCount; i++)
        {
            value += RocksDBCodec.readVarLong(data, cursor);
            if (value <= messageIds[i - 1])
            {
                throw new StoreException("Non-increasing message id in queue segment");
            }
            messageIds[i] = value;
        }
        int ackBytesLength = RocksDBCodec.readVarInt(data, cursor);
        if (ackBytesLength < 0 || cursor._position + ackBytesLength > data.length)
        {
            throw new StoreException("Invalid ack bitmap length: " + ackBytesLength);
        }
        byte[] ackBytes = Arrays.copyOfRange(data, cursor._position, cursor._position + ackBytesLength);
        BitSet acked = BitSet.valueOf(ackBytes);
        if (acked.length() > entryCount)
        {
            throw new StoreException("Ack bitmap has bits beyond entry count");
        }
        if (ackedCount == entryCount && acked.nextClearBit(0) < entryCount)
        {
            throw new StoreException("Ack count indicates full segment but bitmap has gaps");
        }
        int actualAckedCount = acked.cardinality();
        if (ackedCount > entryCount || actualAckedCount != ackedCount)
        {
            throw new StoreException("Invalid ack count: stored=" + ackedCount
                                     + " actual=" + actualAckedCount
                                     + " entryCount=" + entryCount);
        }
        return new QueueSegment(messageIds, acked, entryCount, ackedCount);
    }

    static BitSet insertAckBit(final BitSet acked, final int index)
    {
        if (index <= 0)
        {
            BitSet shifted = new BitSet();
            for (int bit = acked.nextSetBit(0); bit >= 0; bit = acked.nextSetBit(bit + 1))
            {
                shifted.set(bit + 1);
            }
            return shifted;
        }
        BitSet shifted = new BitSet();
        shifted.or(acked.get(0, index));
        for (int bit = acked.nextSetBit(index); bit >= 0; bit = acked.nextSetBit(bit + 1))
        {
            shifted.set(bit + 1);
        }
        return shifted;
    }
}

final class QueueSegmentKey
{
    final UUID _queueId;
    final long _segmentNo;

    QueueSegmentKey(final UUID queueId, final long segmentNo)
    {
        _queueId = queueId;
        _segmentNo = segmentNo;
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
        QueueSegmentKey that = (QueueSegmentKey) object;
        return _segmentNo == that._segmentNo && _queueId.equals(that._queueId);
    }

    @Override
    public int hashCode()
    {
        int result = _queueId.hashCode();
        result = 31 * result + Long.hashCode(_segmentNo);
        return result;
    }
}

final class QueueSegment
{
    long[] _messageIds;
    BitSet _acked;
    int _entryCount;
    int _ackedCount;

    QueueSegment(final long[] messageIds, final BitSet acked, final int entryCount, final int ackedCount)
    {
        _messageIds = messageIds;
        _acked = acked;
        _entryCount = entryCount;
        _ackedCount = ackedCount;
    }
}

final class QueueState
{
    long _headSegment;
    long _tailSegment;
    boolean _headDirty;
    boolean _tailDirty;
    boolean _exists;

    QueueState(final long headSegment, final long tailSegment)
    {
        _headSegment = headSegment;
        _tailSegment = tailSegment;
        _exists = true;
    }
}
