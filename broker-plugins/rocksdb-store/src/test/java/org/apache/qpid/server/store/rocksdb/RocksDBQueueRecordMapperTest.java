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
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.store.StoreException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RocksDBQueueRecordMapperTest
{
    @Test
    void queueStateRoundTrip()
    {
        byte[] encoded = RocksDBQueueRecordMapper.encodeQueueState(5L, 9L);
        QueueState decoded = RocksDBQueueRecordMapper.decodeQueueState(encoded);

        assertEquals(5L, decoded._headSegment);
        assertEquals(9L, decoded._tailSegment);
        assertTrue(decoded._exists);
        assertFalse(decoded._headDirty);
        assertFalse(decoded._tailDirty);
    }

    @Test
    void queueSegmentRoundTrip()
    {
        BitSet acked = new BitSet();
        acked.set(0);
        acked.set(2);
        QueueSegment segment = new QueueSegment(new long[]{1L, 3L, 10L}, acked, 3, 2);

        byte[] encoded = RocksDBQueueRecordMapper.encodeQueueSegment(segment);
        QueueSegment decoded = RocksDBQueueRecordMapper.decodeQueueSegment(encoded);

        assertArrayEquals(new long[]{1L, 3L, 10L}, decoded._messageIds);
        assertEquals(3, decoded._entryCount);
        assertEquals(2, decoded._ackedCount);
        assertTrue(decoded._acked.get(0));
        assertTrue(decoded._acked.get(2));
        assertFalse(decoded._acked.get(1));
    }

    @Test
    void insertAckBitShiftsFollowingBits()
    {
        BitSet acked = new BitSet();
        acked.set(0);
        acked.set(2);

        BitSet shifted = RocksDBQueueRecordMapper.insertAckBit(acked, 1);

        assertTrue(shifted.get(0));
        assertFalse(shifted.get(1));
        assertTrue(shifted.get(3));
        assertFalse(shifted.get(2));
    }

    @Test
    void decodeQueueSegmentThrowsOnAckCountMismatch()
    {
        BitSet acked = new BitSet();
        acked.set(0);
        byte[] ackBytes = acked.toByteArray();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(1);
        RocksDBCodec.writeVarInt(stream, 3);
        RocksDBCodec.writeVarInt(stream, 2);
        RocksDBCodec.writeVarLong(stream, 1L);
        RocksDBCodec.writeVarLong(stream, 2L);
        RocksDBCodec.writeVarLong(stream, 7L);
        RocksDBCodec.writeVarInt(stream, ackBytes.length);
        stream.write(ackBytes, 0, ackBytes.length);

        assertThrows(StoreException.class,
                     () -> RocksDBQueueRecordMapper.decodeQueueSegment(stream.toByteArray()),
                     "Expected StoreException on ack count mismatch");
    }

    @Test
    void decodeQueueSegmentThrowsWhenAckBitsBeyondEntryCount()
    {
        BitSet acked = new BitSet();
        acked.set(5);
        byte[] ackBytes = acked.toByteArray();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(1);
        RocksDBCodec.writeVarInt(stream, 2);
        RocksDBCodec.writeVarInt(stream, 1);
        RocksDBCodec.writeVarLong(stream, 1L);
        RocksDBCodec.writeVarLong(stream, 2L);
        RocksDBCodec.writeVarInt(stream, ackBytes.length);
        stream.write(ackBytes, 0, ackBytes.length);

        StoreException exception = assertThrows(StoreException.class,
                                                () -> RocksDBQueueRecordMapper.decodeQueueSegment(stream.toByteArray()),
                                                "Expected StoreException on ack bits beyond entry count");
        assertTrue(exception.getMessage().contains("bits beyond entry count"));
    }

    @Test
    void decodeQueueSegmentThrowsWhenAckCountFullButBitmapHasGaps()
    {
        BitSet acked = new BitSet();
        acked.set(0);
        acked.set(1);
        byte[] ackBytes = acked.toByteArray();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(1);
        RocksDBCodec.writeVarInt(stream, 3);
        RocksDBCodec.writeVarInt(stream, 3);
        RocksDBCodec.writeVarLong(stream, 1L);
        RocksDBCodec.writeVarLong(stream, 1L);
        RocksDBCodec.writeVarLong(stream, 1L);
        RocksDBCodec.writeVarInt(stream, ackBytes.length);
        stream.write(ackBytes, 0, ackBytes.length);

        StoreException exception = assertThrows(StoreException.class,
                                                () -> RocksDBQueueRecordMapper.decodeQueueSegment(stream.toByteArray()),
                                                "Expected StoreException on ack count full but bitmap has gaps");
        assertTrue(exception.getMessage().contains("full segment"));
    }

    @Test
    void decodeQueueSegmentThrowsOnNonIncreasingMessageIds()
    {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(1);
        RocksDBCodec.writeVarInt(stream, 2);
        RocksDBCodec.writeVarInt(stream, 0);
        RocksDBCodec.writeVarLong(stream, 5L);
        RocksDBCodec.writeVarLong(stream, 0L);
        RocksDBCodec.writeVarInt(stream, 0);

        StoreException exception = assertThrows(StoreException.class,
                                                () -> RocksDBQueueRecordMapper.decodeQueueSegment(stream.toByteArray()),
                                                "Expected StoreException on non-increasing message ids");
        assertTrue(exception.getMessage().contains("Non-increasing"));
    }
}
