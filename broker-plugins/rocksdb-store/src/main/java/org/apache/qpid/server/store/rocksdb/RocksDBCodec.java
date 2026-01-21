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

import org.apache.qpid.server.store.StoreException;

final class RocksDBCodec
{
    static final class Cursor
    {
        int _position;
    }

    private RocksDBCodec()
    {
    }

    static void writeVarInt(final ByteArrayOutputStream output, int value)
    {
        while ((value & ~0x7F) != 0)
        {
            output.write((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        output.write((byte) value);
    }

    static void writeVarLong(final ByteArrayOutputStream output, long value)
    {
        while ((value & ~0x7FL) != 0)
        {
            output.write((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        output.write((byte) value);
    }

    static int readVarInt(final byte[] data, final Cursor cursor)
    {
        int value = 0;
        int shift = 0;
        while (true)
        {
            if (cursor._position >= data.length)
            {
                throw new StoreException("Insufficient data for varint");
            }
            byte b = data[cursor._position++];
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return value;
            }
            shift += 7;
            if (shift > 28)
            {
                throw new StoreException("Varint too long");
            }
        }
    }

    static long readVarLong(final byte[] data, final Cursor cursor)
    {
        long value = 0L;
        int shift = 0;
        while (true)
        {
            if (cursor._position >= data.length)
            {
                throw new StoreException("Insufficient data for varlong");
            }
            byte b = data[cursor._position++];
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return value;
            }
            shift += 7;
            if (shift > 63)
            {
                throw new StoreException("Varlong too long");
            }
        }
    }
}
