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

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.store.StoreException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class RocksDBCodecTest
{
    @Test
    void readVarIntThrowsOnTruncatedData()
    {
        RocksDBCodec.Cursor cursor = new RocksDBCodec.Cursor();
        byte[] truncated = { (byte) 0x80 };
        assertThrows(StoreException.class,
                     () -> RocksDBCodec.readVarInt(truncated, cursor),
                     "Expected StoreException on truncated varint");
    }

    @Test
    void readVarLongThrowsOnTruncatedData()
    {
        RocksDBCodec.Cursor cursor = new RocksDBCodec.Cursor();
        byte[] truncated = { (byte) 0x80 };
        assertThrows(StoreException.class,
                     () -> RocksDBCodec.readVarLong(truncated, cursor),
                     "Expected StoreException on truncated varlong");
    }
}
