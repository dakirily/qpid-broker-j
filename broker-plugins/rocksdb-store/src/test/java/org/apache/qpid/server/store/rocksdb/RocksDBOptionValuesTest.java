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

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.TxnDBWritePolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RocksDBOptionValuesTest
{
    @Test
    void parseCompactionStyleAcceptsKnownValue()
    {
        assertEquals(CompactionStyle.LEVEL,
                     RocksDBOptionValues.parseCompactionStyle("level"));
    }

    @Test
    void parseCompactionStyleRejectsUnknown()
    {
        assertThrows(IllegalArgumentException.class,
                     () -> RocksDBOptionValues.parseCompactionStyle("not-a-style"));
    }

    @Test
    void parseCompressionTypeAcceptsKnownValue()
    {
        assertEquals(CompressionType.LZ4_COMPRESSION,
                     RocksDBOptionValues.parseCompressionType("lz4_compression"));
    }

    @Test
    void parseCompressionTypeRejectsUnknown()
    {
        assertThrows(IllegalArgumentException.class,
                     () -> RocksDBOptionValues.parseCompressionType("not-a-type"));
    }

    @Test
    void parseTxnWritePolicyAcceptsKnownValue()
    {
        assertEquals(TxnDBWritePolicy.WRITE_COMMITTED,
                     RocksDBOptionValues.parseTxnDbWritePolicy("write_committed"));
    }

    @Test
    void parseTxnWritePolicyRejectsUnknown()
    {
        assertThrows(IllegalArgumentException.class,
                     () -> RocksDBOptionValues.parseTxnDbWritePolicy("not-a-policy"));
    }
}
