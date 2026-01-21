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

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RocksDBOptionsFactoryTest
{
    @Test
    void createDbOptionsAcceptsDefaults()
    {
        RocksDBSettings settings = mock(RocksDBSettings.class);
        when(settings.getCreateIfMissing()).thenReturn(true);
        when(settings.getCreateMissingColumnFamilies()).thenReturn(true);

        DBOptions options = RocksDBOptionsFactory.createDbOptions(settings);

        assertNotNull(options);
        options.close();
    }

    @Test
    void createColumnFamilyOptionsAcceptsDefaults()
    {
        RocksDBSettings settings = mock(RocksDBSettings.class);
        when(settings.getCompactionStyle()).thenReturn("");
        when(settings.getCompressionType()).thenReturn("");

        ColumnFamilyOptions options = RocksDBOptionsFactory.createColumnFamilyOptions(settings);

        assertNotNull(options);
        options.close();
    }

    @Test
    void createTransactionDbOptionsAcceptsDefaults()
    {
        RocksDBSettings settings = mock(RocksDBSettings.class);
        when(settings.getTxnWritePolicy()).thenReturn("");

        TransactionDBOptions options = RocksDBOptionsFactory.createTransactionDbOptions(settings);

        assertNotNull(options);
        options.close();
    }

    @Test
    void createWriteOptionsAcceptsDefaults()
    {
        RocksDBSettings settings = mock(RocksDBSettings.class);
        WriteOptions options = RocksDBOptionsFactory.createWriteOptions(settings);
        assertNotNull(options);
        options.close();
    }

    @Test
    void createColumnFamilyOptionsRejectsUnknownCompression()
    {
        RocksDBSettings settings = mock(RocksDBSettings.class);
        when(settings.getCompressionType()).thenReturn("not-a-type");

        assertThrows(IllegalArgumentException.class,
                     () -> RocksDBOptionsFactory.createColumnFamilyOptions(settings));
    }
}
