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

package org.apache.qpid.server.protocol.v1_0.store.rocksdb;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import org.apache.qpid.server.protocol.v1_0.store.LinkStore;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreTestCase;
import org.apache.qpid.server.store.rocksdb.RocksDBContainer;
import org.apache.qpid.server.store.rocksdb.RocksDBEnvironment;
import org.apache.qpid.server.store.rocksdb.RocksDBEnvironmentImpl;
import org.apache.qpid.server.store.rocksdb.RocksDBSettings;
import org.apache.qpid.server.store.rocksdb.RocksDBUtils;

/**
 * Tests the RocksDB-backed link store.
 *
 * Thread-safety: not thread-safe.
 */
class RocksDBLinkStoreTest extends LinkStoreTestCase
{
    private RocksDBEnvironment _environment;

    @TempDir
    private File _storeFolder;

    /**
     * Skips tests when RocksDB native libraries are unavailable.
     *
     * @throws Exception on setup failures.
     */
    @Override
    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        super.setUp();
    }

    /**
     * Creates a RocksDB link store instance.
     *
     * @return the link store.
     */
    @Override
    protected LinkStore createLinkStore()
    {
        RocksDBSettings settings = mock(RocksDBSettings.class);
        when(settings.getCreateIfMissing()).thenReturn(Boolean.TRUE);
        when(settings.getCreateMissingColumnFamilies()).thenReturn(Boolean.TRUE);
        when(settings.getEnableStatistics()).thenReturn(Boolean.FALSE);

        _environment = RocksDBEnvironmentImpl.open(_storeFolder.getAbsolutePath(), settings);

        RocksDBContainer<?> container = mock(RocksDBContainer.class);
        when(container.getRocksDBEnvironment()).thenReturn(_environment);

        return new RocksDBLinkStore(container);
    }

    /**
     * Cleans up the RocksDB environment.
     */
    @Override
    protected void deleteLinkStore()
    {
        if (_environment != null)
        {
            _environment.close();
            _environment = null;
        }
    }
}
