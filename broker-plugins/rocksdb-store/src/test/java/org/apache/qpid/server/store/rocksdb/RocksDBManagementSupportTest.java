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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies management operations for the RocksDB store.
 *
 * Thread-safety: runs in a single-threaded test context.
 */
public class RocksDBManagementSupportTest
{
    private RocksDBEnvironment _environment;

    @TempDir
    private File _storeFolder;

    /**
     * Closes the RocksDB environment after each test.
     */
    @AfterEach
    public void tearDown()
    {
        if (_environment != null)
        {
            _environment.close();
            _environment = null;
        }
    }

    /**
     * Verifies flush and compact operations for all column families.
     */
    @Test
    public void testFlushAndCompactAllFamilies()
    {
        assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        _environment = openEnvironment(false);
        RocksDBManagementSupport.flush(_environment, "", true);
        RocksDBManagementSupport.compactRange(_environment, "");
    }

    /**
     * Verifies flush reports unknown column families.
     */
    @Test
    public void testFlushUnknownColumnFamily()
    {
        assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        _environment = openEnvironment(false);
        assertThrows(IllegalArgumentException.class,
                     () -> RocksDBManagementSupport.flush(_environment, "missing", true),
                     "Expected flush to reject unknown column family");
    }

    /**
     * Verifies compact reports unknown column families.
     */
    @Test
    public void testCompactUnknownColumnFamily()
    {
        assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        _environment = openEnvironment(false);
        assertThrows(IllegalArgumentException.class,
                     () -> RocksDBManagementSupport.compactRange(_environment, "missing"),
                     "Expected compact to reject unknown column family");
    }

    /**
     * Verifies property lookup by prefix.
     */
    @Test
    public void testGetDbPropertiesPrefix()
    {
        assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        _environment = openEnvironment(false);
        Map<String, String> properties = RocksDBManagementSupport.getDbProperties(_environment, "rocksdb.");
        assertNotNull(properties, "Properties map should not be null");
        for (String key : properties.keySet())
        {
            assertTrue(key.startsWith("rocksdb."), "Unexpected property key " + key);
        }
    }

    /**
     * Verifies empty property names return null.
     */
    @Test
    public void testGetDbPropertyEmpty()
    {
        assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        _environment = openEnvironment(false);
        assertNull(RocksDBManagementSupport.getDbProperty(_environment, null));
        assertNull(RocksDBManagementSupport.getDbProperty(_environment, ""));
    }

    /**
     * Verifies statistics are empty when disabled.
     */
    @Test
    public void testStatisticsDisabled()
    {
        assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        _environment = openEnvironment(false);
        Map<String, Object> stats = RocksDBManagementSupport.getStatistics(_environment, false);
        assertTrue(stats.isEmpty(), "Statistics should be empty when disabled");
    }

    /**
     * Verifies statistics are reported when enabled.
     */
    @Test
    public void testStatisticsEnabled()
    {
        assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        _environment = openEnvironment(true);
        Map<String, Object> stats = RocksDBManagementSupport.getStatistics(_environment, false);
        assertTrue(stats.containsKey("rocksdb.stats"), "Expected rocksdb.stats entry");
    }

    /**
     * Verifies mutable configuration updates apply without failure.
     */
    @Test
    public void testUpdateMutableConfig()
    {
        assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        _environment = openEnvironment(false);
        RocksDBSettings settings = mock(RocksDBSettings.class);
        when(settings.getMaxOpenFiles()).thenReturn(32);
        when(settings.getMaxBackgroundJobs()).thenReturn(2);
        when(settings.getStatsDumpPeriodSec()).thenReturn(60);
        when(settings.getMaxTotalWalSize()).thenReturn(1024L);
        when(settings.getBytesPerSync()).thenReturn(1024L);
        when(settings.getWalBytesPerSync()).thenReturn(1024L);
        when(settings.getWriteBufferSize()).thenReturn(1048576L);
        when(settings.getMaxWriteBufferNumber()).thenReturn(2);
        when(settings.getTargetFileSizeBase()).thenReturn(1048576L);
        when(settings.getCompressionType()).thenReturn("");

        RocksDBManagementSupport.updateMutableConfig(_environment, settings);

        Map<String, String> properties = RocksDBManagementSupport.getDbProperties(_environment, "rocksdb.");
        assertNotNull(properties, "Properties should be available after update");
    }

    /**
     * Opens a RocksDB environment with the requested settings.
     *
     * @param enableStatistics true to enable statistics.
     *
     * @return the RocksDB environment.
     */
    private RocksDBEnvironment openEnvironment(final boolean enableStatistics)
    {
        RocksDBSettings settings = createSettings(enableStatistics);
        return RocksDBEnvironmentImpl.open(_storeFolder.getAbsolutePath(), settings);
    }

    /**
     * Creates RocksDB settings for tests.
     *
     * @param enableStatistics true to enable statistics.
     *
     * @return the settings instance.
     */
    private RocksDBSettings createSettings(final boolean enableStatistics)
    {
        RocksDBSettings settings = mock(RocksDBSettings.class);
        when(settings.getCreateIfMissing()).thenReturn(true);
        when(settings.getCreateMissingColumnFamilies()).thenReturn(true);
        when(settings.getEnableStatistics()).thenReturn(enableStatistics);
        when(settings.getStatsDumpPeriodSec()).thenReturn(0);
        return settings;
    }
}
