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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.MutableColumnFamilyOptions;
import org.rocksdb.MutableDBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;

import org.apache.qpid.server.store.StoreException;

/**
 * Provides shared management operations for RocksDB-backed containers.
 *
 * Thread-safety: stateless utility, safe for concurrent use.
 */
public final class RocksDBManagementSupport
{
    private static final List<String> PROPERTY_NAMES = List.of(
            "rocksdb.stats",
            "rocksdb.sstables",
            "rocksdb.cfstats",
            "rocksdb.cfstats-no-file-histogram",
            "rocksdb.num-immutable-mem-table",
            "rocksdb.mem-table-flush-pending",
            "rocksdb.compaction-pending",
            "rocksdb.background-errors",
            "rocksdb.cur-size-active-mem-table",
            "rocksdb.cur-size-all-mem-tables",
            "rocksdb.size-all-mem-tables",
            "rocksdb.num-entries-active-mem-table",
            "rocksdb.num-entries-imm-mem-tables",
            "rocksdb.num-deletes-imm-mem-tables",
            "rocksdb.estimate-num-keys",
            "rocksdb.estimate-live-data-size",
            "rocksdb.total-sst-files-size",
            "rocksdb.live-sst-files-size",
            "rocksdb.base-level",
            "rocksdb.num-running-compactions",
            "rocksdb.num-running-flushes",
            "rocksdb.actual-delayed-write-rate",
            "rocksdb.is-write-stopped",
            "rocksdb.block-cache-capacity",
            "rocksdb.block-cache-usage",
            "rocksdb.block-cache-pinned-usage");

    /**
     * Prevents instantiation.
     */
    private RocksDBManagementSupport()
    {
    }

    /**
     * Applies mutable configuration options to the running database.
     *
     * @param environment RocksDB environment.
     * @param settings settings to apply.
     */
    public static void updateMutableConfig(final RocksDBEnvironment environment, final RocksDBSettings settings)
    {
        RocksDB database = requireDatabase(environment);
        boolean dbOptionsSet = false;
        MutableDBOptions.MutableDBOptionsBuilder dbBuilder = MutableDBOptions.builder();

        if (settings.getMaxOpenFiles() != null && settings.getMaxOpenFiles().intValue() > 0)
        {
            dbBuilder.setMaxOpenFiles(settings.getMaxOpenFiles().intValue());
            dbOptionsSet = true;
        }
        if (settings.getMaxBackgroundJobs() != null && settings.getMaxBackgroundJobs().intValue() > 0)
        {
            dbBuilder.setMaxBackgroundJobs(settings.getMaxBackgroundJobs().intValue());
            dbOptionsSet = true;
        }
        if (settings.getStatsDumpPeriodSec() != null && settings.getStatsDumpPeriodSec().intValue() > 0)
        {
            dbBuilder.setStatsDumpPeriodSec(settings.getStatsDumpPeriodSec().intValue());
            dbOptionsSet = true;
        }
        if (settings.getMaxTotalWalSize() != null && settings.getMaxTotalWalSize().longValue() > 0L)
        {
            dbBuilder.setMaxTotalWalSize(settings.getMaxTotalWalSize().longValue());
            dbOptionsSet = true;
        }
        if (settings.getBytesPerSync() != null && settings.getBytesPerSync().longValue() > 0L)
        {
            dbBuilder.setBytesPerSync(settings.getBytesPerSync().longValue());
            dbOptionsSet = true;
        }
        if (settings.getWalBytesPerSync() != null && settings.getWalBytesPerSync().longValue() > 0L)
        {
            dbBuilder.setWalBytesPerSync(settings.getWalBytesPerSync().longValue());
            dbOptionsSet = true;
        }

        try
        {
            if (dbOptionsSet)
            {
                database.setDBOptions(dbBuilder.build());
            }

            MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder cfBuilder =
                    MutableColumnFamilyOptions.builder();
            boolean cfOptionsSet = false;

            if (settings.getWriteBufferSize() != null && settings.getWriteBufferSize().longValue() > 0L)
            {
                cfBuilder.setWriteBufferSize(settings.getWriteBufferSize().longValue());
                cfOptionsSet = true;
            }
            if (settings.getMaxWriteBufferNumber() != null && settings.getMaxWriteBufferNumber().intValue() > 0)
            {
                cfBuilder.setMaxWriteBufferNumber(settings.getMaxWriteBufferNumber().intValue());
                cfOptionsSet = true;
            }
            if (settings.getTargetFileSizeBase() != null && settings.getTargetFileSizeBase().longValue() > 0L)
            {
                cfBuilder.setTargetFileSizeBase(settings.getTargetFileSizeBase().longValue());
                cfOptionsSet = true;
            }
            if (settings.getCompressionType() != null && !settings.getCompressionType().isEmpty())
            {
                cfBuilder.setCompressionType(RocksDBOptionValues.parseCompressionType(settings.getCompressionType()));
                cfOptionsSet = true;
            }

            if (cfOptionsSet)
            {
                MutableColumnFamilyOptions options = cfBuilder.build();
                for (ColumnFamilyHandle handle : getColumnFamilyHandles(environment))
                {
                    database.setOptions(handle, options);
                }
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to apply RocksDB mutable configuration", e);
        }
    }

    /**
     * Flushes memtables to disk.
     *
     * @param environment RocksDB environment.
     * @param columnFamily column family name or empty for all.
     * @param wait true to wait for flush completion.
     */
    public static void flush(final RocksDBEnvironment environment, final String columnFamily, final boolean wait)
    {
        RocksDB database = requireDatabase(environment);
        try (FlushOptions options = new FlushOptions().setWaitForFlush(wait))
        {
            if (columnFamily == null || columnFamily.isEmpty())
            {
                database.flush(options, getColumnFamilyHandles(environment));
            }
            else
            {
                ColumnFamilyHandle handle = getColumnFamilyHandle(environment, columnFamily);
                database.flush(options, handle);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to flush RocksDB store", e);
        }
    }

    /**
     * Compacts the store data.
     *
     * @param environment RocksDB environment.
     * @param columnFamily column family name or empty for all.
     */
    public static void compactRange(final RocksDBEnvironment environment, final String columnFamily)
    {
        RocksDB database = requireDatabase(environment);
        try
        {
            if (columnFamily == null || columnFamily.isEmpty())
            {
                database.compactRange();
            }
            else
            {
                ColumnFamilyHandle handle = getColumnFamilyHandle(environment, columnFamily);
                database.compactRange(handle);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to compact RocksDB store", e);
        }
    }

    /**
     * Returns properties by prefix.
     *
     * @param environment RocksDB environment.
     * @param propertyPrefix property prefix.
     *
     * @return matching properties.
     */
    public static Map<String, String> getDbProperties(final RocksDBEnvironment environment,
                                                      final String propertyPrefix)
    {
        if (propertyPrefix == null || propertyPrefix.isEmpty())
        {
            return Collections.emptyMap();
        }

        RocksDB database = requireDatabase(environment);
        Map<String, String> properties = new LinkedHashMap<>();
        for (String property : PROPERTY_NAMES)
        {
            if (!property.startsWith(propertyPrefix))
            {
                continue;
            }
            try
            {
                String value = database.getProperty(property);
                if (value != null)
                {
                    properties.put(property, value);
                }
            }
            catch (RocksDBException ignore)
            {
                // Skip unsupported properties.
            }
        }
        return properties;
    }

    /**
     * Returns a single RocksDB property.
     *
     * @param environment RocksDB environment.
     * @param property property name.
     *
     * @return the property value or null.
     */
    public static String getDbProperty(final RocksDBEnvironment environment, final String property)
    {
        if (property == null || property.isEmpty())
        {
            return null;
        }
        RocksDB database = requireDatabase(environment);
        try
        {
            return database.getProperty(property);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to read RocksDB property " + property, e);
        }
    }

    /**
     * Returns RocksDB statistics.
     *
     * @param environment RocksDB environment.
     * @param reset true to reset statistics after reading.
     *
     * @return statistics map.
     */
    public static Map<String, Object> getStatistics(final RocksDBEnvironment environment, final boolean reset)
    {
        Statistics statistics = requireStatistics(environment);
        if (statistics == null)
        {
            return Collections.emptyMap();
        }

        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("rocksdb.stats", statistics.toString());

        if (reset)
        {
            try
            {
                statistics.reset();
            }
            catch (RocksDBException e)
            {
                throw new StoreException("Failed to reset RocksDB statistics", e);
            }
        }

        return stats;
    }

    private static RocksDB requireDatabase(final RocksDBEnvironment environment)
    {
        if (environment == null)
        {
            throw new IllegalStateException("RocksDB environment is not available");
        }
        RocksDB database = environment.getDatabase();
        if (database == null)
        {
            throw new IllegalStateException("RocksDB database is not available");
        }
        return database;
    }

    private static Statistics requireStatistics(final RocksDBEnvironment environment)
    {
        if (environment == null)
        {
            throw new IllegalStateException("RocksDB environment is not available");
        }
        return environment.getStatistics();
    }

    private static ColumnFamilyHandle getColumnFamilyHandle(final RocksDBEnvironment environment,
                                                            final String columnFamily)
    {
        for (RocksDBColumnFamily family : RocksDBColumnFamily.values())
        {
            if (family.getName().equalsIgnoreCase(columnFamily))
            {
                ColumnFamilyHandle handle = environment.getColumnFamilyHandle(family);
                if (handle != null)
                {
                    return handle;
                }
            }
        }
        throw new IllegalArgumentException("Unknown RocksDB column family: " + columnFamily);
    }

    private static List<ColumnFamilyHandle> getColumnFamilyHandles(final RocksDBEnvironment environment)
    {
        List<ColumnFamilyHandle> handles = new ArrayList<>(RocksDBColumnFamily.values().length);
        for (RocksDBColumnFamily family : RocksDBColumnFamily.values())
        {
            ColumnFamilyHandle handle = environment.getColumnFamilyHandle(family);
            if (handle != null)
            {
                handles.add(handle);
            }
        }
        return handles;
    }
}
