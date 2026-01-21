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

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Statistics;

/**
 * Builds RocksDB options from configured settings.
 *
 * Thread-safety: safe for concurrent access.
 */
public final class RocksDBOptionsFactory
{
    /**
     * Prevents instantiation.
     */
    private RocksDBOptionsFactory()
    {
    }

    /**
     * Creates database options from the provided settings.
     *
     * @param settings RocksDB settings.
     *
     * @return the database options.
     */
    public static DBOptions createDbOptions(final RocksDBSettings settings)
    {
        DBOptions options = new DBOptions();
        applyBoolean(settings.getCreateIfMissing(), options::setCreateIfMissing);
        applyBoolean(settings.getCreateMissingColumnFamilies(), options::setCreateMissingColumnFamilies);
        applyInt(settings.getMaxOpenFiles(), options::setMaxOpenFiles);
        applyInt(settings.getMaxBackgroundJobs(), options::setMaxBackgroundJobs);
        applyInt(settings.getMaxSubcompactions(), options::setMaxSubcompactions);
        applyString(settings.getWalDir(), options::setWalDir);
        applyLong(settings.getBytesPerSync(), options::setBytesPerSync);
        applyLong(settings.getWalBytesPerSync(), options::setWalBytesPerSync);
        applyStatistics(settings.getEnableStatistics(), options);
        if (Boolean.TRUE.equals(settings.getEnableStatistics()))
        {
            applyInt(settings.getStatsDumpPeriodSec(), options::setStatsDumpPeriodSec);
        }
        applyLong(settings.getMaxTotalWalSize(), options::setMaxTotalWalSize);
        applyInt(settings.getWalTtlSeconds(), options::setWalTtlSeconds);
        applyInt(settings.getWalSizeLimitMb(), options::setWalSizeLimitMB);
        return options;
    }

    /**
     * Creates column family options from the provided settings.
     *
     * @param settings RocksDB settings.
     *
     * @return the column family options.
     */
    public static ColumnFamilyOptions createColumnFamilyOptions(final RocksDBSettings settings)
    {
        ColumnFamilyOptions options = new ColumnFamilyOptions();
        applyLong(settings.getWriteBufferSize(), options::setWriteBufferSize);
        applyInt(settings.getMaxWriteBufferNumber(), options::setMaxWriteBufferNumber);
        applyInt(settings.getMinWriteBufferNumberToMerge(), options::setMinWriteBufferNumberToMerge);
        applyLong(settings.getTargetFileSizeBase(), options::setTargetFileSizeBase);
        applyBoolean(settings.getLevelCompactionDynamicLevelBytes(),
                     options::setLevelCompactionDynamicLevelBytes);
        applyCompactionStyle(settings.getCompactionStyle(), options);
        applyCompressionType(settings.getCompressionType(), options);
        applyBlockBasedTableConfig(settings, options);
        return options;
    }

    /**
     * Applies block-based table configuration settings.
     *
     * @param settings RocksDB settings.
     * @param options column family options.
     */
    private static void applyBlockBasedTableConfig(final RocksDBSettings settings,
                                                   final ColumnFamilyOptions options)
    {
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        applyLong(settings.getBlockSize(), tableConfig::setBlockSize);
        applyBoolean(settings.getCacheIndexAndFilterBlocks(), tableConfig::setCacheIndexAndFilterBlocks);
        applyBoolean(settings.getPinL0FilterAndIndexBlocksInCache(),
                     tableConfig::setPinL0FilterAndIndexBlocksInCache);
        applyBloomFilter(settings.getBloomFilterBitsPerKey(), tableConfig);
        applyBlockCache(settings.getBlockCacheSize(), tableConfig);
        options.setTableFormatConfig(tableConfig);
    }

    /**
     * Applies the block cache configuration.
     *
     * @param cacheSize cache size.
     * @param tableConfig table config.
     */
    private static void applyBlockCache(final Long cacheSize, final BlockBasedTableConfig tableConfig)
    {
        if (cacheSize != null && cacheSize.longValue() > 0)
        {
            tableConfig.setBlockCache(new LRUCache(cacheSize.longValue()));
        }
    }

    /**
     * Applies RocksDB statistics configuration.
     *
     * @param enableStatistics true to enable statistics.
     * @param options database options.
     */
    private static void applyStatistics(final Boolean enableStatistics, final DBOptions options)
    {
        if (enableStatistics != null && enableStatistics.booleanValue())
        {
            options.setStatistics(new Statistics());
        }
    }

    /**
     * Applies bloom filter configuration.
     *
     * @param bitsPerKey bloom filter bits per key.
     * @param tableConfig table config.
     */
    private static void applyBloomFilter(final Integer bitsPerKey, final BlockBasedTableConfig tableConfig)
    {
        if (bitsPerKey != null && bitsPerKey.intValue() > 0)
        {
            tableConfig.setFilterPolicy(new BloomFilter(bitsPerKey.intValue()));
        }
    }

    /**
     * Applies the compaction style configuration.
     *
     * @param compactionStyle compaction style name.
     * @param options column family options.
     */
    private static void applyCompactionStyle(final String compactionStyle, final ColumnFamilyOptions options)
    {
        if (compactionStyle == null || compactionStyle.isEmpty())
        {
            return;
        }

        options.setCompactionStyle(RocksDBOptionValues.parseCompactionStyle(compactionStyle));
    }

    /**
     * Applies the compression type configuration.
     *
     * @param compressionType compression type name.
     * @param options column family options.
     */
    private static void applyCompressionType(final String compressionType, final ColumnFamilyOptions options)
    {
        if (compressionType == null || compressionType.isEmpty())
        {
            return;
        }

        options.setCompressionType(RocksDBOptionValues.parseCompressionType(compressionType));
    }

    /**
     * Applies a boolean value to a setter.
     *
     * @param value value to apply.
     * @param setter setter to update.
     */
    private static void applyBoolean(final Boolean value, final BooleanSetter setter)
    {
        if (value != null)
        {
            setter.apply(value.booleanValue());
        }
    }

    /**
     * Applies an integer value to a setter.
     *
     * @param value value to apply.
     * @param setter setter to update.
     */
    private static void applyInt(final Integer value, final IntSetter setter)
    {
        if (value != null && value.intValue() > 0)
        {
            setter.apply(value.intValue());
        }
    }

    /**
     * Applies a long value to a setter.
     *
     * @param value value to apply.
     * @param setter setter to update.
     */
    private static void applyLong(final Long value, final LongSetter setter)
    {
        if (value != null && value.longValue() > 0L)
        {
            setter.apply(value.longValue());
        }
    }

    /**
     * Applies a string value to a setter.
     *
     * @param value value to apply.
     * @param setter setter to update.
     */
    private static void applyString(final String value, final StringSetter setter)
    {
        if (value != null && !value.isEmpty())
        {
            setter.apply(value);
        }
    }

    /**
     * Defines a boolean setter.
     */
    private interface BooleanSetter
    {
        void apply(boolean value);
    }

    /**
     * Defines an integer setter.
     */
    private interface IntSetter
    {
        void apply(int value);
    }

    /**
     * Defines a long setter.
     */
    private interface LongSetter
    {
        void apply(long value);
    }

    /**
     * Defines a string setter.
     */
    private interface StringSetter
    {
        void apply(String value);
    }
}
