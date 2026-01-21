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
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.TxnDBWritePolicy;
import org.rocksdb.WriteOptions;

/**
 * Builds RocksDB options from configured settings.
 * <br>
 * Thread-safety: safe for concurrent access.
 */
public final class RocksDBOptionsFactory
{
    public static final class OptionsResources
    {
        private Statistics _statistics;
        private LRUCache _blockCache;
        private BloomFilter _bloomFilter;
        private BlockBasedTableConfig _tableConfig;

        public Statistics getStatistics()
        {
            return _statistics;
        }

        public LRUCache getBlockCache()
        {
            return _blockCache;
        }

        public BloomFilter getBloomFilter()
        {
            return _bloomFilter;
        }

        public BlockBasedTableConfig getTableConfig()
        {
            return _tableConfig;
        }
    }

    public static final class OptionsBundle
    {
        private final DBOptions _dbOptions;
        private final ColumnFamilyOptions _columnFamilyOptions;
        private final TransactionDBOptions _transactionDbOptions;
        private final OptionsResources _resources;

        private OptionsBundle(final DBOptions dbOptions,
                              final ColumnFamilyOptions columnFamilyOptions,
                              final TransactionDBOptions transactionDbOptions,
                              final OptionsResources resources)
        {
            _dbOptions = dbOptions;
            _columnFamilyOptions = columnFamilyOptions;
            _transactionDbOptions = transactionDbOptions;
            _resources = resources;
        }

        public DBOptions getDbOptions()
        {
            return _dbOptions;
        }

        public ColumnFamilyOptions getColumnFamilyOptions()
        {
            return _columnFamilyOptions;
        }

        public TransactionDBOptions getTransactionDbOptions()
        {
            return _transactionDbOptions;
        }

        public OptionsResources getResources()
        {
            return _resources;
        }
    }
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
        return createDbOptions(settings, null);
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
        return createColumnFamilyOptions(settings, null);
    }

    /**
     * Creates TransactionDB options from the provided settings.
     *
     * @param settings RocksDB settings.
     *
     * @return the transaction db options.
     */
    public static TransactionDBOptions createTransactionDbOptions(final RocksDBSettings settings)
    {
        TransactionDBOptions options = new TransactionDBOptions();
        applyLong(settings.getDefaultLockTimeout(), options::setDefaultLockTimeout);
        applyLong(settings.getTransactionLockTimeout(), options::setTransactionLockTimeout);
        applyLong(settings.getMaxNumLocks(), options::setMaxNumLocks);
        applyLong(settings.getNumStripes(), options::setNumStripes);
        applyTxnWritePolicy(settings.getTxnWritePolicy(), options);
        return options;
    }

    public static OptionsBundle createOptionsBundle(final RocksDBSettings settings)
    {
        OptionsResources resources = new OptionsResources();
        DBOptions dbOptions = createDbOptions(settings, resources);
        ColumnFamilyOptions columnFamilyOptions = createColumnFamilyOptions(settings, resources);
        TransactionDBOptions transactionDbOptions = createTransactionDbOptions(settings);
        return new OptionsBundle(dbOptions, columnFamilyOptions, transactionDbOptions, resources);
    }

    /**
     * Creates write options from the provided settings.
     *
     * @param settings RocksDB settings.
     *
     * @return the write options.
     */
    public static WriteOptions createWriteOptions(final RocksDBSettings settings)
    {
        WriteOptions options = new WriteOptions();
        applyBoolean(settings.getWriteSync(), options::setSync);
        applyBoolean(settings.getDisableWAL(), options::setDisableWAL);
        return options;
    }

    /**
     * Applies block-based table configuration settings.
     *
     * @param settings RocksDB settings.
     * @param resources Options resources.
     */
    private static DBOptions createDbOptions(final RocksDBSettings settings, final OptionsResources resources)
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
        applyStatistics(settings.getEnableStatistics(), options, resources);
        if (Boolean.TRUE.equals(settings.getEnableStatistics()))
        {
            applyInt(settings.getStatsDumpPeriodSec(), options::setStatsDumpPeriodSec);
        }
        applyLong(settings.getMaxTotalWalSize(), options::setMaxTotalWalSize);
        applyInt(settings.getWalTtlSeconds(), options::setWalTtlSeconds);
        applyInt(settings.getWalSizeLimitMb(), options::setWalSizeLimitMB);
        return options;
    }

    private static ColumnFamilyOptions createColumnFamilyOptions(final RocksDBSettings settings,
                                                                 final OptionsResources resources)
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
        applyBlockBasedTableConfig(settings, options, resources);
        return options;
    }

    private static void applyBlockBasedTableConfig(final RocksDBSettings settings,
                                                   final ColumnFamilyOptions options,
                                                   final OptionsResources resources)
    {
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        if (resources != null)
        {
            resources._tableConfig = tableConfig;
        }
        applyLong(settings.getBlockSize(), tableConfig::setBlockSize);
        applyBoolean(settings.getCacheIndexAndFilterBlocks(), tableConfig::setCacheIndexAndFilterBlocks);
        applyBoolean(settings.getPinL0FilterAndIndexBlocksInCache(),
                     tableConfig::setPinL0FilterAndIndexBlocksInCache);
        applyBloomFilter(settings.getBloomFilterBitsPerKey(), tableConfig, resources);
        applyBlockCache(settings.getBlockCacheSize(), tableConfig, resources);
        options.setTableFormatConfig(tableConfig);
    }

    /**
     * Applies the block cache configuration.
     *
     * @param cacheSize cache size.
     * @param tableConfig table config.
     */
    private static void applyBlockCache(final Long cacheSize,
                                        final BlockBasedTableConfig tableConfig,
                                        final OptionsResources resources)
    {
        if (cacheSize != null && cacheSize > 0)
        {
            LRUCache cache = new LRUCache(cacheSize);
            tableConfig.setBlockCache(cache);
            if (resources != null)
            {
                resources._blockCache = cache;
            }
        }
    }

    /**
     * Applies RocksDB statistics configuration.
     *
     * @param enableStatistics true to enable statistics.
     * @param options database options.
     */
    private static void applyStatistics(final Boolean enableStatistics,
                                        final DBOptions options,
                                        final OptionsResources resources)
    {
        if (enableStatistics != null && enableStatistics)
        {
            Statistics statistics = new Statistics();
            options.setStatistics(statistics);
            if (resources != null)
            {
                resources._statistics = statistics;
            }
        }
    }

    /**
     * Applies bloom filter configuration.
     *
     * @param bitsPerKey bloom filter bits per key.
     * @param tableConfig table config.
     */
    private static void applyBloomFilter(final Integer bitsPerKey,
                                         final BlockBasedTableConfig tableConfig,
                                         final OptionsResources resources)
    {
        if (bitsPerKey != null && bitsPerKey > 0)
        {
            BloomFilter filter = new BloomFilter(bitsPerKey);
            tableConfig.setFilterPolicy(filter);
            if (resources != null)
            {
                resources._bloomFilter = filter;
            }
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
     * Applies the TransactionDB write policy.
     *
     * @param writePolicy write policy name.
     * @param options transaction db options.
     */
    private static void applyTxnWritePolicy(final String writePolicy, final TransactionDBOptions options)
    {
        if (writePolicy == null || writePolicy.isEmpty())
        {
            return;
        }
        TxnDBWritePolicy policy = RocksDBOptionValues.parseTxnDbWritePolicy(writePolicy);
        options.setWritePolicy(policy);
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
            setter.apply(value);
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
        if (value != null && value > 0)
        {
            setter.apply(value);
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
        if (value != null && value > 0L)
        {
            setter.apply(value);
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
