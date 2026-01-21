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

import org.apache.qpid.server.store.Settings;

/**
 * Declares common RocksDB configuration attributes.
 *
 * Thread-safety: safe for concurrent access by the configuration model.
 */
public interface RocksDBSettings extends Settings
{
    /**
     * Context key for createIfMissing.
     */
    String ROCKSDB_CREATE_IF_MISSING = "qpid.broker.rocksdb.createIfMissing";

    /**
     * Context key for createMissingColumnFamilies.
     */
    String ROCKSDB_CREATE_MISSING_COLUMN_FAMILIES = "qpid.broker.rocksdb.createMissingColumnFamilies";

    /**
     * Context key for enableStatistics.
     */
    String ROCKSDB_ENABLE_STATISTICS = "qpid.broker.rocksdb.enableStatistics";

    /**
     * Context key for statsDumpPeriodSec.
     */
    String ROCKSDB_STATS_DUMP_PERIOD_SEC = "qpid.broker.rocksdb.statsDumpPeriodSec";

    /**
     * Returns whether the store should be created when it is missing.
     *
     * @return true to create a new store when missing.
     */
    Boolean getCreateIfMissing();

    /**
     * Returns whether missing column families should be created.
     *
     * @return true to create missing column families.
     */
    Boolean getCreateMissingColumnFamilies();

    /**
     * Returns the maximum number of open files or 0 to use RocksDB defaults.
     *
     * @return maximum open files or 0 to use RocksDB defaults.
     */
    Integer getMaxOpenFiles();

    /**
     * Returns the maximum number of background jobs or 0 to use RocksDB defaults.
     *
     * @return maximum background jobs or 0 to use RocksDB defaults.
     */
    Integer getMaxBackgroundJobs();

    /**
     * Returns the maximum number of subcompactions or 0 to use RocksDB defaults.
     *
     * @return maximum subcompactions or 0 to use RocksDB defaults.
     */
    Integer getMaxSubcompactions();

    /**
     * Returns a custom WAL directory or empty to use the store path.
     *
     * @return WAL directory or empty to use the store path.
     */
    String getWalDir();

    /**
     * Returns the bytes per sync or 0 to use RocksDB defaults.
     *
     * @return bytes per sync or 0 to use RocksDB defaults.
     */
    Long getBytesPerSync();

    /**
     * Returns the WAL bytes per sync or 0 to use RocksDB defaults.
     *
     * @return WAL bytes per sync or 0 to use RocksDB defaults.
     */
    Long getWalBytesPerSync();

    /**
     * Returns whether RocksDB statistics are enabled.
     *
     * @return true to enable RocksDB statistics.
     */
    Boolean getEnableStatistics();

    /**
     * Returns the statistics dump period in seconds or 0 to disable periodic dumps.
     *
     * @return statistics dump period in seconds or 0 to disable.
     */
    Integer getStatsDumpPeriodSec();

    /**
     * Returns the max total WAL size or 0 to use RocksDB defaults.
     *
     * @return max total WAL size or 0 to use RocksDB defaults.
     */
    Long getMaxTotalWalSize();

    /**
     * Returns the WAL TTL in seconds or 0 to use RocksDB defaults.
     *
     * @return WAL TTL in seconds or 0 to use RocksDB defaults.
     */
    Integer getWalTtlSeconds();

    /**
     * Returns the WAL size limit in MB or 0 to use RocksDB defaults.
     *
     * @return WAL size limit in MB or 0 to use RocksDB defaults.
     */
    Integer getWalSizeLimitMb();

    /**
     * Returns the write buffer size or 0 to use RocksDB defaults.
     *
     * @return write buffer size or 0 to use RocksDB defaults.
     */
    Long getWriteBufferSize();

    /**
     * Returns the maximum number of write buffers or 0 to use RocksDB defaults.
     *
     * @return maximum number of write buffers or 0 to use RocksDB defaults.
     */
    Integer getMaxWriteBufferNumber();

    /**
     * Returns the minimum number of write buffers to merge or 0 to use RocksDB defaults.
     *
     * @return minimum number of write buffers to merge or 0 to use RocksDB defaults.
     */
    Integer getMinWriteBufferNumberToMerge();

    /**
     * Returns the target file size base or 0 to use RocksDB defaults.
     *
     * @return target file size base or 0 to use RocksDB defaults.
     */
    Long getTargetFileSizeBase();

    /**
     * Returns whether dynamic level bytes are enabled.
     *
     * @return true to enable dynamic level bytes.
     */
    Boolean getLevelCompactionDynamicLevelBytes();

    /**
     * Returns the compaction style or empty to use RocksDB defaults.
     *
     * @return compaction style or empty to use RocksDB defaults.
     */
    String getCompactionStyle();

    /**
     * Returns the compression type or empty to use RocksDB defaults.
     *
     * @return compression type or empty to use RocksDB defaults.
     */
    String getCompressionType();

    /**
     * Returns the block cache size or 0 to use RocksDB defaults.
     *
     * @return block cache size or 0 to use RocksDB defaults.
     */
    Long getBlockCacheSize();

    /**
     * Returns the block size or 0 to use RocksDB defaults.
     *
     * @return block size or 0 to use RocksDB defaults.
     */
    Long getBlockSize();

    /**
     * Returns the bloom filter bits per key or 0 to use RocksDB defaults.
     *
     * @return bloom filter bits per key or 0 to use RocksDB defaults.
     */
    Integer getBloomFilterBitsPerKey();

    /**
     * Returns whether to cache index and filter blocks.
     *
     * @return true to cache index and filter blocks.
     */
    Boolean getCacheIndexAndFilterBlocks();

    /**
     * Returns whether to pin level 0 filter and index blocks in cache.
     *
     * @return true to pin level 0 filter and index blocks in cache.
     */
    Boolean getPinL0FilterAndIndexBlocksInCache();
}
