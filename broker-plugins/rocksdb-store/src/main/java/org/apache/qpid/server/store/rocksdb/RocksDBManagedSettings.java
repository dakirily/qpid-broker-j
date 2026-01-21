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

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;

/**
 * Declares managed attributes for RocksDB settings.
 * <br>
 * Thread-safety: safe for concurrent access by the configuration model.
 *
 * @param <X> the configured object type.
 */
public interface RocksDBManagedSettings<X extends ConfiguredObject<X>> extends ConfiguredObject<X>, RocksDBSettings
{
    /**
     * Default value for createIfMissing.
     */
    @ManagedContextDefault(name = ROCKSDB_CREATE_IF_MISSING)
    boolean DEFAULT_ROCKSDB_CREATE_IF_MISSING = true;

    /**
     * Default value for createMissingColumnFamilies.
     */
    @ManagedContextDefault(name = ROCKSDB_CREATE_MISSING_COLUMN_FAMILIES)
    boolean DEFAULT_ROCKSDB_CREATE_MISSING_COLUMN_FAMILIES = true;

    /**
     * Default value for enableStatistics.
     */
    @ManagedContextDefault(name = ROCKSDB_ENABLE_STATISTICS)
    boolean DEFAULT_ROCKSDB_ENABLE_STATISTICS = false;

    /**
     * Default value for statsDumpPeriodSec.
     */
    @ManagedContextDefault(name = ROCKSDB_STATS_DUMP_PERIOD_SEC)
    int DEFAULT_ROCKSDB_STATS_DUMP_PERIOD_SEC = 0;

    /**
     * Default value for async committer notify threshold.
     */
    @ManagedContextDefault(name = ROCKSDB_COMMITTER_NOTIFY_THRESHOLD)
    int DEFAULT_ROCKSDB_COMMITTER_NOTIFY_THRESHOLD = 8;

    /**
     * Default value for async committer wait timeout in milliseconds.
     */
    @ManagedContextDefault(name = ROCKSDB_COMMITTER_WAIT_TIMEOUT_MS)
    long DEFAULT_ROCKSDB_COMMITTER_WAIT_TIMEOUT_MS = 500L;

    /**
     * Default value for write sync.
     */
    @ManagedContextDefault(name = ROCKSDB_WRITE_SYNC)
    boolean DEFAULT_ROCKSDB_WRITE_SYNC = false;

    /**
     * Default value for disable WAL.
     */
    @ManagedContextDefault(name = ROCKSDB_DISABLE_WAL)
    boolean DEFAULT_ROCKSDB_DISABLE_WAL = false;

    /**
     * Default value for messageChunkSize.
     */
    @ManagedContextDefault(name = ROCKSDB_MESSAGE_CHUNK_SIZE)
    int DEFAULT_ROCKSDB_MESSAGE_CHUNK_SIZE = 64 * 1024;

    /**
     * Default value for messageInlineThreshold.
     */
    @ManagedContextDefault(name = ROCKSDB_MESSAGE_INLINE_THRESHOLD)
    int DEFAULT_ROCKSDB_MESSAGE_INLINE_THRESHOLD = DEFAULT_ROCKSDB_MESSAGE_CHUNK_SIZE;

    /**
     * Default value for queueSegmentShift.
     */
    @ManagedContextDefault(name = ROCKSDB_QUEUE_SEGMENT_SHIFT)
    int DEFAULT_ROCKSDB_QUEUE_SEGMENT_SHIFT = 16;

    /**
     * Default value for TransactionDB default lock timeout.
     */
    @ManagedContextDefault(name = ROCKSDB_DEFAULT_LOCK_TIMEOUT)
    long DEFAULT_ROCKSDB_DEFAULT_LOCK_TIMEOUT = 0L;

    /**
     * Default value for TransactionDB lock timeout.
     */
    @ManagedContextDefault(name = ROCKSDB_TRANSACTION_LOCK_TIMEOUT)
    long DEFAULT_ROCKSDB_TRANSACTION_LOCK_TIMEOUT = 0L;

    /**
     * Default value for TransactionDB max number of locks.
     */
    @ManagedContextDefault(name = ROCKSDB_MAX_NUM_LOCKS)
    long DEFAULT_ROCKSDB_MAX_NUM_LOCKS = 0L;

    /**
     * Default value for TransactionDB number of lock stripes.
     */
    @ManagedContextDefault(name = ROCKSDB_NUM_STRIPES)
    long DEFAULT_ROCKSDB_NUM_STRIPES = 0L;

    /**
     * Default value for TransactionDB write policy.
     */
    @ManagedContextDefault(name = ROCKSDB_TXN_WRITE_POLICY)
    String DEFAULT_ROCKSDB_TXN_WRITE_POLICY = "";

    /**
     * Default value for TransactionDB retry attempts.
     */
    @ManagedContextDefault(name = ROCKSDB_TXN_RETRY_ATTEMPTS)
    int DEFAULT_ROCKSDB_TXN_RETRY_ATTEMPTS = 5;

    /**
     * Default value for TransactionDB retry base sleep in milliseconds.
     */
    @ManagedContextDefault(name = ROCKSDB_TXN_RETRY_BASE_SLEEP_MILLIS)
    long DEFAULT_ROCKSDB_TXN_RETRY_BASE_SLEEP_MILLIS = 25L;

    /**
     * Returns whether the store should be created when it is missing.
     *
     * @return true to create a new store when missing.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.createIfMissing}", immutable = true)
    Boolean getCreateIfMissing();

    /**
     * Returns whether missing column families should be created.
     *
     * @return true to create missing column families.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.createMissingColumnFamilies}",
                      immutable = true)
    Boolean getCreateMissingColumnFamilies();

    /**
     * Returns the maximum number of open files or 0 to use RocksDB defaults.
     *
     * @return maximum open files or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Integer getMaxOpenFiles();

    /**
     * Returns the maximum number of background jobs or 0 to use RocksDB defaults.
     *
     * @return maximum background jobs or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Integer getMaxBackgroundJobs();

    /**
     * Returns the maximum number of subcompactions or 0 to use RocksDB defaults.
     *
     * @return maximum subcompactions or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Integer getMaxSubcompactions();

    /**
     * Returns a custom WAL directory or empty to use the store path.
     *
     * @return WAL directory or empty to use the store path.
     */
    @Override
    @ManagedAttribute(defaultValue = "", immutable = true)
    String getWalDir();

    /**
     * Returns the bytes per sync or 0 to use RocksDB defaults.
     *
     * @return bytes per sync or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Long getBytesPerSync();

    /**
     * Returns the WAL bytes per sync or 0 to use RocksDB defaults.
     *
     * @return WAL bytes per sync or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Long getWalBytesPerSync();

    /**
     * Returns whether RocksDB statistics are enabled.
     *
     * @return true to enable RocksDB statistics.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.enableStatistics}", immutable = true)
    Boolean getEnableStatistics();

    /**
     * Returns the statistics dump period in seconds or 0 to disable periodic dumps.
     *
     * @return statistics dump period in seconds or 0 to disable.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.statsDumpPeriodSec}")
    Integer getStatsDumpPeriodSec();

    /**
     * Returns the notify threshold for async commits.
     *
     * @return async committer notify threshold.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.committerNotifyThreshold}", immutable = true)
    Integer getCommitterNotifyThreshold();

    /**
     * Returns the wait timeout for async commits in milliseconds.
     *
     * @return async committer wait timeout in milliseconds.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.committerWaitTimeoutMs}", immutable = true)
    Long getCommitterWaitTimeoutMs();

    /**
     * Returns whether writes should be synchronized on commit.
     *
     * @return true to enable sync on writes.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.writeSync}", immutable = true)
    Boolean getWriteSync();

    /**
     * Returns whether WAL should be disabled for writes.
     *
     * @return true to disable WAL.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.disableWAL}", immutable = true)
    Boolean getDisableWAL();

    /**
     * Returns the message chunk size in bytes.
     *
     * @return the message chunk size in bytes.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.messageChunkSize}", immutable = true)
    Integer getMessageChunkSize();

    /**
     * Returns the inline content threshold in bytes.
     *
     * @return the inline content threshold in bytes.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.messageInlineThreshold}", immutable = true)
    Integer getMessageInlineThreshold();

    /**
     * Returns the queue segment shift.
     *
     * @return queue segment shift.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.queueSegmentShift}", immutable = true)
    Integer getQueueSegmentShift();

    /**
     * Returns default lock timeout in milliseconds.
     *
     * @return default lock timeout.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.defaultLockTimeoutMs}", immutable = true)
    Long getDefaultLockTimeout();

    /**
     * Returns transaction lock timeout in milliseconds.
     *
     * @return transaction lock timeout.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.transactionLockTimeoutMs}", immutable = true)
    Long getTransactionLockTimeout();

    /**
     * Returns max number of locks.
     *
     * @return max number of locks.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.maxNumLocks}", immutable = true)
    Long getMaxNumLocks();

    /**
     * Returns number of lock stripes.
     *
     * @return number of lock stripes.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.numStripes}", immutable = true)
    Long getNumStripes();

    /**
     * Returns transaction write policy.
     *
     * @return transaction write policy.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.txnWritePolicy}", immutable = true)
    String getTxnWritePolicy();

    /**
     * Returns transaction retry attempts.
     *
     * @return retry attempts.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.txnRetryAttempts}", immutable = true)
    Integer getTxnRetryAttempts();

    /**
     * Returns transaction retry base sleep in milliseconds.
     *
     * @return retry base sleep.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.broker.rocksdb.txnRetryBaseSleepMs}", immutable = true)
    Long getTxnRetryBaseSleepMs();

    /**
     * Returns the max total WAL size or 0 to use RocksDB defaults.
     *
     * @return max total WAL size or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Long getMaxTotalWalSize();

    /**
     * Returns the WAL TTL in seconds or 0 to use RocksDB defaults.
     *
     * @return WAL TTL in seconds or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Integer getWalTtlSeconds();

    /**
     * Returns the WAL size limit in MB or 0 to use RocksDB defaults.
     *
     * @return WAL size limit in MB or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Integer getWalSizeLimitMb();

    /**
     * Returns the write buffer size or 0 to use RocksDB defaults.
     *
     * @return write buffer size or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Long getWriteBufferSize();

    /**
     * Returns the maximum number of write buffers or 0 to use RocksDB defaults.
     *
     * @return maximum number of write buffers or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Integer getMaxWriteBufferNumber();

    /**
     * Returns the minimum number of write buffers to merge or 0 to use RocksDB defaults.
     *
     * @return minimum number of write buffers to merge or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Integer getMinWriteBufferNumberToMerge();

    /**
     * Returns the target file size base or 0 to use RocksDB defaults.
     *
     * @return target file size base or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Long getTargetFileSizeBase();

    /**
     * Returns whether dynamic level bytes are enabled.
     *
     * @return true to enable dynamic level bytes.
     */
    @Override
    @ManagedAttribute(defaultValue = "false", immutable = true)
    Boolean getLevelCompactionDynamicLevelBytes();

    /**
     * Returns the compaction style or empty to use RocksDB defaults.
     *
     * @return compaction style or empty to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "", immutable = true)
    String getCompactionStyle();

    /**
     * Returns the compression type or empty to use RocksDB defaults.
     *
     * @return compression type or empty to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "", immutable = true)
    String getCompressionType();

    /**
     * Returns the block cache size or 0 to use RocksDB defaults.
     *
     * @return block cache size or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Long getBlockCacheSize();

    /**
     * Returns the block size or 0 to use RocksDB defaults.
     *
     * @return block size or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Long getBlockSize();

    /**
     * Returns the bloom filter bits per key or 0 to use RocksDB defaults.
     *
     * @return bloom filter bits per key or 0 to use RocksDB defaults.
     */
    @Override
    @ManagedAttribute(defaultValue = "0", immutable = true)
    Integer getBloomFilterBitsPerKey();

    /**
     * Returns whether to cache index and filter blocks.
     *
     * @return true to cache index and filter blocks.
     */
    @Override
    @ManagedAttribute(defaultValue = "false", immutable = true)
    Boolean getCacheIndexAndFilterBlocks();

    /**
     * Returns whether to pin level 0 filter and index blocks in cache.
     *
     * @return true to pin level 0 filter and index blocks in cache.
     */
    @Override
    @ManagedAttribute(defaultValue = "false", immutable = true)
    Boolean getPinL0FilterAndIndexBlocksInCache();
}
