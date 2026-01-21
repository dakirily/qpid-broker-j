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
 * <br>
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
     * Context key for async committer notify threshold.
     */
    String ROCKSDB_COMMITTER_NOTIFY_THRESHOLD = "qpid.broker.rocksdb.committerNotifyThreshold";

    /**
     * Context key for async committer wait timeout in milliseconds.
     */
    String ROCKSDB_COMMITTER_WAIT_TIMEOUT_MS = "qpid.broker.rocksdb.committerWaitTimeoutMs";

    /**
     * Context key for write sync.
     */
    String ROCKSDB_WRITE_SYNC = "qpid.broker.rocksdb.writeSync";

    /**
     * Context key for write disable WAL.
     */
    String ROCKSDB_DISABLE_WAL = "qpid.broker.rocksdb.disableWAL";

    /**
     * Context key for message chunk size.
     */
    String ROCKSDB_MESSAGE_CHUNK_SIZE = "qpid.broker.rocksdb.messageChunkSize";

    /**
     * Context key for message inline threshold.
     */
    String ROCKSDB_MESSAGE_INLINE_THRESHOLD = "qpid.broker.rocksdb.messageInlineThreshold";

    /**
     * Context key for queue segment shift.
     */
    String ROCKSDB_QUEUE_SEGMENT_SHIFT = "qpid.broker.rocksdb.queueSegmentShift";
    /**
     * Context key for TransactionDB default lock timeout.
     */
    String ROCKSDB_DEFAULT_LOCK_TIMEOUT = "qpid.broker.rocksdb.defaultLockTimeoutMs";

    /**
     * Context key for TransactionDB transaction lock timeout.
     */
    String ROCKSDB_TRANSACTION_LOCK_TIMEOUT = "qpid.broker.rocksdb.transactionLockTimeoutMs";

    /**
     * Context key for TransactionDB max number of locks.
     */
    String ROCKSDB_MAX_NUM_LOCKS = "qpid.broker.rocksdb.maxNumLocks";

    /**
     * Context key for TransactionDB number of lock stripes.
     */
    String ROCKSDB_NUM_STRIPES = "qpid.broker.rocksdb.numStripes";

    /**
     * Context key for TransactionDB write policy.
     */
    String ROCKSDB_TXN_WRITE_POLICY = "qpid.broker.rocksdb.txnWritePolicy";

    /**
     * Context key for TransactionDB retry attempts.
     */
    String ROCKSDB_TXN_RETRY_ATTEMPTS = "qpid.broker.rocksdb.txnRetryAttempts";

    /**
     * Context key for TransactionDB retry base sleep in milliseconds.
     */
    String ROCKSDB_TXN_RETRY_BASE_SLEEP_MILLIS = "qpid.broker.rocksdb.txnRetryBaseSleepMs";

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
     * Returns the notify threshold for async commits.
     *
     * @return async committer notify threshold.
     */
    Integer getCommitterNotifyThreshold();

    /**
     * Returns the wait timeout for async commits in milliseconds.
     *
     * @return async committer wait timeout in milliseconds.
     */
    Long getCommitterWaitTimeoutMs();

    /**
     * Returns whether writes should be synchronized on commit.
     *
     * @return true to enable sync on writes.
     */
    Boolean getWriteSync();

    /**
     * Returns whether WAL should be disabled for writes.
     *
     * @return true to disable WAL.
     */
    Boolean getDisableWAL();

    /**
     * Returns the message chunk size in bytes or 0 to use the default.
     *
     * @return the message chunk size in bytes.
     */
    Integer getMessageChunkSize();

    /**
     * Returns the inline content threshold in bytes or 0 to use the chunk size.
     *
     * @return the inline content threshold in bytes.
     */
    Integer getMessageInlineThreshold();

    /**
     * Returns the queue segment shift or 0 to use the default.
     *
     * @return queue segment shift.
     */
    Integer getQueueSegmentShift();

    /**
     * Returns default lock timeout in milliseconds or 0 to use RocksDB defaults.
     *
     * @return default lock timeout.
     */
    Long getDefaultLockTimeout();

    /**
     * Returns transaction lock timeout in milliseconds or 0 to use RocksDB defaults.
     *
     * @return transaction lock timeout.
     */
    Long getTransactionLockTimeout();

    /**
     * Returns max number of locks or 0 to use RocksDB defaults.
     *
     * @return max number of locks.
     */
    Long getMaxNumLocks();

    /**
     * Returns number of lock stripes or 0 to use RocksDB defaults.
     *
     * @return number of lock stripes.
     */
    Long getNumStripes();

    /**
     * Returns transaction write policy or empty to use RocksDB defaults.
     *
     * @return transaction write policy.
     */
    String getTxnWritePolicy();

    /**
     * Returns the number of retry attempts for transactional operations or 0 to use defaults.
     *
     * @return retry attempts.
     */
    Integer getTxnRetryAttempts();

    /**
     * Returns the retry base sleep in milliseconds or 0 to use defaults.
     *
     * @return retry base sleep.
     */
    Long getTxnRetryBaseSleepMs();

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
