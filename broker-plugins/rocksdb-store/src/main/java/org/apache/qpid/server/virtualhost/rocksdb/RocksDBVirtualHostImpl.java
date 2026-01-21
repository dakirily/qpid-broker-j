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

package org.apache.qpid.server.virtualhost.rocksdb;

import java.util.Map;

import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.rocksdb.RocksDBMessageStore;
import org.apache.qpid.server.store.rocksdb.RocksDBEnvironment;
import org.apache.qpid.server.store.rocksdb.RocksDBManagementSupport;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;

/**
 * Provides the RocksDB virtual host implementation.
 *
 * Thread-safety: safe for concurrent access by the configuration model.
 */
@ManagedObject(category = false, type = RocksDBVirtualHostImpl.VIRTUAL_HOST_TYPE)
public class RocksDBVirtualHostImpl extends AbstractVirtualHost<RocksDBVirtualHostImpl>
        implements RocksDBVirtualHost<RocksDBVirtualHostImpl>
{
    /**
     * The managed object type name.
     */
    public static final String VIRTUAL_HOST_TYPE = "ROCKSDB";

    @ManagedAttributeField
    private String _storePath;
    @ManagedAttributeField
    private Long _storeUnderfullSize;
    @ManagedAttributeField
    private Long _storeOverfullSize;

    @ManagedAttributeField
    private Boolean _createIfMissing;
    @ManagedAttributeField
    private Boolean _createMissingColumnFamilies;
    @ManagedAttributeField
    private Integer _maxOpenFiles;
    @ManagedAttributeField
    private Integer _maxBackgroundJobs;
    @ManagedAttributeField
    private Integer _maxSubcompactions;
    @ManagedAttributeField
    private String _walDir;
    @ManagedAttributeField
    private Long _bytesPerSync;
    @ManagedAttributeField
    private Long _walBytesPerSync;
    @ManagedAttributeField
    private Boolean _enableStatistics;
    @ManagedAttributeField
    private Integer _statsDumpPeriodSec;
    @ManagedAttributeField
    private Long _maxTotalWalSize;
    @ManagedAttributeField
    private Integer _walTtlSeconds;
    @ManagedAttributeField
    private Integer _walSizeLimitMb;
    @ManagedAttributeField
    private Long _writeBufferSize;
    @ManagedAttributeField
    private Integer _maxWriteBufferNumber;
    @ManagedAttributeField
    private Integer _minWriteBufferNumberToMerge;
    @ManagedAttributeField
    private Long _targetFileSizeBase;
    @ManagedAttributeField
    private Boolean _levelCompactionDynamicLevelBytes;
    @ManagedAttributeField
    private String _compactionStyle;
    @ManagedAttributeField
    private String _compressionType;
    @ManagedAttributeField
    private Long _blockCacheSize;
    @ManagedAttributeField
    private Long _blockSize;
    @ManagedAttributeField
    private Integer _bloomFilterBitsPerKey;
    @ManagedAttributeField
    private Boolean _cacheIndexAndFilterBlocks;
    @ManagedAttributeField
    private Boolean _pinL0FilterAndIndexBlocksInCache;

    /**
     * Creates a RocksDB virtual host instance.
     *
     * @param attributes configuration attributes.
     * @param virtualHostNode parent virtual host node.
     */
    @ManagedObjectFactoryConstructor(conditionallyAvailable = true,
                                     condition = "org.apache.qpid.server.store.rocksdb.RocksDBUtils#isAvailable()")
    public RocksDBVirtualHostImpl(final Map<String, Object> attributes, final VirtualHostNode<?> virtualHostNode)
    {
        super(attributes, virtualHostNode);
    }

    /**
     * Creates the message store instance.
     *
     * @return the message store.
     */
    @Override
    protected MessageStore createMessageStore()
    {
        return new RocksDBMessageStore();
    }

    /**
     * Returns the store path.
     *
     * @return the store path.
     */
    @Override
    public String getStorePath()
    {
        return _storePath;
    }

    /**
     * Returns the store underfull size threshold.
     *
     * @return the underfull size threshold.
     */
    @Override
    public Long getStoreUnderfullSize()
    {
        return _storeUnderfullSize;
    }

    /**
     * Returns the store overfull size threshold.
     *
     * @return the overfull size threshold.
     */
    @Override
    public Long getStoreOverfullSize()
    {
        return _storeOverfullSize;
    }

    /**
     * Returns the RocksDB environment.
     *
     * @return the RocksDB environment.
     */
    @Override
    public RocksDBEnvironment getRocksDBEnvironment()
    {
        if (getMessageStore() instanceof RocksDBMessageStore)
        {
            return ((RocksDBMessageStore) getMessageStore()).getEnvironment();
        }
        return null;
    }

    /**
     * Applies mutable RocksDB settings.
     */
    @Override
    public void updateMutableConfig()
    {
        RocksDBManagementSupport.updateMutableConfig(getRocksDBEnvironment(), this);
    }

    /**
     * Flushes memtables to disk.
     *
     * @param columnFamily column family name or empty for all.
     * @param wait true to wait for completion.
     */
    @Override
    public void flush(final String columnFamily, final boolean wait)
    {
        RocksDBManagementSupport.flush(getRocksDBEnvironment(), columnFamily, wait);
    }

    /**
     * Compacts the store data.
     *
     * @param columnFamily column family name or empty for all.
     */
    @Override
    public void compactRange(final String columnFamily)
    {
        RocksDBManagementSupport.compactRange(getRocksDBEnvironment(), columnFamily);
    }

    /**
     * Returns properties by prefix.
     *
     * @param propertyPrefix property prefix.
     *
     * @return the properties.
     */
    @Override
    public Map<String, String> getDbProperties(final String propertyPrefix)
    {
        return RocksDBManagementSupport.getDbProperties(getRocksDBEnvironment(), propertyPrefix);
    }

    /**
     * Returns a single property value.
     *
     * @param property property name.
     *
     * @return the property value.
     */
    @Override
    public String getDbProperty(final String property)
    {
        return RocksDBManagementSupport.getDbProperty(getRocksDBEnvironment(), property);
    }

    /**
     * Returns RocksDB statistics.
     *
     * @param reset true to reset statistics.
     *
     * @return the statistics.
     */
    @Override
    public Map<String, Object> getRocksDBStatistics(final boolean reset)
    {
        return RocksDBManagementSupport.getStatistics(getRocksDBEnvironment(), reset);
    }

    /**
     * Returns whether to create a missing store.
     *
     * @return true to create missing store.
     */
    @Override
    public Boolean getCreateIfMissing()
    {
        return _createIfMissing;
    }

    /**
     * Returns whether to create missing column families.
     *
     * @return true to create missing column families.
     */
    @Override
    public Boolean getCreateMissingColumnFamilies()
    {
        return _createMissingColumnFamilies;
    }

    /**
     * Returns the maximum open files setting.
     *
     * @return the maximum open files setting.
     */
    @Override
    public Integer getMaxOpenFiles()
    {
        return _maxOpenFiles;
    }

    /**
     * Returns the maximum background jobs setting.
     *
     * @return the maximum background jobs setting.
     */
    @Override
    public Integer getMaxBackgroundJobs()
    {
        return _maxBackgroundJobs;
    }

    /**
     * Returns the maximum subcompactions setting.
     *
     * @return the maximum subcompactions setting.
     */
    @Override
    public Integer getMaxSubcompactions()
    {
        return _maxSubcompactions;
    }

    /**
     * Returns the WAL directory.
     *
     * @return the WAL directory.
     */
    @Override
    public String getWalDir()
    {
        return _walDir;
    }

    /**
     * Returns the bytes per sync setting.
     *
     * @return the bytes per sync setting.
     */
    @Override
    public Long getBytesPerSync()
    {
        return _bytesPerSync;
    }

    /**
     * Returns the WAL bytes per sync setting.
     *
     * @return the WAL bytes per sync setting.
     */
    @Override
    public Long getWalBytesPerSync()
    {
        return _walBytesPerSync;
    }

    /**
     * Returns whether statistics are enabled.
     *
     * @return true when statistics are enabled.
     */
    @Override
    public Boolean getEnableStatistics()
    {
        return _enableStatistics;
    }

    /**
     * Returns the statistics dump period.
     *
     * @return the statistics dump period in seconds.
     */
    @Override
    public Integer getStatsDumpPeriodSec()
    {
        return _statsDumpPeriodSec;
    }

    /**
     * Returns the maximum total WAL size.
     *
     * @return the maximum total WAL size.
     */
    @Override
    public Long getMaxTotalWalSize()
    {
        return _maxTotalWalSize;
    }

    /**
     * Returns the WAL TTL setting.
     *
     * @return WAL TTL in seconds.
     */
    @Override
    public Integer getWalTtlSeconds()
    {
        return _walTtlSeconds;
    }

    /**
     * Returns the WAL size limit setting.
     *
     * @return WAL size limit in MB.
     */
    @Override
    public Integer getWalSizeLimitMb()
    {
        return _walSizeLimitMb;
    }

    /**
     * Returns the write buffer size.
     *
     * @return the write buffer size.
     */
    @Override
    public Long getWriteBufferSize()
    {
        return _writeBufferSize;
    }

    /**
     * Returns the maximum number of write buffers.
     *
     * @return the maximum number of write buffers.
     */
    @Override
    public Integer getMaxWriteBufferNumber()
    {
        return _maxWriteBufferNumber;
    }

    /**
     * Returns the minimum number of write buffers to merge.
     *
     * @return the minimum number of write buffers to merge.
     */
    @Override
    public Integer getMinWriteBufferNumberToMerge()
    {
        return _minWriteBufferNumberToMerge;
    }

    /**
     * Returns the target file size base.
     *
     * @return the target file size base.
     */
    @Override
    public Long getTargetFileSizeBase()
    {
        return _targetFileSizeBase;
    }

    /**
     * Returns whether dynamic level bytes are enabled.
     *
     * @return true when dynamic level bytes are enabled.
     */
    @Override
    public Boolean getLevelCompactionDynamicLevelBytes()
    {
        return _levelCompactionDynamicLevelBytes;
    }

    /**
     * Returns the compaction style.
     *
     * @return the compaction style.
     */
    @Override
    public String getCompactionStyle()
    {
        return _compactionStyle;
    }

    /**
     * Returns the compression type.
     *
     * @return the compression type.
     */
    @Override
    public String getCompressionType()
    {
        return _compressionType;
    }

    /**
     * Returns the block cache size.
     *
     * @return the block cache size.
     */
    @Override
    public Long getBlockCacheSize()
    {
        return _blockCacheSize;
    }

    /**
     * Returns the block size.
     *
     * @return the block size.
     */
    @Override
    public Long getBlockSize()
    {
        return _blockSize;
    }

    /**
     * Returns the bloom filter bits per key.
     *
     * @return the bloom filter bits per key.
     */
    @Override
    public Integer getBloomFilterBitsPerKey()
    {
        return _bloomFilterBitsPerKey;
    }

    /**
     * Returns whether index and filter blocks are cached.
     *
     * @return true when index and filter blocks are cached.
     */
    @Override
    public Boolean getCacheIndexAndFilterBlocks()
    {
        return _cacheIndexAndFilterBlocks;
    }

    /**
     * Returns whether level 0 filter and index blocks are pinned in cache.
     *
     * @return true when level 0 filter and index blocks are pinned.
     */
    @Override
    public Boolean getPinL0FilterAndIndexBlocksInCache()
    {
        return _pinL0FilterAndIndexBlocksInCache;
    }
}
