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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.AbstractDurableConfigurationStoreTestCase;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.virtualhostnode.rocksdb.RocksDBVirtualHostNode;

/**
 * Tests RocksDB configuration store persistence.
 *
 * Thread-safety: runs in a single-threaded test context.
 */
public class RocksDBConfigurationStoreTest extends AbstractDurableConfigurationStoreTestCase
{
    /**
     * Creates a RocksDB virtual host node stub.
     *
     * @param storeLocation store path.
     * @param factory configured object factory.
     *
     * @return the virtual host node.
     */
    @Override
    protected VirtualHostNode<?> createVirtualHostNode(final String storeLocation,
                                                       final ConfiguredObjectFactory factory)
    {
        final RocksDBVirtualHostNode<?> parent = mock(RocksDBVirtualHostNode.class);
        when(parent.getStorePath()).thenReturn(storeLocation);
        when(parent.getCreateIfMissing()).thenReturn(true);
        when(parent.getCreateMissingColumnFamilies()).thenReturn(true);
        when(parent.getMaxOpenFiles()).thenReturn(0);
        when(parent.getMaxBackgroundJobs()).thenReturn(0);
        when(parent.getMaxSubcompactions()).thenReturn(0);
        when(parent.getWalDir()).thenReturn("");
        when(parent.getBytesPerSync()).thenReturn(0L);
        when(parent.getWalBytesPerSync()).thenReturn(0L);
        when(parent.getEnableStatistics()).thenReturn(false);
        when(parent.getStatsDumpPeriodSec()).thenReturn(0);
        when(parent.getMaxTotalWalSize()).thenReturn(0L);
        when(parent.getWalTtlSeconds()).thenReturn(0);
        when(parent.getWalSizeLimitMb()).thenReturn(0);
        when(parent.getWriteBufferSize()).thenReturn(0L);
        when(parent.getMaxWriteBufferNumber()).thenReturn(0);
        when(parent.getMinWriteBufferNumberToMerge()).thenReturn(0);
        when(parent.getTargetFileSizeBase()).thenReturn(0L);
        when(parent.getLevelCompactionDynamicLevelBytes()).thenReturn(false);
        when(parent.getCompactionStyle()).thenReturn("");
        when(parent.getCompressionType()).thenReturn("");
        when(parent.getBlockCacheSize()).thenReturn(0L);
        when(parent.getBlockSize()).thenReturn(0L);
        when(parent.getBloomFilterBitsPerKey()).thenReturn(0);
        when(parent.getCacheIndexAndFilterBlocks()).thenReturn(false);
        when(parent.getPinL0FilterAndIndexBlocksInCache()).thenReturn(false);
        return parent;
    }

    /**
     * Creates a RocksDB configuration store instance.
     *
     * @return the configuration store.
     *
     * @throws Exception on creation errors.
     */
    @Override
    protected DurableConfigurationStore createConfigStore() throws Exception
    {
        return new RocksDBConfigurationStore(VirtualHost.class);
    }

    /**
     * Verifies reload returns persisted records.
     *
     * @throws Exception on test errors.
     */
    @Test
    public void testReload() throws Exception
    {
        DurableConfigurationStore store = getConfigurationStore();
        UUID recordId = UUID.randomUUID();
        Map<String, Object> attributes = Map.of("name", "test");
        ConfiguredObjectRecord record =
                new ConfiguredObjectRecordImpl(recordId, "TestType", attributes, Map.of());
        store.create(record);

        ConfiguredObjectRecordHandler handler = mock(ConfiguredObjectRecordHandler.class);
        store.reload(handler);

        verify(handler).handle(matchesRecord(recordId, "TestType", attributes, Map.of()));
    }
}
