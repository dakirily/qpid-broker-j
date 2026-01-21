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

package org.apache.qpid.server.virtualhostnode.rocksdb;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.rocksdb.RocksDBColumnFamily;
import org.apache.qpid.server.store.rocksdb.RocksDBUtils;
import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Tests management operations for the RocksDB virtual host node.
 *
 * Thread-safety: runs in a single-threaded test context.
 */
public class RocksDBVirtualHostNodeManagementTest extends UnitTestBase
{
    @TempDir
    private File _storeFolder;

    private VirtualHostNode<?> _virtualHostNode;

    /**
     * Deletes the virtual host node after each test.
     */
    @AfterEach
    public void tearDown()
    {
        if (_virtualHostNode != null)
        {
            _virtualHostNode.close();
            _virtualHostNode = null;
        }
    }

    /**
     * Verifies management operations on the virtual host node.
     */
    @Test
    public void testManagementOperations()
    {
        assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");

        Broker<?> broker = BrokerTestHelper.createBrokerMock();
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, getTestName());
        attributes.put(ConfiguredObject.TYPE, RocksDBVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);
        attributes.put(RocksDBVirtualHostNode.STORE_PATH, _storeFolder.getAbsolutePath());
        attributes.put(VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION, "{}");
        attributes.put("createIfMissing", true);
        attributes.put("createMissingColumnFamilies", true);
        attributes.put("enableStatistics", true);
        attributes.put("statsDumpPeriodSec", 0);

        _virtualHostNode = broker.getObjectFactory().create(VirtualHostNode.class, attributes, broker);
        _virtualHostNode.start();

        RocksDBVirtualHostNode<?> node = (RocksDBVirtualHostNode<?>) _virtualHostNode;
        node.updateMutableConfig();
        node.flush(RocksDBColumnFamily.MESSAGE_METADATA.getName(), true);
        node.compactRange(RocksDBColumnFamily.MESSAGE_METADATA.getName());

        Map<String, String> properties = node.getDbProperties("rocksdb.");
        assertNotNull(properties, "Properties should not be null");
        for (String key : properties.keySet())
        {
            assertTrue(key.startsWith("rocksdb."), "Unexpected property key " + key);
        }
        if (!properties.isEmpty())
        {
            String propertyKey = properties.keySet().iterator().next();
            assertNotNull(node.getDbProperty(propertyKey), "Property value is missing for " + propertyKey);
        }

        Map<String, Object> stats = node.getRocksDBStatistics(false);
        assertTrue(stats.containsKey("rocksdb.stats"), "Expected rocksdb.stats entry");
    }
}
