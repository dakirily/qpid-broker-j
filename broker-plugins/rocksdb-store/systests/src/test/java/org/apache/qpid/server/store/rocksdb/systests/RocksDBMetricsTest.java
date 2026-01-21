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

package org.apache.qpid.server.store.rocksdb.systests;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.tests.utils.ConfigItem;

@ConfigItem(name = "qpid.broker.rocksdb.enableStatistics", value = "true")
public class RocksDBMetricsTest extends JmsTestBase
{
    @BeforeAll
    public static void verifyStoreType()
    {
        String type = System.getProperty("virtualhostnode.type", "");
        assumeTrue("ROCKSDB".equalsIgnoreCase(type), "Tests require virtualhostnode.type=ROCKSDB");
    }

    @BeforeEach
    public void ensurePersistentStore()
    {
        assumeTrue(getBrokerAdmin().supportsRestart(), "Tests require persistent store");
        assumeTrue(getBrokerAdmin().isManagementSupported(), "Management support required");
    }

    @Test
    public void rocksdbStatisticsReportedViaManagement() throws Exception
    {
        Map<String, Object> arguments = Collections.singletonMap("reset", Boolean.TRUE);
        Object response = performOperationUsingAmqpManagement(getVirtualHostName(),
                                                              "getRocksDBStatistics",
                                                              "org.apache.qpid.VirtualHost",
                                                              arguments);

        assertNotNull(response, "Statistics response is null");
        assertTrue(response instanceof Map, "Statistics response is not a map");

        @SuppressWarnings("unchecked")
        Map<String, Object> stats = (Map<String, Object>) response;
        assertTrue(stats.containsKey("rocksdb.stats"), "Expected rocksdb.stats entry");
        assertTrue(stats.get("rocksdb.stats") instanceof String, "rocksdb.stats should be a string");
    }

}
