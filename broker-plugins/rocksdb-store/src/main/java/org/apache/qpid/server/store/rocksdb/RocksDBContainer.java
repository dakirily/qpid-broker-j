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

import java.util.Map;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.Param;

/**
 * Declares operational access to the RocksDB environment.
 *
 * Thread-safety: thread-safe implementations are required.
 *
 * @param <X> the configured object type.
 */
public interface RocksDBContainer<X extends ConfiguredObject<X>> extends ConfiguredObject<X>
{
    /**
     * Returns the RocksDB environment.
     *
     * @return the RocksDB environment.
     */
    RocksDBEnvironment getRocksDBEnvironment();

    /**
     * Applies mutable configuration changes to the live RocksDB instance.
     */
    @ManagedOperation(description = "Apply mutable RocksDB configuration from settings",
                      changesConfiguredObjectState = false)
    void updateMutableConfig();

    /**
     * Flushes the memtables to disk.
     *
     * @param columnFamily the column family name or empty for all column families.
     * @param wait true to wait for the flush to finish.
     */
    @ManagedOperation(description = "Flush memtables to disk",
                      changesConfiguredObjectState = false)
    void flush(@Param(name = "columnFamily", defaultValue = "") String columnFamily,
               @Param(name = "wait", defaultValue = "true") boolean wait);

    /**
     * Compacts the store data.
     *
     * @param columnFamily the column family name or empty for all column families.
     */
    @ManagedOperation(description = "Compact the data files",
                      changesConfiguredObjectState = false)
    void compactRange(@Param(name = "columnFamily", defaultValue = "") String columnFamily);

    /**
     * Returns a set of properties by prefix.
     *
     * @param propertyPrefix the property prefix, for example "rocksdb.".
     *
     * @return the properties matching the prefix.
     */
    @ManagedOperation(description = "Get RocksDB properties by prefix",
                      nonModifying = true,
                      changesConfiguredObjectState = false)
    Map<String, String> getDbProperties(
            @Param(name = "propertyPrefix", defaultValue = "rocksdb.") String propertyPrefix);

    /**
     * Returns a single property value.
     *
     * @param property the property name.
     *
     * @return the property value or null if not available.
     */
    @ManagedOperation(description = "Get a single RocksDB property",
                      nonModifying = true,
                      changesConfiguredObjectState = false)
    String getDbProperty(@Param(name = "property") String property);

    /**
     * Returns RocksDB statistics.
     *
     * @param reset true to reset statistics after sampling.
     *
     * @return the statistics data.
     */
    @ManagedOperation(description = "Get RocksDB statistics",
                      nonModifying = true,
                      changesConfiguredObjectState = false)
    Map<String, Object> getRocksDBStatistics(@Param(name = "reset", defaultValue = "false") boolean reset);
}
