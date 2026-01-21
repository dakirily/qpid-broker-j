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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;

/**
 * Provides access to the shared RocksDB environment.
 * <br>
 * Thread-safety: thread-safe implementations are required.
 */
public interface RocksDBEnvironment
{
    /**
     * Returns the store path.
     *
     * @return the store path.
     */
    String getStorePath();

    /**
     * Returns the RocksDB instance.
     *
     * @return the RocksDB instance.
     */
    RocksDB getDatabase();

    /**
     * Returns a column family handle.
     *
     * @param columnFamily column family identifier.
     *
     * @return the column family handle.
     */
    ColumnFamilyHandle getColumnFamilyHandle(RocksDBColumnFamily columnFamily);

    /**
     * Returns RocksDB statistics or null if disabled.
     *
     * @return the statistics instance or null.
     */
    Statistics getStatistics();

    /**
     * Closes the environment.
     */
    void close();
}
