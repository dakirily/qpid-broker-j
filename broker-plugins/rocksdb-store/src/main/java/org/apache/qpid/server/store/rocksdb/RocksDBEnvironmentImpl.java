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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;

import org.apache.qpid.server.store.StoreException;

/**
 * Provides a RocksDB environment implementation.
 *
 * Thread-safety: safe for concurrent access by the configuration model.
 */
public class RocksDBEnvironmentImpl implements RocksDBEnvironment
{
    private final RocksDB _database;
    private final Map<RocksDBColumnFamily, ColumnFamilyHandle> _handles;
    private final List<ColumnFamilyHandle> _handleList;
    private final DBOptions _dbOptions;
    private final ColumnFamilyOptions _columnFamilyOptions;
    private final String _storePath;
    private final Statistics _statistics;

    /**
     * Opens a RocksDB environment using the provided settings.
     *
     * @param storePath store path.
     * @param settings RocksDB settings.
     *
     * @return the RocksDB environment.
     *
     * @throws StoreException on open failures.
     */
    public static RocksDBEnvironmentImpl open(final String storePath, final RocksDBSettings settings)
            throws StoreException
    {
        try
        {
            RocksDB.loadLibrary();
            DBOptions dbOptions = RocksDBOptionsFactory.createDbOptions(settings);
            ColumnFamilyOptions columnFamilyOptions = RocksDBOptionsFactory.createColumnFamilyOptions(settings);
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (RocksDBColumnFamily family : RocksDBColumnFamily.values())
            {
                descriptors.add(new ColumnFamilyDescriptor(family.getNameBytes(), columnFamilyOptions));
            }

            List<ColumnFamilyHandle> handles = new ArrayList<>();
            RocksDB database = RocksDB.open(dbOptions, storePath, descriptors, handles);
            Map<RocksDBColumnFamily, ColumnFamilyHandle> handleMap = buildHandleMap(handles);
            return new RocksDBEnvironmentImpl(database, handleMap, handles, dbOptions, columnFamilyOptions, storePath);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to open RocksDB store", e);
        }
    }

    /**
     * Builds a handle map from the handle list.
     *
     * @param handles handle list.
     *
     * @return handle map.
     */
    private static Map<RocksDBColumnFamily, ColumnFamilyHandle> buildHandleMap(
            final List<ColumnFamilyHandle> handles)
    {
        Map<RocksDBColumnFamily, ColumnFamilyHandle> handleMap =
                new EnumMap<>(RocksDBColumnFamily.class);
        RocksDBColumnFamily[] families = RocksDBColumnFamily.values();
        for (int i = 0; i < families.length; i++)
        {
            handleMap.put(families[i], handles.get(i));
        }
        return handleMap;
    }

    /**
     * Creates a RocksDB environment instance.
     *
     * @param database RocksDB instance.
     * @param handles column family handles.
     * @param handleList handle list for closing.
     * @param dbOptions database options.
     * @param columnFamilyOptions column family options.
     * @param storePath store path.
     */
    private RocksDBEnvironmentImpl(final RocksDB database,
                                   final Map<RocksDBColumnFamily, ColumnFamilyHandle> handles,
                                   final List<ColumnFamilyHandle> handleList,
                                   final DBOptions dbOptions,
                                   final ColumnFamilyOptions columnFamilyOptions,
                                   final String storePath)
    {
        _database = database;
        _handles = handles;
        _handleList = handleList;
        _dbOptions = dbOptions;
        _columnFamilyOptions = columnFamilyOptions;
        _storePath = storePath;
        _statistics = dbOptions.statistics();
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
     * Returns the RocksDB instance.
     *
     * @return the RocksDB instance.
     */
    @Override
    public RocksDB getDatabase()
    {
        return _database;
    }

    /**
     * Returns the column family handle.
     *
     * @param columnFamily column family identifier.
     *
     * @return the column family handle.
     */
    @Override
    public ColumnFamilyHandle getColumnFamilyHandle(final RocksDBColumnFamily columnFamily)
    {
        return _handles.get(columnFamily);
    }

    /**
     * Returns RocksDB statistics or null when disabled.
     *
     * @return the statistics instance or null.
     */
    @Override
    public Statistics getStatistics()
    {
        return _statistics;
    }

    /**
     * Closes the RocksDB environment.
     */
    @Override
    public void close()
    {
        for (ColumnFamilyHandle handle : _handleList)
        {
            handle.close();
        }
        _database.close();
        _columnFamilyOptions.close();
        _dbOptions.close();
    }
}
