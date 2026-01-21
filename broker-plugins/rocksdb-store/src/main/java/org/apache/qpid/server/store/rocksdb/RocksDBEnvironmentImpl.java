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
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;

import org.apache.qpid.server.store.StoreException;

/**
 * Provides a RocksDB environment implementation.
 * <br>
 * Thread-safety: safe for concurrent access by the configuration model.
 */
public class RocksDBEnvironmentImpl implements RocksDBEnvironment
{
    private final TransactionDB _database;
    private final Map<RocksDBColumnFamily, ColumnFamilyHandle> _handles;
    private final List<ColumnFamilyHandle> _handleList;
    private final DBOptions _dbOptions;
    private final ColumnFamilyOptions _columnFamilyOptions;
    private final TransactionDBOptions _transactionDbOptions;
    private final RocksDBOptionsFactory.OptionsResources _optionsResources;
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
        DBOptions dbOptions = null;
        ColumnFamilyOptions columnFamilyOptions = null;
        TransactionDBOptions transactionOptions = null;
        RocksDBOptionsFactory.OptionsResources optionsResources = null;
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try
        {
            if (!RocksDBUtils.isAvailable())
            {
                throw new StoreException("RocksDB native library is not available");
            }
            RocksDBOptionsFactory.OptionsBundle bundle = RocksDBOptionsFactory.createOptionsBundle(settings);
            dbOptions = bundle.getDbOptions();
            columnFamilyOptions = bundle.getColumnFamilyOptions();
            transactionOptions = bundle.getTransactionDbOptions();
            optionsResources = bundle.getResources();
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            for (RocksDBColumnFamily family : RocksDBColumnFamily.values())
            {
                descriptors.add(new ColumnFamilyDescriptor(family.getNameBytes(), columnFamilyOptions));
            }

            TransactionDB database = TransactionDB.open(dbOptions, transactionOptions, storePath, descriptors, handles);
            Map<RocksDBColumnFamily, ColumnFamilyHandle> handleMap = buildHandleMap(handles);
            return new RocksDBEnvironmentImpl(database, handleMap, handles, dbOptions, columnFamilyOptions,
                    transactionOptions, optionsResources, storePath);
        }
        catch (RocksDBException | RuntimeException e)
        {
            for (ColumnFamilyHandle handle : handles)
            {
                handle.close();
            }
            if (transactionOptions != null)
            {
                transactionOptions.close();
            }
            if (columnFamilyOptions != null)
            {
                columnFamilyOptions.close();
            }
            if (dbOptions != null)
            {
                dbOptions.close();
            }
            closeOptionsResources(optionsResources);
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
    private static Map<RocksDBColumnFamily, ColumnFamilyHandle> buildHandleMap(final List<ColumnFamilyHandle> handles)
    {
        Map<RocksDBColumnFamily, ColumnFamilyHandle> handleMap = new EnumMap<>(RocksDBColumnFamily.class);
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
    private RocksDBEnvironmentImpl(final TransactionDB database,
                                   final Map<RocksDBColumnFamily, ColumnFamilyHandle> handles,
                                   final List<ColumnFamilyHandle> handleList,
                                   final DBOptions dbOptions,
                                   final ColumnFamilyOptions columnFamilyOptions,
                                   final TransactionDBOptions transactionDbOptions,
                                   final RocksDBOptionsFactory.OptionsResources optionsResources,
                                   final String storePath)
    {
        _database = database;
        _handles = handles;
        _handleList = handleList;
        _dbOptions = dbOptions;
        _columnFamilyOptions = columnFamilyOptions;
        _transactionDbOptions = transactionDbOptions;
        _optionsResources = optionsResources;
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
        _transactionDbOptions.close();
        closeOptionsResources(_optionsResources);
    }

    private static void closeOptionsResources(final RocksDBOptionsFactory.OptionsResources resources)
    {
        if (resources == null)
        {
            return;
        }
        closeQuietly(resources.getBloomFilter());
        closeQuietly(resources.getBlockCache());
        closeQuietly(resources.getStatistics());
        closeQuietly(resources.getTableConfig());
    }

    private static void closeQuietly(final Object resource)
    {
        if (resource == null)
        {
            return;
        }
        try
        {
            if (resource instanceof AutoCloseable)
            {
                ((AutoCloseable) resource).close();
                return;
            }
            try
            {
                resource.getClass().getMethod("close").invoke(resource);
            }
            catch (NoSuchMethodException ignored)
            {
            }
        }
        catch (Exception ignored)
        {
        }
    }
}
