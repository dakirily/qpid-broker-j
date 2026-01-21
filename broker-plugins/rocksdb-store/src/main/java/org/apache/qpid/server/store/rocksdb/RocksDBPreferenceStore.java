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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.model.ModelVersion;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;

/**
 * Provides a RocksDB-backed preference store.
 *
 * Thread-safety: synchronized for configuration model usage.
 */
public class RocksDBPreferenceStore implements PreferenceStore
{
    private static final byte[] VERSION_KEY =
            "preferences_version".getBytes(StandardCharsets.US_ASCII);
    private static final int UUID_BYTES = 16;

    private enum StoreState { CLOSED, OPENED, ERRORED }

    private final RocksDBEnvironment _environment;
    private final RocksDBSettings _settings;
    private final ObjectMapper _objectMapper;
    private StoreState _state = StoreState.CLOSED;

    public RocksDBPreferenceStore(final RocksDBEnvironment environment, final RocksDBSettings settings)
    {
        _environment = environment;
        _settings = settings;
        _objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(true);
    }

    @Override
    public synchronized Collection<PreferenceRecord> openAndLoad(final PreferenceStoreUpdater updater)
            throws StoreException
    {
        if (_state != StoreState.CLOSED)
        {
            throw new IllegalStateException(String.format("PreferenceStore cannot be opened when in state '%s'",
                                                          _state));
        }
        if (_environment == null)
        {
            throw new StoreException("Preference store is not initialized");
        }
        try
        {
            RocksDB database = _environment.getDatabase();
            ColumnFamilyHandle handle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.PREFERENCES);

            String currentVersion = updater.getLatestVersion();
            String storedVersion = readVersion(database, handle);
            if (storedVersion == null)
            {
                storedVersion = currentVersion;
                writeVersion(database, handle, currentVersion);
            }

            ModelVersion stored = ModelVersion.fromString(storedVersion);
            ModelVersion current = ModelVersion.fromString(currentVersion);
            if (current.lessThan(stored))
            {
                throw new IllegalStateException(String.format(
                        "Cannot downgrade preference store storedVersion from '%s' to '%s'",
                        storedVersion,
                        currentVersion));
            }

            Collection<PreferenceRecord> records = loadRecords(database, handle);
            if (stored.lessThan(current))
            {
                records = updater.updatePreferences(storedVersion, records);
                replaceAll(database, handle, records, currentVersion);
            }

            _state = StoreState.OPENED;
            return records;
        }
        catch (RuntimeException e)
        {
            _state = StoreState.ERRORED;
            close();
            throw e;
        }
    }

    @Override
    public synchronized void close()
    {
        if (_state != StoreState.CLOSED)
        {
            _state = StoreState.CLOSED;
        }
    }

    @Override
    public synchronized void updateOrCreate(final Collection<PreferenceRecord> preferenceRecords)
    {
        if (_state != StoreState.OPENED)
        {
            throw new IllegalStateException("PreferenceStore is not opened");
        }
        if (preferenceRecords.isEmpty())
        {
            return;
        }
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle handle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.PREFERENCES);
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = createWriteOptions())
        {
            for (PreferenceRecord record : preferenceRecords)
            {
                byte[] key = encodeUuid(record.getId());
                batch.put(handle, key, serializeRecord(record));
            }
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to update preferences", e);
        }
    }

    @Override
    public synchronized void replace(final Collection<UUID> preferenceRecordsToRemove,
                                     final Collection<PreferenceRecord> preferenceRecordsToAdd)
    {
        if (_state != StoreState.OPENED)
        {
            throw new IllegalStateException("PreferenceStore is not opened");
        }
        if (preferenceRecordsToRemove.isEmpty() && preferenceRecordsToAdd.isEmpty())
        {
            return;
        }

        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle handle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.PREFERENCES);
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = createWriteOptions())
        {
            for (UUID id : preferenceRecordsToRemove)
            {
                batch.delete(handle, encodeUuid(id));
            }
            for (PreferenceRecord record : preferenceRecordsToAdd)
            {
                batch.put(handle, encodeUuid(record.getId()), serializeRecord(record));
            }
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to replace preferences", e);
        }
    }

    @Override
    public synchronized void onDelete()
    {
        if (_environment == null)
        {
            return;
        }
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle handle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.PREFERENCES);
        try
        {
            deleteAll(database, handle);
        }
        catch (StoreException e)
        {
            throw e;
        }
        finally
        {
            close();
        }
    }

    private WriteOptions createWriteOptions()
    {
        if (_settings == null)
        {
            return new WriteOptions();
        }
        return RocksDBOptionsFactory.createWriteOptions(_settings);
    }

    private Collection<PreferenceRecord> loadRecords(final RocksDB database, final ColumnFamilyHandle handle)
    {
        List<PreferenceRecord> records = new ArrayList<>();
        try (RocksIterator iterator = database.newIterator(handle))
        {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
            {
                byte[] key = iterator.key();
                if (isVersionKey(key))
                {
                    continue;
                }
                if (key.length != UUID_BYTES)
                {
                    continue;
                }
                StoredPreferenceRecord record = deserializeRecord(iterator.value());
                if (record.getId() == null)
                {
                    record.setId(decodeUuid(key));
                }
                if (record.getAttributes() == null)
                {
                    record.setAttributes(Collections.emptyMap());
                }
                records.add(record);
            }
        }
        return records;
    }

    private void replaceAll(final RocksDB database,
                            final ColumnFamilyHandle handle,
                            final Collection<PreferenceRecord> records,
                            final String version)
    {
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = createWriteOptions())
        {
            try (RocksIterator iterator = database.newIterator(handle))
            {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                {
                    batch.delete(handle, iterator.key());
                }
            }
            for (PreferenceRecord record : records)
            {
                batch.put(handle, encodeUuid(record.getId()), serializeRecord(record));
            }
            batch.put(handle, VERSION_KEY, version.getBytes(StandardCharsets.UTF_8));
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to update preferences", e);
        }
    }

    private void deleteAll(final RocksDB database, final ColumnFamilyHandle handle)
    {
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = createWriteOptions())
        {
            try (RocksIterator iterator = database.newIterator(handle))
            {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                {
                    batch.delete(handle, iterator.key());
                }
            }
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to delete preferences", e);
        }
    }

    private String readVersion(final RocksDB database, final ColumnFamilyHandle handle)
    {
        try
        {
            byte[] value = database.get(handle, VERSION_KEY);
            return value == null ? null : new String(value, StandardCharsets.UTF_8);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to read preferences version", e);
        }
    }

    private void writeVersion(final RocksDB database, final ColumnFamilyHandle handle, final String version)
    {
        try
        {
            database.put(handle, VERSION_KEY, version.getBytes(StandardCharsets.UTF_8));
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to write preferences version", e);
        }
    }

    private byte[] encodeUuid(final UUID id)
    {
        ByteBuffer buffer = ByteBuffer.allocate(UUID_BYTES);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        return buffer.array();
    }

    private UUID decodeUuid(final byte[] data)
    {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long most = buffer.getLong();
        long least = buffer.getLong();
        return new UUID(most, least);
    }

    private boolean isVersionKey(final byte[] key)
    {
        if (key.length != VERSION_KEY.length)
        {
            return false;
        }
        for (int i = 0; i < key.length; i++)
        {
            if (key[i] != VERSION_KEY[i])
            {
                return false;
            }
        }
        return true;
    }

    private byte[] serializeRecord(final PreferenceRecord record)
    {
        StoredPreferenceRecord stored = new StoredPreferenceRecord(record);
        try
        {
            return _objectMapper.writeValueAsBytes(stored);
        }
        catch (JacksonException e)
        {
            throw new StoreException("Failed to serialize preference record", e);
        }
    }

    private StoredPreferenceRecord deserializeRecord(final byte[] data)
    {
        try
        {
            return _objectMapper.readValue(data, StoredPreferenceRecord.class);
        }
        catch (JacksonException e)
        {
            throw new StoreException("Failed to deserialize preference record", e);
        }
    }

    private static class StoredPreferenceRecord implements PreferenceRecord
    {
        private UUID _id;
        private Map<String, Object> _attributes;

        public StoredPreferenceRecord()
        {
        }

        public StoredPreferenceRecord(final PreferenceRecord preferenceRecord)
        {
            _id = preferenceRecord.getId();
            _attributes = Collections.unmodifiableMap(new LinkedHashMap<>(preferenceRecord.getAttributes()));
        }

        @Override
        public UUID getId()
        {
            return _id;
        }

        public void setId(final UUID id)
        {
            _id = id;
        }

        @Override
        public Map<String, Object> getAttributes()
        {
            return _attributes;
        }

        public void setAttributes(final Map<String, Object> attributes)
        {
            _attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
        }
    }
}
