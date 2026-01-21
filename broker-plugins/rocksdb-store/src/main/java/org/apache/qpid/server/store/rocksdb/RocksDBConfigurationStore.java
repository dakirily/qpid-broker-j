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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreProvider;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;
import org.apache.qpid.server.store.preferences.JsonFilePreferenceStore;
import org.apache.qpid.server.store.preferences.NoopPreferenceStoreFactoryService;
import org.apache.qpid.server.store.preferences.PreferenceStore;

/**
 * Provides a RocksDB-backed configuration store.
 *
 * Thread-safety: safe for concurrent access by the configuration model.
 */
public class RocksDBConfigurationStore implements DurableConfigurationStore, MessageStoreProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBConfigurationStore.class);
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_ATTRIBUTES = "attributes";
    private static final char HIERARCHY_SEPARATOR = '|';
    private static final String STORE_DIRECTORY = "store";

    private enum State { CLOSED, CONFIGURED, OPEN }

    private final Class<? extends ConfiguredObject> _rootClass;
    private final MessageStore _providedMessageStore;
    private final ObjectMapper _objectMapper;
    private final Object _lock = new Object();
    private State _state = State.CLOSED;
    private RocksDBEnvironment _environment;
    private PreferenceStore _preferenceStore;

    /**
     * Creates a RocksDB configuration store instance.
     *
     * @param rootClass root configured object class.
     */
    public RocksDBConfigurationStore(final Class<? extends ConfiguredObject> rootClass)
    {
        _rootClass = rootClass;
        _providedMessageStore = new RocksDBMessageStore();
        _objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(true);
    }

    /**
     * Initializes the configuration store.
     *
     * @param parent parent object.
     *
     * @throws StoreException on store errors.
     */
    @Override
    public void init(final ConfiguredObject<?> parent) throws StoreException
    {
        changeState(State.CLOSED, State.CONFIGURED);
        if (!(parent instanceof FileBasedSettings) || !(parent instanceof RocksDBSettings))
        {
            throw new StoreException("Parent does not provide RocksDB settings");
        }
        String baseStorePath = ((FileBasedSettings) parent).getStorePath();
        String dataStorePath = getDataStorePath(baseStorePath);
        createStoreDirectories(baseStorePath, dataStorePath);
        _environment = RocksDBEnvironmentImpl.open(dataStorePath, (RocksDBSettings) parent);
        _preferenceStore = createPreferenceStore(parent, baseStorePath);
        if (_providedMessageStore instanceof RocksDBMessageStore)
        {
            ((RocksDBMessageStore) _providedMessageStore).setEnvironment(_environment);
        }
    }

    /**
     * Upgrades the store structure if required.
     *
     * @throws StoreException on store errors.
     */
    @Override
    public void upgradeStoreStructure() throws StoreException
    {
        // No-op for initial RocksDB implementation.
    }

    /**
     * Opens the configuration store and loads records.
     *
     * @param handler record handler.
     * @param initialRecords initial records.
     *
     * @return true if the store was opened.
     *
     * @throws StoreException on store errors.
     */
    @Override
    public boolean openConfigurationStore(final ConfiguredObjectRecordHandler handler,
                                          final ConfiguredObjectRecord... initialRecords)
            throws StoreException
    {
        changeState(State.CONFIGURED, State.OPEN);
        List<ConfiguredObjectRecord> records = loadRecords();
        boolean isNew = records.isEmpty();
        if (isNew)
        {
            ConfiguredObjectRecord[] recordsToStore = initialRecords == null
                    ? new ConfiguredObjectRecord[0]
                    : initialRecords;
            if (recordsToStore.length > 0)
            {
                update(true, recordsToStore);
            }
            records = Arrays.asList(recordsToStore);
        }
        for (ConfiguredObjectRecord record : records)
        {
            handler.handle(record);
        }
        return isNew;
    }

    /**
     * Reloads configuration records.
     *
     * @param handler record handler.
     *
     * @throws StoreException on store errors.
     */
    @Override
    public void reload(final ConfiguredObjectRecordHandler handler) throws StoreException
    {
        assertState(State.OPEN);
        List<ConfiguredObjectRecord> records = loadRecords();
        for (ConfiguredObjectRecord record : records)
        {
            handler.handle(record);
        }
    }

    /**
     * Creates a configured object record.
     *
     * @param object record to create.
     *
     * @throws StoreException on store errors.
     */
    @Override
    public void create(final ConfiguredObjectRecord object) throws StoreException
    {
        assertState(State.OPEN);
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle objectsHandle =
                _environment.getColumnFamilyHandle(RocksDBColumnFamily.CONFIGURED_OBJECTS);
        byte[] key = encodeUuidKey(object.getId());
        try
        {
            if (database.get(objectsHandle, key) != null)
            {
                throw new StoreException("Configured object record with id " + object.getId() + " already exists");
            }
            try (WriteBatch batch = new WriteBatch();
                 WriteOptions options = new WriteOptions())
            {
                batch.put(objectsHandle, key, serializeRecord(object));
                writeHierarchyEntries(batch, object);
                database.write(options, batch);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to create configured object record", e);
        }
    }

    /**
     * Updates configured object records.
     *
     * @param createIfNecessary true to create missing records.
     * @param records records to update.
     *
     * @throws StoreException on store errors.
     */
    @Override
    public void update(final boolean createIfNecessary, final ConfiguredObjectRecord... records)
            throws StoreException
    {
        assertState(State.OPEN);
        if (records == null || records.length == 0)
        {
            return;
        }
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle objectsHandle =
                _environment.getColumnFamilyHandle(RocksDBColumnFamily.CONFIGURED_OBJECTS);
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = new WriteOptions())
        {
            for (ConfiguredObjectRecord record : records)
            {
                byte[] key = encodeUuidKey(record.getId());
                byte[] existing = database.get(objectsHandle, key);
                if (existing == null && !createIfNecessary)
                {
                    throw new StoreException("Configured object record with id " + record.getId() + " does not exist");
                }
                if (existing != null)
                {
                    deleteHierarchyEntries(batch, record.getId());
                }
                batch.put(objectsHandle, key, serializeRecord(record));
                writeHierarchyEntries(batch, record);
            }
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to update configured object records", e);
        }
    }

    /**
     * Removes configured object records.
     *
     * @param objects records to remove.
     *
     * @return removed object ids.
     *
     * @throws StoreException on store errors.
     * @throws StoreException on delete errors.
     */
    @Override
    public UUID[] remove(final ConfiguredObjectRecord... objects) throws StoreException
    {
        assertState(State.OPEN);
        if (objects == null || objects.length == 0)
        {
            return new UUID[0];
        }
        List<UUID> removed = new ArrayList<>();
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle objectsHandle =
                _environment.getColumnFamilyHandle(RocksDBColumnFamily.CONFIGURED_OBJECTS);
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = new WriteOptions())
        {
            for (ConfiguredObjectRecord record : objects)
            {
                byte[] key = encodeUuidKey(record.getId());
                if (database.get(objectsHandle, key) != null)
                {
                    removed.add(record.getId());
                    batch.delete(objectsHandle, key);
                    deleteHierarchyEntries(batch, record.getId());
                }
            }
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to remove configured object records", e);
        }
        return removed.toArray(new UUID[removed.size()]);
    }

    /**
     * Closes the configuration store.
     *
     * @throws StoreException on store errors.
     */
    @Override
    public void closeConfigurationStore() throws StoreException
    {
        if (_environment != null)
        {
            _environment.close();
            _environment = null;
        }
        synchronized (_lock)
        {
            _state = State.CLOSED;
        }
    }

    /**
     * Deletes the configuration store.
     *
     * @param parent parent object.
     *
     */
    @Override
    public void onDelete(final ConfiguredObject<?> parent)
    {
        if (!(parent instanceof FileBasedSettings))
        {
            return;
        }
        String storePath = ((FileBasedSettings) parent).getStorePath();
        if (storePath == null)
        {
            return;
        }
        if (!org.apache.qpid.server.util.FileUtils.delete(new java.io.File(storePath), true))
        {
            LOGGER.info("Failed to delete RocksDB store at location {}", storePath);
        }
    }

    /**
     * Returns the provided message store.
     *
     * @return the message store.
     */
    @Override
    public MessageStore getMessageStore()
    {
        return _providedMessageStore;
    }

    /**
     * Returns the preference store provided by the configuration store.
     *
     * @return the preference store.
     */
    public PreferenceStore getPreferenceStore()
    {
        return _preferenceStore;
    }

    /**
     * Returns the RocksDB environment.
     *
     * @return the RocksDB environment.
     */
    public RocksDBEnvironment getEnvironment()
    {
        return _environment;
    }

    /**
     * Creates a preference store for RocksDB-backed configurations.
     *
     * @param parent the owning configured object.
     * @param storePath the RocksDB store path.
     *
     * @return the preference store.
     */
    private PreferenceStore createPreferenceStore(final ConfiguredObject<?> parent, final String storePath)
    {
        if (storePath == null)
        {
            return new NoopPreferenceStoreFactoryService().createInstance(parent, Map.of());
        }
        String posixFilePermissions = parent.getContextValue(String.class, SystemConfig.POSIX_FILE_PERMISSIONS);
        String preferenceStorePath = new File(storePath, "preferences.json").getPath();
        return new JsonFilePreferenceStore(preferenceStorePath, posixFilePermissions);
    }

    /**
     * Returns the RocksDB data directory path under the configured store path.
     *
     * @param storePath the configured store path.
     *
     * @return the data store path.
     */
    private String getDataStorePath(final String storePath)
    {
        if (storePath == null || storePath.isEmpty())
        {
            return storePath;
        }
        File base = new File(storePath);
        if (STORE_DIRECTORY.equals(base.getName()))
        {
            return storePath;
        }
        return new File(base, STORE_DIRECTORY).getPath();
    }

    /**
     * Creates the base and data store directories when required.
     *
     * @param baseStorePath configured base store path.
     * @param dataStorePath data store directory path.
     *
     * @throws StoreException on directory creation failures.
     */
    private void createStoreDirectories(final String baseStorePath, final String dataStorePath) throws StoreException
    {
        if (baseStorePath == null || baseStorePath.isEmpty())
        {
            return;
        }
        try
        {
            Path basePath = Paths.get(baseStorePath);
            Files.createDirectories(basePath);
            if (dataStorePath != null && !dataStorePath.isEmpty())
            {
                Files.createDirectories(Paths.get(dataStorePath));
            }
        }
        catch (java.io.IOException | SecurityException e)
        {
            throw new StoreException("Failed to create RocksDB store directories for " + dataStorePath, e);
        }
    }

    /**
     * Loads configured object records from RocksDB.
     *
     * @return the configured object records.
     *
     * @throws StoreException on load errors.
     */
    private List<ConfiguredObjectRecord> loadRecords() throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle objectsHandle =
                _environment.getColumnFamilyHandle(RocksDBColumnFamily.CONFIGURED_OBJECTS);
        ColumnFamilyHandle hierarchyHandle =
                _environment.getColumnFamilyHandle(RocksDBColumnFamily.CONFIGURED_OBJECT_HIERARCHY);
        Map<UUID, RecordData> recordData = new LinkedHashMap<>();
        try (RocksIterator iterator = database.newIterator(objectsHandle))
        {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
            {
                UUID id = decodeUuidKey(iterator.key());
                recordData.put(id, deserializeRecord(iterator.value()));
            }
        }
        Map<UUID, Map<String, UUID>> parents = loadParents(database, hierarchyHandle);
        List<ConfiguredObjectRecord> records = new ArrayList<>(recordData.size());
        for (Map.Entry<UUID, RecordData> entry : recordData.entrySet())
        {
            Map<String, UUID> parentMap = parents.get(entry.getKey());
            if (parentMap == null)
            {
                parentMap = Map.of();
            }
            else
            {
                Map<String, UUID> filtered = new LinkedHashMap<>();
                for (Map.Entry<String, UUID> parentEntry : parentMap.entrySet())
                {
                    if (recordData.containsKey(parentEntry.getValue()))
                    {
                        filtered.put(parentEntry.getKey(), parentEntry.getValue());
                    }
                }
                parentMap = filtered.isEmpty() ? Map.of() : filtered;
            }
            RecordData data = entry.getValue();
            records.add(new ConfiguredObjectRecordImpl(entry.getKey(),
                                                       data.getType(),
                                                       data.getAttributes(),
                                                       parentMap));
        }
        return records;
    }

    /**
     * Loads parent mappings for configured objects.
     *
     * @param database the RocksDB instance.
     * @param hierarchyHandle the hierarchy column family handle.
     *
     * @return the parent mappings by child id.
     *
     * @throws StoreException on load errors.
     */
    private Map<UUID, Map<String, UUID>> loadParents(final RocksDB database,
                                                     final ColumnFamilyHandle hierarchyHandle) throws StoreException
    {
        Map<UUID, Map<String, UUID>> parents = new LinkedHashMap<>();
        try (RocksIterator iterator = database.newIterator(hierarchyHandle))
        {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
            {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                int separatorIndex = key.indexOf(HIERARCHY_SEPARATOR);
                if (separatorIndex <= 0 || separatorIndex >= key.length() - 1)
                {
                    throw new StoreException("Invalid hierarchy key: " + key);
                }
                UUID childId = UUID.fromString(key.substring(0, separatorIndex));
                String parentType = key.substring(separatorIndex + 1);
                UUID parentId = decodeUuidKey(iterator.value());
                parents.computeIfAbsent(childId, id -> new LinkedHashMap<>()).put(parentType, parentId);
            }
        }
        return parents;
    }

    /**
     * Serializes a configured object record.
     *
     * @param record configured object record.
     *
     * @return the serialized record bytes.
     *
     * @throws StoreException on serialization errors.
     */
    private byte[] serializeRecord(final ConfiguredObjectRecord record) throws StoreException
    {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put(FIELD_TYPE, record.getType());
        data.put(FIELD_ATTRIBUTES, record.getAttributes());
        try
        {
            return _objectMapper.writeValueAsBytes(data);
        }
        catch (JacksonException e)
        {
            throw new StoreException("Failed to serialize configured object record " + record.getId(), e);
        }
    }

    /**
     * Deserializes a configured object record payload.
     *
     * @param payload record payload bytes.
     *
     * @return the record data.
     *
     * @throws StoreException on deserialization errors.
     */
    private RecordData deserializeRecord(final byte[] payload) throws StoreException
    {
        Map<String, Object> data;
        try
        {
            data = _objectMapper.readValue(payload, Map.class);
        }
        catch (JacksonException e)
        {
            throw new StoreException("Failed to parse configured object record payload", e);
        }
        Object typeValue = data.get(FIELD_TYPE);
        Object attributesValue = data.get(FIELD_ATTRIBUTES);
        if (!(typeValue instanceof String))
        {
            throw new StoreException("Configured object record is missing type");
        }
        if (attributesValue == null)
        {
            attributesValue = Map.of();
        }
        if (!(attributesValue instanceof Map))
        {
            throw new StoreException("Configured object record attributes are not a map");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> attributes = (Map<String, Object>) attributesValue;
        return new RecordData((String) typeValue, attributes);
    }

    /**
     * Writes hierarchy entries for a configured object record.
     *
     * @param batch write batch.
     * @param record configured object record.
     *
     * @throws StoreException on write errors.
     */
    private void writeHierarchyEntries(final WriteBatch batch, final ConfiguredObjectRecord record)
            throws StoreException
    {
        ColumnFamilyHandle hierarchyHandle =
                _environment.getColumnFamilyHandle(RocksDBColumnFamily.CONFIGURED_OBJECT_HIERARCHY);
        for (Map.Entry<String, UUID> entry : record.getParents().entrySet())
        {
            String key = record.getId() + String.valueOf(HIERARCHY_SEPARATOR) + entry.getKey();
            try
            {
                batch.put(hierarchyHandle,
                          key.getBytes(StandardCharsets.UTF_8),
                          encodeUuidKey(entry.getValue()));
            }
            catch (RocksDBException e)
            {
                throw new StoreException("Failed to write hierarchy entry for " + record.getId(), e);
            }
        }
    }

    /**
     * Deletes hierarchy entries for the configured object id.
     *
     * @param batch write batch.
     * @param childId configured object id.
     *
     * @throws StoreException on delete errors.
     */
    private void deleteHierarchyEntries(final WriteBatch batch, final UUID childId) throws StoreException
    {
        ColumnFamilyHandle hierarchyHandle =
                _environment.getColumnFamilyHandle(RocksDBColumnFamily.CONFIGURED_OBJECT_HIERARCHY);
        byte[] prefix = (childId.toString() + HIERARCHY_SEPARATOR).getBytes(StandardCharsets.UTF_8);
        RocksDB database = _environment.getDatabase();
        try (RocksIterator iterator = database.newIterator(hierarchyHandle))
        {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next())
            {
                byte[] key = iterator.key();
                if (!hasPrefix(key, prefix))
                {
                    break;
                }
                byte[] copy = Arrays.copyOf(key, key.length);
                batch.delete(hierarchyHandle, copy);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to delete hierarchy entries for " + childId, e);
        }
    }

    /**
     * Encodes a UUID into a key byte array.
     *
     * @param id UUID to encode.
     *
     * @return the key bytes.
     */
    private byte[] encodeUuidKey(final UUID id)
    {
        return id.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decodes a UUID from a key byte array.
     *
     * @param key key bytes.
     *
     * @return the UUID value.
     *
     * @throws StoreException on invalid UUIDs.
     */
    private UUID decodeUuidKey(final byte[] key) throws StoreException
    {
        try
        {
            return UUID.fromString(new String(key, StandardCharsets.UTF_8));
        }
        catch (IllegalArgumentException e)
        {
            throw new StoreException("Invalid UUID key in RocksDB store", e);
        }
    }

    /**
     * Returns whether the key has the expected prefix.
     *
     * @param key the key bytes.
     * @param prefix the prefix bytes.
     *
     * @return true when the key starts with the prefix.
     */
    private boolean hasPrefix(final byte[] key, final byte[] prefix)
    {
        if (key.length < prefix.length)
        {
            return false;
        }
        for (int i = 0; i < prefix.length; i++)
        {
            if (key[i] != prefix[i])
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Asserts the current store state.
     *
     * @param state expected state.
     */
    private void assertState(final State state)
    {
        synchronized (_lock)
        {
            if (_state != state)
            {
                throw new IllegalStateException("Store must be " + state + " but is " + _state);
            }
        }
    }

    /**
     * Changes the state from an expected value.
     *
     * @param oldState expected state.
     * @param newState new state.
     */
    private void changeState(final State oldState, final State newState)
    {
        synchronized (_lock)
        {
            assertState(oldState);
            _state = newState;
        }
    }

    /**
     * Holds parsed record data.
     *
     * Thread-safety: immutable.
     */
    private static final class RecordData
    {
        private final String _type;
        private final Map<String, Object> _attributes;

        /**
         * Creates record data.
         *
         * @param type record type.
         * @param attributes record attributes.
         */
        private RecordData(final String type, final Map<String, Object> attributes)
        {
            _type = type;
            _attributes = attributes;
        }

        /**
         * Returns the record type.
         *
         * @return the record type.
         */
        private String getType()
        {
            return _type;
        }

        /**
         * Returns the record attributes.
         *
         * @return the record attributes.
         */
        private Map<String, Object> getAttributes()
        {
            return _attributes;
        }
    }
}
