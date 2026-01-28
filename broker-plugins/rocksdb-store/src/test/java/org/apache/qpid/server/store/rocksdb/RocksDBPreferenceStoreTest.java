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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceRecordImpl;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class RocksDBPreferenceStoreTest extends UnitTestBase
{
    private String _storePath;
    private RocksDBEnvironment _environment;
    private RocksDBSettings _settings;
    private RocksDBPreferenceStore _store;
    private PreferenceStoreUpdater _updater;

    @BeforeEach
    public void setUp() throws Exception
    {
        Assumptions.assumeTrue(RocksDBUtils.isAvailable(), "RocksDB is not available");
        _storePath = TMP_FOLDER + File.separator + getTestName() + System.currentTimeMillis();
        _settings = createSettings();
        _environment = RocksDBEnvironmentImpl.open(_storePath, _settings);
        _store = new RocksDBPreferenceStore(_environment, _settings);
        _updater = mock(PreferenceStoreUpdater.class);
        when(_updater.getLatestVersion()).thenReturn(BrokerModel.MODEL_VERSION);
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_store != null)
        {
            _store.close();
        }
        if (_environment != null)
        {
            _environment.close();
        }
        if (_storePath != null)
        {
            FileUtils.delete(new File(_storePath), true);
        }
    }

    @Test
    public void testOpenAndLoadEmpty()
    {
        Collection<PreferenceRecord> records = _store.openAndLoad(_updater);
        assertTrue(records.isEmpty(), "Expected empty preference store");
    }

    @Test
    public void testUpdateOrCreate()
    {
        UUID id = randomUUID();
        Map<String, Object> attributes = Map.of("test1", "test2");
        PreferenceRecord record = new PreferenceRecordImpl(id, attributes);

        _store.openAndLoad(_updater);
        _store.updateOrCreate(Set.of(record));

        reopenStore();
        Collection<PreferenceRecord> records = _store.openAndLoad(_updater);
        assertSingleRecord(records, id, attributes);
    }

    @Test
    public void testReplace()
    {
        UUID id = randomUUID();
        Map<String, Object> attributes = Map.of("test1", "test2");
        PreferenceRecord record = new PreferenceRecordImpl(id, attributes);

        UUID newId = randomUUID();
        Map<String, Object> newAttributes = Map.of("test3", "test4");
        PreferenceRecord newRecord = new PreferenceRecordImpl(newId, newAttributes);

        _store.openAndLoad(_updater);
        _store.updateOrCreate(Set.of(record));
        _store.replace(Set.of(id), Set.of(newRecord));

        reopenStore();
        Collection<PreferenceRecord> records = _store.openAndLoad(_updater);
        assertSingleRecord(records, newId, newAttributes);
    }

    @Test
    public void testReplaceToDelete()
    {
        UUID id = randomUUID();
        Map<String, Object> attributes = Map.of("test1", "test2");
        PreferenceRecord record = new PreferenceRecordImpl(id, attributes);

        _store.openAndLoad(_updater);
        _store.updateOrCreate(Set.of(record));
        _store.replace(Set.of(id), List.of());

        reopenStore();
        Collection<PreferenceRecord> records = _store.openAndLoad(_updater);
        assertTrue(records.isEmpty(), "Expected preference store to be empty");
    }

    @Test
    public void testUpdateFailIfNotOpened()
    {
        assertThrows(IllegalStateException.class,
                     () -> _store.updateOrCreate(List.of()),
                     "Should not be able to update or create");
    }

    @Test
    public void testReplaceFailIfNotOpened()
    {
        assertThrows(IllegalStateException.class,
                     () -> _store.replace(List.of(), List.of()),
                     "Should not be able to replace");
    }

    private void reopenStore()
    {
        _store.close();
        _store = new RocksDBPreferenceStore(_environment, _settings);
    }

    private void assertSingleRecord(final Collection<PreferenceRecord> records,
                                    final UUID id,
                                    final Map<String, Object> attributes)
    {
        assertEquals(1, records.size(), "Unexpected size of preference store");
        PreferenceRecord record = records.iterator().next();
        assertEquals(id, record.getId(), "Unexpected preference id");
        assertEquals(attributes, new HashMap<>(record.getAttributes()), "Unexpected preference attributes");
    }

    private RocksDBSettings createSettings()
    {
        RocksDBSettings settings = mock(RocksDBSettings.class);
        when(settings.getCreateIfMissing()).thenReturn(true);
        when(settings.getCreateMissingColumnFamilies()).thenReturn(true);
        return settings;
    }
}
