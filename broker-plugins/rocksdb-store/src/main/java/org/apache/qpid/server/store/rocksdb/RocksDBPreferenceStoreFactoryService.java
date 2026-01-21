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

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreFactoryService;

@SuppressWarnings("unused")
@PluggableService
public class RocksDBPreferenceStoreFactoryService implements PreferenceStoreFactoryService
{
    public static final String TYPE = "RocksDB";

    @Override
    public PreferenceStore createInstance(final ConfiguredObject<?> parent,
                                          final Map<String, Object> preferenceStoreAttributes)
    {
        final RocksDBEnvironment environment = resolveEnvironment(parent);
        final RocksDBSettings settings = resolveSettings(parent);
        if (environment == null || settings == null)
        {
            throw new IllegalConfigurationException(
                    "RocksDB preference store requires RocksDB environment and settings");
        }
        return new RocksDBPreferenceStore(environment, settings);
    }

    @Override
    public String getType()
    {
        return TYPE;
    }

    private RocksDBEnvironment resolveEnvironment(final ConfiguredObject<?> parent)
    {
        if (parent instanceof RocksDBContainer)
        {
            return ((RocksDBContainer<?>) parent).getRocksDBEnvironment();
        }
        if (parent instanceof SystemConfig)
        {
            final DurableConfigurationStore store = ((SystemConfig<?>) parent).getConfigurationStore();
            if (store instanceof RocksDBConfigurationStore)
            {
                return ((RocksDBConfigurationStore) store).getEnvironment();
            }
        }
        return null;
    }

    private RocksDBSettings resolveSettings(final ConfiguredObject<?> parent)
    {
        if (parent instanceof RocksDBSettings)
        {
            return (RocksDBSettings) parent;
        }
        return null;
    }
}
