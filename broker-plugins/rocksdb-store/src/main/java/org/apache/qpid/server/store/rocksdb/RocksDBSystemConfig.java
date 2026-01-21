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

import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.store.SizeMonitoringSettings;
import org.apache.qpid.server.store.preferences.PreferenceStoreAttributes;
import org.apache.qpid.server.store.preferences.PreferenceStoreProvider;

/**
 * Defines system-level configuration for the RocksDB store.
 *
 * Thread-safety: safe for concurrent access by the configuration model.
 *
 * @param <X> the system config type.
 */
public interface RocksDBSystemConfig<X extends RocksDBSystemConfig<X>> extends SystemConfig<X>,
                                                                              FileBasedSettings,
                                                                              SizeMonitoringSettings,
                                                                              PreferenceStoreProvider,
                                                                              RocksDBManagedSettings<X>
{
    /**
     * Returns the store path for broker configuration.
     *
     * @return the store path.
     */
    @Override
    @ManagedAttribute(defaultValue = "${qpid.work_dir}${file.separator}config")
    String getStorePath();

    /**
     * Returns the store underfull size threshold.
     *
     * @return the underfull size threshold.
     */
    @Override
    @ManagedAttribute(mandatory = true, defaultValue = "0")
    Long getStoreUnderfullSize();

    /**
     * Returns the store overfull size threshold.
     *
     * @return the overfull size threshold.
     */
    @Override
    @ManagedAttribute(mandatory = true, defaultValue = "0")
    Long getStoreOverfullSize();

    /**
     * Returns preference store attributes.
     *
     * @return preference store attributes.
     */
    @Override
    @ManagedAttribute(description = "Configuration for the preference store, e.g. type, path, etc.",
                      defaultValue = "{\"type\": \"Provided\"}")
    PreferenceStoreAttributes getPreferenceStoreAttributes();
}
