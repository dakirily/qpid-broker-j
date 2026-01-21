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

import java.nio.charset.StandardCharsets;

/**
 * Defines RocksDB column family names used by the store.
 *
 * Thread-safety: safe for concurrent access.
 */
public enum RocksDBColumnFamily
{
    /**
     * Default RocksDB column family.
     */
    DEFAULT("default"),
    /**
     * Configured object records.
     */
    CONFIGURED_OBJECTS("configured_objects"),
    /**
     * Configured object hierarchy.
     */
    CONFIGURED_OBJECT_HIERARCHY("configured_object_hierarchy"),
    /**
     * Preferences store data.
     */
    PREFERENCES("preferences"),
    /**
     * Message metadata.
     */
    MESSAGE_METADATA("message_metadata"),
    /**
     * Message content.
     */
    MESSAGE_CONTENT("message_content"),
    /**
     * Queue entry records.
     */
    QUEUE_ENTRIES("queue_entries"),
    /**
     * Distributed transaction records.
     */
    XIDS("xids"),
    /**
     * Store version record.
     */
    VERSION("version"),
    /**
     * AMQP 1.0 link store.
     */
    LINKS("amqp_1_0_links");

    private final String _name;
    private final byte[] _nameBytes;

    /**
     * Creates a column family entry.
     *
     * @param name column family name.
     */
    RocksDBColumnFamily(final String name)
    {
        _name = name;
        _nameBytes = name.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Returns the column family name.
     *
     * @return the column family name.
     */
    public String getName()
    {
        return _name;
    }

    /**
     * Returns the column family name bytes.
     *
     * @return the column family name bytes.
     */
    public byte[] getNameBytes()
    {
        return _nameBytes;
    }
}
