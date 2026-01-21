/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.qpid.server.protocol.v1_0.store.rocksdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ModelVersion;
import org.apache.qpid.server.protocol.v1_0.LinkDefinition;
import org.apache.qpid.server.protocol.v1_0.LinkDefinitionImpl;
import org.apache.qpid.server.protocol.v1_0.LinkKey;
import org.apache.qpid.server.protocol.v1_0.store.AbstractLinkStore;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreUpdater;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreUtils;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.rocksdb.RocksDBColumnFamily;
import org.apache.qpid.server.store.rocksdb.RocksDBContainer;
import org.apache.qpid.server.store.rocksdb.RocksDBEnvironment;
import org.apache.qpid.server.store.rocksdb.RocksDBOptionsFactory;
import org.apache.qpid.server.store.rocksdb.RocksDBSettings;

/**
 * Provides a RocksDB-backed link store.
 *
 * Thread-safety: safe for concurrent access by the configuration model.
 */
public class RocksDBLinkStore extends AbstractLinkStore
{
    private static final byte CURRENT_LINK_VALUE_VERSION = 0;
    private static final byte[] VERSION_KEY = "amqp_1_0_links_version".getBytes(StandardCharsets.US_ASCII);

    private final RocksDBContainer<?> _container;

    /**
     * Creates a RocksDB link store instance.
     *
     * @param container RocksDB container.
     */
    public RocksDBLinkStore(final RocksDBContainer<?> container)
    {
        _container = container;
    }

    /**
     * Opens the store and loads link definitions.
     *
     * @param updater link store updater.
     *
     * @return link definitions.
     *
     */
    @Override
    protected Collection<LinkDefinition<Source, Target>> doOpenAndLoad(final LinkStoreUpdater updater)
    {
        RocksDBEnvironment environment = getEnvironment();
        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle linksHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.LINKS);
        ColumnFamilyHandle versionHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.VERSION);
        ModelVersion currentVersion =
                new ModelVersion(BrokerModel.MODEL_MAJOR_VERSION, BrokerModel.MODEL_MINOR_VERSION);
        ModelVersion storedVersion = readStoredVersion(database, versionHandle);
        if (storedVersion == null)
        {
            writeStoredVersion(database, versionHandle, currentVersion);
            storedVersion = currentVersion;
        }
        Collection<LinkDefinition<Source, Target>> links = loadLinks(database, linksHandle);
        if (currentVersion.lessThan(storedVersion))
        {
            throw new StoreException(String.format("Cannot downgrade link store from '%s' to '%s'",
                                                   storedVersion,
                                                   currentVersion));
        }
        if (storedVersion.lessThan(currentVersion))
        {
            links = updater.update(storedVersion.toString(), links);
            rewriteLinks(database, linksHandle, links);
            writeStoredVersion(database, versionHandle, currentVersion);
        }
        return links;
    }

    /**
     * Closes the store.
     */
    @Override
    protected void doClose()
    {
    }

    /**
     * Deletes the store data.
     *
     */
    @Override
    protected void doDelete()
    {
        RocksDBEnvironment environment = _container.getRocksDBEnvironment();
        if (environment == null)
        {
            return;
        }

        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle linksHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.LINKS);
        ColumnFamilyHandle versionHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.VERSION);
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = createWriteOptions();
             RocksIterator iterator = database.newIterator(linksHandle))
        {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
            {
                byte[] key = Arrays.copyOf(iterator.key(), iterator.key().length);
                batch.delete(linksHandle, key);
            }
            batch.delete(versionHandle, VERSION_KEY);
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to delete link store data", e);
        }
    }

    /**
     * Saves a link definition.
     *
     * @param link link definition to save.
     *
     */
    @Override
    protected void doSaveLink(final LinkDefinition<Source, Target> link)
    {
        RocksDBEnvironment environment = getEnvironment();
        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle linksHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.LINKS);
        LinkKey linkKey = new LinkKey(link);
        try
        {
            try (WriteOptions options = createWriteOptions())
            {
                database.put(linksHandle, options, encodeLinkKey(linkKey), encodeLinkValue(link));
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException(String.format("Failed saving link '%s'", linkKey), e);
        }
    }

    /**
     * Deletes a link definition.
     *
     * @param link link definition to delete.
     *
     */
    @Override
    protected void doDeleteLink(final LinkDefinition<Source, Target> link)
    {
        RocksDBEnvironment environment = getEnvironment();
        RocksDB database = environment.getDatabase();
        ColumnFamilyHandle linksHandle = environment.getColumnFamilyHandle(RocksDBColumnFamily.LINKS);
        LinkKey linkKey = new LinkKey(link);
        try
        {
            try (WriteOptions options = createWriteOptions())
            {
                database.delete(linksHandle, options, encodeLinkKey(linkKey));
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException(String.format("Failed deleting link '%s'", linkKey), e);
        }
    }

    /**
     * Returns the highest supported terminus durability.
     *
     * @return the highest supported terminus durability.
     */
    @Override
    public TerminusDurability getHighestSupportedTerminusDurability()
    {
        return TerminusDurability.CONFIGURATION;
    }

    /**
     * Returns the associated container.
     *
     * @return the RocksDB container.
     */
    public RocksDBContainer<?> getContainer()
    {
        return _container;
    }

    /**
     * Returns the active RocksDB environment.
     *
     * @return the RocksDB environment.
     *
     * @throws StoreException if the environment is not available.
     */
    private RocksDBEnvironment getEnvironment()
    {
        RocksDBEnvironment environment = _container.getRocksDBEnvironment();
        if (environment == null)
        {
            throw new StoreException("RocksDB environment is not available");
        }
        return environment;
    }

    /**
     * Loads link definitions from RocksDB.
     *
     * @param database RocksDB instance.
     * @param linksHandle column family handle.
     *
     * @return the loaded link definitions.
     */
    private Collection<LinkDefinition<Source, Target>> loadLinks(final RocksDB database,
                                                                 final ColumnFamilyHandle linksHandle)
    {
        Collection<LinkDefinition<Source, Target>> links = new ArrayList<>();
        try (RocksIterator iterator = database.newIterator(linksHandle))
        {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
            {
                LinkKey linkKey = decodeLinkKey(iterator.key());
                LinkValue linkValue = decodeLinkValue(iterator.value());
                links.add(new LinkDefinitionImpl<>(linkKey.getRemoteContainerId(),
                                                   linkKey.getLinkName(),
                                                   linkKey.getRole(),
                                                   linkValue.getSource(),
                                                   linkValue.getTarget()));
            }
        }
        return links;
    }

    /**
     * Rewrites all link entries with the updated collection.
     *
     * @param database RocksDB instance.
     * @param linksHandle column family handle.
     * @param links updated link definitions.
     *
     * @throws StoreException on write failures.
     */
    private void rewriteLinks(final RocksDB database,
                              final ColumnFamilyHandle linksHandle,
                              final Collection<LinkDefinition<Source, Target>> links)
    {
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = createWriteOptions();
             RocksIterator iterator = database.newIterator(linksHandle))
        {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
            {
                byte[] key = Arrays.copyOf(iterator.key(), iterator.key().length);
                batch.delete(linksHandle, key);
            }
            for (LinkDefinition<Source, Target> link : links)
            {
                LinkKey linkKey = new LinkKey(link);
                batch.put(linksHandle, encodeLinkKey(linkKey), encodeLinkValue(link));
            }
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to rewrite link definitions", e);
        }
    }

    /**
     * Reads the stored version from RocksDB.
     *
     * @param database RocksDB instance.
     * @param versionHandle column family handle.
     *
     * @return the stored version or null if not present.
     */
    private ModelVersion readStoredVersion(final RocksDB database, final ColumnFamilyHandle versionHandle)
    {
        try
        {
            byte[] value = database.get(versionHandle, VERSION_KEY);
            if (value == null)
            {
                return null;
            }
            return ModelVersion.fromString(new String(value, StandardCharsets.US_ASCII));
        }
        catch (RocksDBException | IllegalArgumentException e)
        {
            throw new StoreException("Failed to read link store version", e);
        }
    }

    /**
     * Writes the current version to RocksDB.
     *
     * @param database RocksDB instance.
     * @param versionHandle column family handle.
     * @param version version to write.
     */
    private void writeStoredVersion(final RocksDB database,
                                    final ColumnFamilyHandle versionHandle,
                                    final ModelVersion version)
    {
        try
        {
            try (WriteOptions options = createWriteOptions())
            {
                database.put(versionHandle,
                             options,
                             VERSION_KEY,
                             version.toString().getBytes(StandardCharsets.US_ASCII));
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to write link store version", e);
        }
    }

    private WriteOptions createWriteOptions()
    {
        if (_container instanceof RocksDBSettings)
        {
            return RocksDBOptionsFactory.createWriteOptions((RocksDBSettings) _container);
        }
        return new WriteOptions();
    }

    /**
     * Encodes a link key into bytes.
     *
     * @param linkKey link key.
     *
     * @return encoded key bytes.
     */
    private byte[] encodeLinkKey(final LinkKey linkKey)
    {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream();
             DataOutputStream output = new DataOutputStream(stream))
        {
            writeBytes(output, linkKey.getRemoteContainerId().getBytes(StandardCharsets.UTF_8));
            writeBytes(output, linkKey.getLinkName().getBytes(StandardCharsets.UTF_8));
            output.writeBoolean(linkKey.getRole().getValue());
            return stream.toByteArray();
        }
        catch (IOException e)
        {
            throw new StoreException("Failed to encode link key", e);
        }
    }

    /**
     * Decodes a link key from bytes.
     *
     * @param key encoded key bytes.
     *
     * @return the decoded link key.
     */
    private LinkKey decodeLinkKey(final byte[] key)
    {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(key)))
        {
            String remoteContainerId = new String(readBytes(input), StandardCharsets.UTF_8);
            String linkName = new String(readBytes(input), StandardCharsets.UTF_8);
            Role role = Role.valueOf(input.readBoolean());
            return new LinkKey(remoteContainerId, linkName, role);
        }
        catch (IOException | IllegalArgumentException e)
        {
            throw new StoreException("Failed to decode link key", e);
        }
    }

    /**
     * Encodes a link value into bytes.
     *
     * @param link link definition.
     *
     * @return encoded link value bytes.
     */
    private byte[] encodeLinkValue(final LinkDefinition<Source, Target> link)
    {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream();
             DataOutputStream output = new DataOutputStream(stream))
        {
            output.writeByte(CURRENT_LINK_VALUE_VERSION);
            writeBytes(output, LinkStoreUtils.objectToAmqpBytes(link.getSource()));
            writeBytes(output, LinkStoreUtils.objectToAmqpBytes(link.getTarget()));
            return stream.toByteArray();
        }
        catch (IOException e)
        {
            throw new StoreException("Failed to encode link value", e);
        }
    }

    /**
     * Decodes a link value from bytes.
     *
     * @param data encoded value bytes.
     *
     * @return decoded link value.
     */
    private LinkValue decodeLinkValue(final byte[] data)
    {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(data)))
        {
            byte version = input.readByte();
            if (version != CURRENT_LINK_VALUE_VERSION)
            {
                throw new StoreException("Unsupported link value version " + version);
            }
            Object source = LinkStoreUtils.amqpBytesToObject(readBytes(input));
            if (!(source instanceof Source))
            {
                throw new StoreException("Unexpected source type " + formatType(source));
            }
            Object target = LinkStoreUtils.amqpBytesToObject(readBytes(input));
            if (!(target instanceof Target))
            {
                throw new StoreException("Unexpected target type " + formatType(target));
            }
            return new LinkValue((Source) source, (Target) target, version);
        }
        catch (IOException e)
        {
            throw new StoreException("Failed to decode link value", e);
        }
    }

    /**
     * Writes a length-prefixed byte array.
     *
     * @param output output stream.
     * @param data bytes to write.
     *
     * @throws IOException on write failures.
     */
    private void writeBytes(final DataOutputStream output, final byte[] data) throws IOException
    {
        output.writeInt(data.length);
        output.write(data);
    }

    /**
     * Reads a length-prefixed byte array.
     *
     * @param input input stream.
     *
     * @return the decoded bytes.
     *
     * @throws IOException on read failures.
     */
    private byte[] readBytes(final DataInputStream input) throws IOException
    {
        int size = input.readInt();
        if (size < 0)
        {
            throw new IOException("Negative payload size " + size);
        }
        byte[] data = new byte[size];
        input.readFully(data);
        return data;
    }

    /**
     * Formats a type name for diagnostics.
     *
     * @param value value to format.
     *
     * @return the type name or "null".
     */
    private String formatType(final Object value)
    {
        return value == null ? "null" : value.getClass().getName();
    }

    /**
     * Holds decoded link value data.
     *
     * Thread-safety: immutable.
     */
    private static final class LinkValue
    {
        private final Source _source;
        private final Target _target;
        private final byte _version;

        /**
         * Creates a link value wrapper.
         *
         * @param source source terminus.
         * @param target target terminus.
         * @param version value version.
         */
        private LinkValue(final Source source, final Target target, final byte version)
        {
            _source = source;
            _target = target;
            _version = version;
        }

        /**
         * Returns the source terminus.
         *
         * @return the source terminus.
         */
        private Source getSource()
        {
            return _source;
        }

        /**
         * Returns the target terminus.
         *
         * @return the target terminus.
         */
        private Target getTarget()
        {
            return _target;
        }

        /**
         * Returns the stored version.
         *
         * @return the stored version.
         */
        private byte getVersion()
        {
            return _version;
        }
    }
}
