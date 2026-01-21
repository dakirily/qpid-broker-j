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

import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v1_0.store.LinkStore;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreFactory;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.rocksdb.RocksDBContainer;

/**
 * Provides a link store factory backed by RocksDB.
 *
 * Thread-safety: safe for concurrent access.
 */
@PluggableService
public class RocksDBLinkStoreFactory implements LinkStoreFactory
{
    /**
     * The link store type name.
     */
    private static final String TYPE = "ROCKSDB";

    /**
     * Returns the link store type.
     *
     * @return the link store type.
     */
    @Override
    public String getType()
    {
        return TYPE;
    }

    /**
     * Creates a RocksDB link store for the given address space.
     *
     * @param addressSpace address space.
     *
     * @return the link store.
     *
     * @throws StoreException when the address space is not supported.
     */
    @Override
    public LinkStore create(final NamedAddressSpace addressSpace)
    {
        VirtualHost<?> virtualHost = (VirtualHost<?>) addressSpace;
        if (virtualHost instanceof RocksDBContainer)
        {
            return new RocksDBLinkStore((RocksDBContainer<?>) virtualHost);
        }
        else if (virtualHost.getParent() instanceof RocksDBContainer)
        {
            return new RocksDBLinkStore((RocksDBContainer<?>) virtualHost.getParent());
        }
        else
        {
            throw new StoreException("Cannot create RocksDB Link Store for " + addressSpace);
        }
    }

    /**
     * Returns whether the factory supports the given address space.
     *
     * @param addressSpace address space.
     *
     * @return true if supported.
     */
    @Override
    public boolean supports(final NamedAddressSpace addressSpace)
    {
        if (addressSpace instanceof VirtualHost)
        {
            if (addressSpace instanceof RocksDBContainer)
            {
                return true;
            }
            else if (((VirtualHost<?>) addressSpace).getParent() instanceof RocksDBContainer)
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns the priority of the factory.
     *
     * @return the priority.
     */
    @Override
    public int getPriority()
    {
        return 100;
    }
}
