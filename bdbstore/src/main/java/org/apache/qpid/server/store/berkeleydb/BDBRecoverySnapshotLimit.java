/*
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
 */
package org.apache.qpid.server.store.berkeleydb;

import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.DEFAULT_BDB_RECOVERY_SNAPSHOT_MAX_BYTES;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES;

import java.util.function.Function;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;

public final class BDBRecoverySnapshotLimit
{
    static final int BLOCK_BYTES = 8 * 1024;
    private static final long MAX_BYTES = (long) Integer.MAX_VALUE * BLOCK_BYTES;
    private static final String DEFAULT_BYTES = Long.toString(DEFAULT_BDB_RECOVERY_SNAPSHOT_MAX_BYTES);

    private BDBRecoverySnapshotLimit()
    {
    }

    static int getBlockLimit(final ConfiguredObject<?> host)
    {
        return getBlockLimit(name -> BDBUtils.getContextValue(host, String.class, name, DEFAULT_BYTES));
    }

    public static int getBlockLimit(final Function<String, String> contextValue)
    {
        final long maxBytes;
        try
        {
            maxBytes = Long.parseLong(contextValue.apply(QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES));
        }
        catch (final IllegalArgumentException e)
        {
            throw new IllegalConfigurationException(QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES +
                                                    " must be a whole number of bytes", e);
        }
        if (maxBytes < BLOCK_BYTES || maxBytes > MAX_BYTES)
        {
            throw new IllegalConfigurationException(QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES +
                                                    " must be between " + BLOCK_BYTES + " and " + MAX_BYTES + " bytes");
        }
        return (int) (maxBytes / BLOCK_BYTES);
    }
}
