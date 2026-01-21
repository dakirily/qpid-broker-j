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

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides availability checks for RocksDB.
 * <br>
 * Thread-safety: safe for concurrent access.
 */
public final class RocksDBUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBUtils.class);
    private static final long DEFAULT_LOAD_TIMEOUT_MS = 10_000L;
    private static final Object AVAILABILITY_LOCK = new Object();
    private static volatile Boolean AVAILABLE;

    /**
     * Prevents instantiation.
     */
    private RocksDBUtils()
    {
    }

    /**
     * Returns whether RocksDB classes and native libraries are available.
     *
     * @return true if RocksDB is available.
     */
    public static boolean isAvailable()
    {
        Boolean available = AVAILABLE;
        if (available != null)
        {
            return available;
        }
        synchronized (AVAILABILITY_LOCK)
        {
            if (AVAILABLE != null)
            {
                return AVAILABLE;
            }
            AVAILABLE = checkAvailability();
            return AVAILABLE;
        }
    }

    private static boolean checkAvailability()
    {
        try
        {
            Class<?> rocksDbClass = Class.forName("org.rocksdb.RocksDB");
            Method loadLibrary = rocksDbClass.getMethod("loadLibrary");
            long timeoutMs = Long.getLong("qpid.rocksdb.loadLibraryTimeoutMs", DEFAULT_LOAD_TIMEOUT_MS);
            if (timeoutMs <= 0)
            {
                loadLibrary.invoke(null);
                return true;
            }
            return invokeWithTimeout(loadLibrary, timeoutMs);
        }
        catch (ReflectiveOperationException | NoClassDefFoundError e)
        {
            return false;
        }
    }

    static boolean invokeWithTimeout(final Method loadLibrary, final long timeoutMs)
    {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread loader = new Thread(() ->
        {
            try
            {
                loadLibrary.invoke(null);
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
        }, "rocksdb-load-library");
        loader.setDaemon(true);
        loader.start();
        try
        {
            loader.join(timeoutMs);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return false;
        }
        if (loader.isAlive())
        {
            LOGGER.warn("RocksDB native library load did not complete within {} ms", timeoutMs);
            return false;
        }
        if (failure.get() != null)
        {
            LOGGER.warn("RocksDB native library load failed", failure.get());
            return false;
        }
        return true;
    }
}
