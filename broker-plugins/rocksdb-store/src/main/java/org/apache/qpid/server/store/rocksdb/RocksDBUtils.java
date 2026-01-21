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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Provides availability checks for RocksDB.
 *
 * Thread-safety: safe for concurrent access.
 */
public final class RocksDBUtils
{
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
        try
        {
            Class<?> rocksDbClass = Class.forName("org.rocksdb.RocksDB");
            Method loadLibrary = rocksDbClass.getMethod("loadLibrary");
            loadLibrary.invoke(null);
            return true;
        }
        catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e)
        {
            return false;
        }
        catch (InvocationTargetException e)
        {
            return false;
        }
        catch (NoClassDefFoundError e)
        {
            return false;
        }
    }
}
