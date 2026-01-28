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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RocksDBUtilsTest
{
    @Test
    void invokeWithTimeoutReturnsFalseWhenLoadHangs() throws Exception
    {
        Method method = RocksDBUtilsTest.class.getDeclaredMethod("hangingLoad");
        method.setAccessible(true);
        boolean result = RocksDBUtils.invokeWithTimeout(method, 1L);
        assertFalse(result, "Expected invokeWithTimeout to fail on timeout");
    }

    @Test
    void invokeWithTimeoutReturnsTrueWhenLoadCompletes() throws Exception
    {
        Method method = RocksDBUtilsTest.class.getDeclaredMethod("fastLoad");
        method.setAccessible(true);
        boolean result = RocksDBUtils.invokeWithTimeout(method, 1000L);
        assertTrue(result, "Expected invokeWithTimeout to succeed for fast call");
    }

    private static void hangingLoad() throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);
    }

    private static void fastLoad()
    {
    }
}
