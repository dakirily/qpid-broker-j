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
 *
 */

package org.apache.qpid.server.virtualhost;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

public class NullCache<K, V> implements Cache<K, V>
{
    @Override
    public V getIfPresent(final Object key)
    {
        return null;
    }


    @Override
    public V get(final K k, final Function<? super K, ? extends V> function)
    {
        return null;
    }

    @Override
    public Map<K, V> getAllPresent(final Iterable<? extends K> iterable)
    {
        return Map.of();
    }

    @Override
    public Map<K, V> getAll(final Iterable<? extends K> iterable,
                            final Function<? super Set<? extends K>, ? extends Map<? extends K, ? extends V>> function)
    {
        return Map.of();
    }

    @Override
    public void put(final K key, final V value)
    {
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m)
    {
    }

    @Override
    public void invalidateAll(final Iterable<? extends K> iterable)
    {

    }

    @Override
    public void invalidate(final Object key)
    {
    }


    @Override
    public void invalidateAll()
    {
    }

    @Override
    public long estimatedSize()
    {
        return 0;
    }

    @Override
    public CacheStats stats()
    {
        return null;
    }

    @Override
    public ConcurrentMap<K, V> asMap()
    {
        return new ConcurrentHashMap<>();
    }

    @Override
    public void cleanUp()
    {
    }

    @Override
    public Policy<K, V> policy()
    {
        return null;
    }
}
