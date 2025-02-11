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
package org.apache.qpid.server.protocol.v1_0.codec;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface SpecializedDescribedType
{
    MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    Map<Class<?>, MethodHandle> METHOD_HANDLE_CACHE = new ConcurrentHashMap<>();
    String METHOD_NAME = "getInvalidValue";

    static <X extends SpecializedDescribedType> X getInvalidValue(final Class<X> clazz, final DescribedType value)
    {
        if (METHOD_HANDLE_CACHE.containsKey(clazz))
        {
            try
            {
                return clazz.cast(METHOD_HANDLE_CACHE.get(clazz).invoke(value));
            }
            catch (Throwable e)
            {
                return null;
            }
        }

        try
        {
            return clazz.cast(getMethodHandle(clazz).invoke(value));
        }
        catch (Throwable e)
        {
            return null;
        }
    }

    static <X extends SpecializedDescribedType> boolean hasInvalidValue(final Class<X> clazz)
    {
        if (METHOD_HANDLE_CACHE.containsKey(clazz))
        {
            return true;
        }

        try
        {
            getMethodHandle(clazz);
            return true;
        }
        catch (NoSuchMethodException | IllegalAccessException e)
        {
            return false;
        }
    }

    static <X extends SpecializedDescribedType> MethodHandle getMethodHandle(final Class<X> clazz)
            throws NoSuchMethodException, IllegalAccessException
    {
        final MethodType methodType = MethodType.methodType(clazz, DescribedType.class);
        final MethodHandle methodHandle = LOOKUP.findStatic(clazz, METHOD_NAME, methodType);
        METHOD_HANDLE_CACHE.put(clazz, methodHandle);
        return methodHandle;
    }
}
