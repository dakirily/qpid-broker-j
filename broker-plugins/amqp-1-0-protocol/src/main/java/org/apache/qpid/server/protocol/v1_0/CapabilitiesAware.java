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

package org.apache.qpid.server.protocol.v1_0;

import org.apache.qpid.server.protocol.v1_0.type.Symbol;

import java.util.Objects;

public interface CapabilitiesAware
{
    default boolean contains(final Symbol[] array, final Symbol symbol)
    {
        if (null == array)
        {
            return false;
        }
        for (final Symbol element : array)
        {
            if (Objects.equals(element, symbol))
            {
                return true;
            }
        }
        return false;
    }

    default boolean contains(final Symbol[] array, final Symbol symbol1, final Symbol symbol2)
    {
        if (null == array)
        {
            return false;
        }
        boolean found1 = false;
        boolean found2 = false;
        for (final Symbol element : array)
        {
            if (Objects.equals(element, symbol1))
            {
                found1 = true;
            }
            if (Objects.equals(element, symbol2))
            {
                found2 = true;
            }
            if (found1 && found2)
            {
                return true;
            }
        }
        return false;
    }

    default boolean contains(final Symbol[] array, final Symbol symbol1, final Symbol symbol2, final Symbol symbol3)
    {
        if (null == array)
        {
            return false;
        }
        boolean found1 = false;
        boolean found2 = false;
        boolean found3 = false;
        for (final Symbol element : array)
        {
            if (Objects.equals(element, symbol1))
            {
                found1 = true;
            }
            if (Objects.equals(element, symbol2))
            {
                found2 = true;
            }
            if (Objects.equals(element, symbol3))
            {
                found3 = true;
            }
            if (found1 && found2 && found3)
            {
                return true;
            }
        }
        return false;
    }

    default boolean hasCapability(final Symbol symbol)
    {
        return contains(getCapabilities(), symbol);
    }

    default boolean hasCapabilities(final Symbol symbol1, final Symbol symbol2)
    {
        return contains(getCapabilities(), symbol1, symbol2);
    }

    default boolean hasCapabilities(final Symbol symbol1, final Symbol symbol2, final Symbol symbol3)
    {
        return contains(getCapabilities(), symbol1, symbol2, symbol3);
    }

    default boolean hasOutcome(final Symbol symbol)
    {
        return contains(getOutcomes(), symbol);
    }

    default Symbol[] getCapabilities()
    {
        return new Symbol[0];
    }

    default Symbol[] getOutcomes()
    {
        return new Symbol[0];
    }
}
