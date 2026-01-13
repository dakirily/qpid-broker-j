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

package org.apache.qpid.server.protocol.v1_0;

import org.apache.qpid.server.protocol.v1_0.type.Symbol;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Utility interface provides helper methods for {@link Symbol} related operations */
public interface SymbolAware
{
    /** {@link Symbol} array of zero length */
    Symbol[] EMPTY_SYMBOL_ARRAY = new Symbol[0];

    /**
     * Returns true if source array contains supplied symbol, false otherwise
     * @param array {@link Symbol} array
     * @param symbol {@link Symbol} instance to be searched for
     * @return True if source array contains supplied symbol, false otherwise
     */
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

    /**
     * Returns true if source array contains both supplied symbols, false otherwise
     * @param array {@link Symbol} array
     * @param symbol1 {@link Symbol} first instance to be searched for
     * @param symbol2 {@link Symbol} second instance to be searched for
     * @return True if source array contains both supplied symbols, false otherwise
     */
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

    /**
     * Returns true if source array contains all three supplied symbols, false otherwise
     * @param array {@link Symbol} array
     * @param symbol1 {@link Symbol} first instance to be searched for
     * @param symbol2 {@link Symbol} second instance to be searched for
     * @param symbol3 {@link Symbol} third instance to be searched for
     * @return True if source array contains all three supplied symbols, false otherwise
     */
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

    /**
     * Returns new {@link Symbol} array containing supplied {@link Symbol} instances if they're present in the source array
     * @param source {@link Symbol} array
     * @param symbol1 {@link Symbol} first instance to filter for
     * @param symbol2 {@link Symbol} second instance to filter for
     * @param symbol3 {@link Symbol} third instance to filter for
     * @return New {@link Symbol} array containing supplied {@link Symbol} instances if they're present in the source array
     */
    default Symbol[] filterSymbols(final Symbol[] source,
                                   final Symbol symbol1,
                                   final Symbol symbol2,
                                   final Symbol symbol3)
    {
        final boolean hasSymbol1 = contains(source, symbol1);
        final boolean hasSymbol2 = contains(source, symbol2);
        final boolean hasSymbol3 = contains(source, symbol3);

        int size = 0;

        if (hasSymbol1)
        {
            size ++;
        }

        if (hasSymbol2)
        {
            size ++;
        }

        if (hasSymbol3)
        {
            size ++;
        }

        if (size == 0)
        {
            return EMPTY_SYMBOL_ARRAY;
        }

        final Symbol[] result = new Symbol[size];

        int index = 0;

        if (hasSymbol1)
        {
            result[index++] = symbol1;
        }

        if (hasSymbol2)
        {
            result[index++] = symbol2;
        }

        if (hasSymbol3)
        {
            result[index] = symbol3;
        }

        return result;
    }

    /**
     * Merges two {@link Symbol} arrays, removing nulls and duplicates, returns the resulting array
     * @param array1 First {@link Symbol} array
     * @param array2 Second {@link Symbol} array
     * @return New {@link Symbol} array
     */
    static Symbol[] mergeDistinct(final Symbol[] array1, final Symbol[] array2)
    {
        if ((array1 == null || array1.length == 0) && (array2 == null || array2.length == 0))
        {
            return EMPTY_SYMBOL_ARRAY;
        }

        final Set<Symbol> set = new LinkedHashSet<>();

        if (array1 != null)
        {
            for (final Symbol symbol : array1)
            {
                if (symbol != null)
                {
                    set.add(symbol);
                }
            }
        }

        if (array2 != null)
        {
            for (final Symbol symbol : array2)
            {
                if (symbol != null)
                {
                    set.add(symbol);
                }
            }
        }

        return set.isEmpty() ? EMPTY_SYMBOL_ARRAY : set.toArray(EMPTY_SYMBOL_ARRAY);
    }
}
