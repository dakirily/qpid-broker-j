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

package org.apache.qpid.server.protocol.v1_0.type;

import java.io.Serial;

public final class UnsignedShort extends Number implements Comparable<UnsignedShort>
{
    @Serial
    private static final long serialVersionUID = 1L;
    private static final UnsignedShort[] cachedValues = new UnsignedShort[256];
    public static final UnsignedShort MAX_VALUE = new UnsignedShort((short) 0xffff);

    private final short _underlying;

    static
    {
        for (short i = 0; i < 256; i++)
        {
            cachedValues[i] = new UnsignedShort(i);
        }
    }

    public static final UnsignedShort ZERO = UnsignedShort.valueOf((short) 0);

    private UnsignedShort(final short underlying)
    {
        _underlying = underlying;
    }

    @Override
    public short shortValue()
    {
        return _underlying;
    }

    @Override
    public int intValue()
    {
        return _underlying & 0xFFFF;
    }

    @Override
    public long longValue()
    {
        return ((long) _underlying) & 0xFFFFL;
    }

    @Override
    public float floatValue()
    {
        return (float) intValue();
    }

    @Override
    public double doubleValue()
    {
        return intValue();
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final UnsignedShort that = (UnsignedShort) o;
        return _underlying == that._underlying;
    }

    @Override
    public int compareTo(final UnsignedShort o)
    {
        return Integer.signum(intValue() - o.intValue());
    }

    @Override
    public int hashCode()
    {
        return _underlying;
    }

    @Override
    public String toString()
    {
        return String.valueOf(longValue());
    }

    public static UnsignedShort valueOf(final short underlying)
    {
        if ((underlying & 0xFF00) == 0)
        {
            return cachedValues[underlying];
        }
        else
        {
            return new UnsignedShort(underlying);
        }
    }

    public static UnsignedShort valueOf(final int intValue)
    {
        if (intValue < 0 || intValue >= (1 << 16))
        {
            final String msg = "Value \"%d\" lies outside the range [%d-%d).".formatted(intValue, 0, (1 << 16));
            throw new NumberFormatException(msg);
        }
        return valueOf((short) intValue);
    }

    public static UnsignedShort valueOf(final String value)
    {
        int intVal = Integer.parseInt(value);
        return valueOf(intVal);
    }
}
