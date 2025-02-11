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

import java.nio.charset.Charset;

final class BinaryString
{
    private final byte[] _data;
    private final int _offset;
    private final int _size;
    private final int _hashCode;

    BinaryString(final byte[] data)
    {
        this(data, 0, data.length);
    }

    BinaryString(final byte[] data, final int offset, final int size)
    {
        _data = data;
        _offset = offset;
        _size = size;
        _hashCode = computeHashCode(data, offset, size);
    }

    private static int computeHashCode(final byte[] data, final int offset, final int size)
    {
        int hc = 0;
        for (int i = offset, end = offset + size; i < end; i++)
        {
            hc = ((hc << 5) - hc) + (0xFF & data[i]);
        }
        return hc;
    }

    public String asString(final Charset charset)
    {
        return new String(_data, _offset, _size, charset);
    }

    @Override
    public int hashCode()
    {
        return _hashCode;
    }

    @Override
    public boolean equals(final Object object)
    {
        // identity check
        if (this == object)
        {
            return true;
        }

        if (!(object instanceof final BinaryString other))
        {
            return false;
        }

        // check size and hashcode
        if (this._size != other._size || this._hashCode != other._hashCode)
        {
            return false;
        }

        // compare byte by byte
        for (int i = 0; i < _size; i++)
        {
            if (this._data[this._offset + i] != other._data[other._offset + i])
            {
                return false;
            }
        }

        return true;
    }
}
