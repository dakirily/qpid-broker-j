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

import java.util.Objects;

public class DescribedType
{
    private final Object _descriptor;
    private final Object _described;

    public DescribedType(final Object descriptor, final Object described)
    {
        _descriptor = descriptor;
        _described = described;
    }

    public Object getDescriptor()
    {
        return _descriptor;
    }

    public Object getDescribed()
    {
        return _described;
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

        final DescribedType that = (DescribedType) o;

        if (!Objects.equals(_described, that._described))
        {
            return false;
        }

        return Objects.equals(_descriptor, that._descriptor);
    }

    @Override
    public int hashCode()
    {
        int result = _descriptor != null ? _descriptor.hashCode() : 0;
        result = 31 * result + (_described != null ? _described.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "DescribedType{"+ _descriptor +
               ", " + _described +
               '}';
    }
}
