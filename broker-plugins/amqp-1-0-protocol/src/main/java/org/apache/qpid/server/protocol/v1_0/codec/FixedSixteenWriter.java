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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public abstract class FixedSixteenWriter<T> implements ValueWriter<T>
{
    private final long _msb;
    private final long _lsb;

    public FixedSixteenWriter(final T object)
    {
        _msb = convertValueToMSB(object);
        _lsb = convertValueToLSB(object);
    }

    @Override
    public final void writeToBuffer(final QpidByteBuffer buffer)
    {
        buffer.put(getFormatCode());
        buffer.putLong(_msb);
        buffer.putLong(_lsb);
    }

    @Override
    public final int getEncodedSize()
    {
        return 17;
    }

    abstract byte getFormatCode();

    abstract long convertValueToMSB(final T value);

    abstract long convertValueToLSB(final T value);
}
