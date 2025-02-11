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
package org.apache.qpid.server.protocol.v1_0.codec;

import java.lang.reflect.Array;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;

public abstract class ArrayTypeConstructor implements TypeConstructor<Object[]>
{
    @Override
    public Object[] construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
    {
        final int size = read(in);
        final long initialRemaining = in.remaining();

        if (initialRemaining < size)
        {
            throw AmqpErrorException.decode()
                    .message("Insufficient data to decode array - requires %d octets, only %d remaining.")
                    .args(size, initialRemaining);
        }

        // read the number of elements
        final int count = read(in);

        final TypeConstructor typeConstructor = handler.readConstructor(in);

        // if there are no elements, return null
        if (count == 0)
        {
            return new Object[0];
        }

        // read first element
        final Object firstElement = typeConstructor.construct(in, handler);

        // allocate result array using the runtime type of the first element
        final Object[] result = (Object[]) Array.newInstance(firstElement.getClass(), count);
        result[0] = firstElement;

        for (int i = 1; i < count; i++)
        {
            result[i] = typeConstructor.construct(in, handler);
        }

        final long expectedRemaining = initialRemaining - size;
        final long unconsumedBytes = in.remaining() - expectedRemaining;
        if (unconsumedBytes > 0)
        {
            throw AmqpErrorException.decode()
                    .message("Array incorrectly encoded, %d bytes remaining after decoding %d elements")
                    .args(unconsumedBytes, count);
        }
        else if (unconsumedBytes < 0)
        {
            throw AmqpErrorException.decode()
                    .message("Array incorrectly encoded, %d bytes beyond provided size consumed after decoding %d elements")
                    .args(-unconsumedBytes, count);
        }

        return result;
    }

    abstract int read(QpidByteBuffer in) throws AmqpErrorException;

    private static final ArrayTypeConstructor ONE_BYTE_SIZE_ARRAY = new ArrayTypeConstructor()
    {
        @Override
        int read(final QpidByteBuffer in) throws AmqpErrorException
        {
            if (!in.hasRemaining())
            {
                throw AmqpErrorException.decode().message("Insufficient data to decode array").build();
            }
            return ((int) in.get()) & 0xff;
        }
    };

    private static final ArrayTypeConstructor FOUR_BYTE_SIZE_ARRAY = new ArrayTypeConstructor()
    {
        @Override
        int read(final QpidByteBuffer in) throws AmqpErrorException
        {
            if (!in.hasRemaining(4))
            {
                throw AmqpErrorException.decode().message("Insufficient data to decode array").build();
            }
            return in.getInt();
        }
    };

    public static ArrayTypeConstructor getOneByteSizeTypeConstructor()
    {
        return ONE_BYTE_SIZE_ARRAY;
    }

    public static ArrayTypeConstructor getFourByteSizeTypeConstructor()
    {
        return FOUR_BYTE_SIZE_ARRAY;
    }
}
