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

package org.apache.qpid.server.protocol.v1_0.type.messaging.codec;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.TypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AbstractSection;

public abstract class DescribedListSectionConstructor<S extends AbstractSection> implements DescribedTypeConstructor<S>
{
    @Override
    public TypeConstructor<S> construct(final Object descriptor,
                                        final QpidByteBuffer in,
                                        final int originalPosition,
                                        final ValueHandler valueHandler)
            throws AmqpErrorException
    {
        if (!in.hasRemaining())
        {
            throw AmqpErrorException.decode().message("Insufficient data to decode section.").build();
        }
        int constructorByte = in.getUnsignedByte();
        int sizeBytes = switch (constructorByte)
        {
            case 0x45 -> 0;
            case 0xc0 -> 1;
            case 0xd0 -> 4;
            default -> throw AmqpErrorException.decode().message("The described section must always be a list").build();
        };

        return new LazyConstructor(sizeBytes, originalPosition);
    }


    private class LazyConstructor extends AbstractLazyConstructor<S>
    {
        private final int _sizeBytes;

        LazyConstructor(final int sizeBytes, final int originalPosition)
        {
            super(originalPosition);
            _sizeBytes = sizeBytes;
        }

        @Override
        protected S createObject(final QpidByteBuffer encoding, final ValueHandler handler)
        {
            return DescribedListSectionConstructor.this.createObject(encoding);
        }

        @Override
        protected void skipValue(final QpidByteBuffer in) throws AmqpErrorException
        {
            if (!in.hasRemaining(_sizeBytes))
            {
                throw AmqpErrorException.decode().message("Insufficient data to decode section.").build();
            }
            int size = switch (_sizeBytes)
            {
                case 0 -> 0;
                case 1 -> in.getUnsignedByte();
                case 4 -> in.getInt();
                default -> throw AmqpErrorException.decode().message("Unexpected constructor type, can only be 0,1 or 4").build();
            };
            if (!in.hasRemaining(size))
            {
                throw AmqpErrorException.decode().message("Insufficient data to decode section.").build();
            }
            in.position(in.position() + size);
        }
    }

    protected abstract S createObject(final QpidByteBuffer encodedForm);
}
