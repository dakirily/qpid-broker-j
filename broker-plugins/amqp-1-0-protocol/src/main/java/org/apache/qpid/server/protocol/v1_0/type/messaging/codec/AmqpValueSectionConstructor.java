
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
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.TypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;

public class AmqpValueSectionConstructor implements DescribedTypeConstructor<AmqpValueSection>
{
    private static final Object[] DESCRIPTORS =
    {
            Symbols.AMQP_VALUE, UnsignedLong.valueOf(0x0000000000000077L),
    };

    private static final AmqpValueSectionConstructor INSTANCE = new AmqpValueSectionConstructor();

    public static void register(final DescribedTypeConstructorRegistry registry)
    {
        for (final Object descriptor : DESCRIPTORS)
        {
            registry.register(descriptor, INSTANCE);
        }
    }

    @Override
    public TypeConstructor<AmqpValueSection> construct(final Object descriptor,
                                                       final QpidByteBuffer in,
                                                       final int originalPosition,
                                                       final ValueHandler valueHandler)
            throws AmqpErrorException
    {
        return new LazyConstructor(originalPosition);
    }

    private static class LazyConstructor extends AbstractLazyConstructor<AmqpValueSection>
    {
        LazyConstructor(final int originalPosition)
        {
            super(originalPosition);
        }

        @Override
        protected AmqpValueSection createObject(final QpidByteBuffer encoding, final ValueHandler handler)
        {
            return new AmqpValueSection(encoding);
        }

        @Override
        protected void skipValue(final QpidByteBuffer in) throws AmqpErrorException
        {
            if (!in.hasRemaining())
            {
                throw AmqpErrorException.decode().message("Insufficient data to decode AMQP value section.").build();
            }

            byte formatCode = in.get();

            if (formatCode == ValueHandler.DESCRIBED_TYPE)
            {
                // This is only valid if the described value is not an array
                skipValue(in);
                skipValue(in);
            }
            else
            {
                final int skipLength;
                int category = (formatCode >> 4) & 0x0F;
                skipLength = switch (category)
                {
                    case 0x04 -> 0;
                    case 0x05 -> 1;
                    case 0x06 -> 2;
                    case 0x07 -> 4;
                    case 0x08 -> 8;
                    case 0x09 -> 16;
                    case 0x0a, 0x0c, 0x0e ->
                    {
                        if (!in.hasRemaining())
                        {
                            throw AmqpErrorException.decode().message("Insufficient data to decode AMQP value section.").build();
                        }
                        yield in.getUnsignedByte();
                    }
                    case 0x0b, 0x0d, 0x0f ->
                    {
                        if (!in.hasRemaining(4))
                        {
                            throw AmqpErrorException.decode().message("Insufficient data to decode AMQP value section.").build();
                        }
                        yield in.getInt();
                    }
                    default -> throw AmqpErrorException.decode().message("Unknown type").build();
                };
                if (!in.hasRemaining(skipLength))
                {
                    throw AmqpErrorException.decode().message("Insufficient data to decode AMQP value section.").build();
                }
                in.position(in.position() + skipLength);
            }
        }
    }
}
