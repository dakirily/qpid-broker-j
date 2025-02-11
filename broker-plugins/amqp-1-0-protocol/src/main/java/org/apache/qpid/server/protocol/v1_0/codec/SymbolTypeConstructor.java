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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;

public class SymbolTypeConstructor extends VariableWidthTypeConstructor<Symbol>
{
    private static final Charset ASCII = StandardCharsets.US_ASCII;
    private static final ConcurrentMap<BinaryString, Symbol> SYMBOL_MAP = new ConcurrentHashMap<>(2048);

    public static SymbolTypeConstructor getInstance(int i)
    {
        return new SymbolTypeConstructor(i);
    }

    private SymbolTypeConstructor(int size)
    {
        super(size);
    }

    @Override
    public Symbol construct(final QpidByteBuffer in, final ValueHandler handler) throws AmqpErrorException
    {
        final int lengthFieldSize = getSize();

        if (!in.hasRemaining(lengthFieldSize))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct symbol: insufficient input data");
        }

        final int length = lengthFieldSize == 1 ? in.getUnsignedByte() : in.getInt();

        if (!in.hasRemaining(length))
        {
            throw new AmqpErrorException(AmqpError.DECODE_ERROR, "Cannot construct symbol: insufficient input data");
        }

        byte[] data = new byte[length];
        in.get(data);

        final BinaryString binaryKey = new BinaryString(data);

        return SYMBOL_MAP.computeIfAbsent(binaryKey, key -> Symbol.valueOf(key.asString(ASCII)));
    }
}
