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

import java.util.Map;

import org.apache.qpid.server.protocol.v1_0.codec.AbstractDescribedTypeConstructor;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotations;

public class DeliveryAnnotationsConstructor extends AbstractDescribedTypeConstructor<DeliveryAnnotations>
{
    private static final Object[] DESCRIPTORS =
    {
            Symbols.AMQP_DELIVERY_ANNOTATIONS, UnsignedLong.valueOf(0x0000000000000071L),
    };

    private static final DeliveryAnnotationsConstructor INSTANCE = new DeliveryAnnotationsConstructor();

    public static void register(final DescribedTypeConstructorRegistry registry)
    {
        for (final Object descriptor : DESCRIPTORS)
        {
            registry.register(descriptor, INSTANCE);
        }
    }

    @Override
    public DeliveryAnnotations construct(final Object underlying) throws AmqpErrorException
    {
        if (underlying instanceof Map)
        {
            return new DeliveryAnnotations((Map) underlying);
        }
        else
        {
            throw AmqpErrorException.decode()
                    .message("Cannot decode 'delivery-annotations' from '%s'")
                    .args(underlying == null ? null : underlying.getClass().getSimpleName());
        }
    }
}
