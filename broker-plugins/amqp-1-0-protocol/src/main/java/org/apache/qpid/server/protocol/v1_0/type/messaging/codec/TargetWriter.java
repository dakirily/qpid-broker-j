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

import org.apache.qpid.server.protocol.v1_0.codec.AbstractDescribedTypeWriter;
import org.apache.qpid.server.protocol.v1_0.codec.AbstractListWriter;
import org.apache.qpid.server.protocol.v1_0.codec.UnsignedLongWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;

public class TargetWriter extends AbstractDescribedTypeWriter<Target>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x29);
    private static final Factory<Target> FACTORY = TargetWriter::new;

    public TargetWriter(final Registry registry, final Target object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }

    private static class Writer extends AbstractListWriter<Target>
    {
        private final Target _value;
        private final int _count;
        private int _field;

        public Writer(final Registry registry, final Target value)
        {
            super(registry);
            _value = value;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if ( _value.getCapabilities() != null)
            {
                return 7;
            }

            if ( _value.getDynamicNodeProperties() != null)
            {
                return 6;
            }

            if ( _value.getDynamic() != null)
            {
                return 5;
            }

            if ( _value.getTimeout() != null)
            {
                return 4;
            }

            if ( _value.getExpiryPolicy() != null)
            {
                return 3;
            }

            if ( _value.getDurable() != null)
            {
                return 2;
            }

            if ( _value.getAddress() != null)
            {
                return 1;
            }

            return 0;
        }

        @Override
        protected int getCount()
        {
            return _count;
        }

        @Override
        protected boolean hasNext()
        {
            return _field < _count;
        }

        @Override
        protected Object next()
        {
            return switch (_field++)
            {
                case 0 -> _value.getAddress();
                case 1 -> _value.getDurable();
                case 2 -> _value.getExpiryPolicy();
                case 3 -> _value.getTimeout();
                case 4 -> _value.getDynamic();
                case 5 -> _value.getDynamicNodeProperties();
                case 6 -> _value.getCapabilities();
                default -> null;
            };
        }

        @Override
        protected void reset()
        {
            _field = 0;
        }
    }

    public static void register(final ValueWriter.Registry registry)
    {
        registry.register(Target.class, FACTORY);
    }
}
