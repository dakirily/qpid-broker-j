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
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;

public class PropertiesWriter extends AbstractDescribedTypeWriter<Properties>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x73);
    private static final Factory<Properties> FACTORY = PropertiesWriter::new;

    public PropertiesWriter(final Registry registry, final Properties object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }

    private static class Writer extends AbstractListWriter<Properties>
    {
        private int _field;
        private final Properties _value;
        private final int _count;

        public Writer(final Registry registry, final Properties value)
        {
            super(registry);
            _value = value;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if ( _value.getReplyToGroupId() != null)
            {
                return 13;
            }

            if ( _value.getGroupSequence() != null)
            {
                return 12;
            }

            if ( _value.getGroupId() != null)
            {
                return 11;
            }

            if ( _value.getCreationTime() != null)
            {
                return 10;
            }

            if ( _value.getAbsoluteExpiryTime() != null)
            {
                return 9;
            }

            if ( _value.getContentEncoding() != null)
            {
                return 8;
            }

            if ( _value.getContentType() != null)
            {
                return 7;
            }

            if ( _value.getCorrelationId() != null)
            {
                return 6;
            }

            if ( _value.getReplyTo() != null)
            {
                return 5;
            }

            if ( _value.getSubject() != null)
            {
                return 4;
            }

            if ( _value.getTo() != null)
            {
                return 3;
            }

            if ( _value.getUserId() != null)
            {
                return 2;
            }

            if ( _value.getMessageId() != null)
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
                case 0 -> _value.getMessageId();
                case 1 -> _value.getUserId();
                case 2 -> _value.getTo();
                case 3 -> _value.getSubject();
                case 4 -> _value.getReplyTo();
                case 5 -> _value.getCorrelationId();
                case 6 -> _value.getContentType();
                case 7 -> _value.getContentEncoding();
                case 8 -> _value.getAbsoluteExpiryTime();
                case 9 -> _value.getCreationTime();
                case 10 -> _value.getGroupId();
                case 11 -> _value.getGroupSequence();
                case 12 -> _value.getReplyToGroupId();
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
        registry.register(Properties.class, FACTORY);
    }
}
