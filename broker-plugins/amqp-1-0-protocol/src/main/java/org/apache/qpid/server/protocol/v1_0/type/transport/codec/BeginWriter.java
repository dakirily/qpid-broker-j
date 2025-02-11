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

package org.apache.qpid.server.protocol.v1_0.type.transport.codec;

import org.apache.qpid.server.protocol.v1_0.codec.AbstractDescribedTypeWriter;
import org.apache.qpid.server.protocol.v1_0.codec.AbstractListWriter;
import org.apache.qpid.server.protocol.v1_0.codec.UnsignedLongWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;

public class BeginWriter extends AbstractDescribedTypeWriter<Begin>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x11);
    private static final Factory<Begin> FACTORY = BeginWriter::new;

    private BeginWriter(final Registry registry, final Begin object)
    {
         super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }

    private static class Writer extends AbstractListWriter<Begin>
    {
        private final Begin _value;
        private final int _count;

        private int _field;

        public Writer(final Registry registry, final Begin object)
        {
            super(registry);
            _value = object;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if ( _value.getProperties() != null)
            {
                return 8;
            }

            if ( _value.getDesiredCapabilities() != null)
            {
                return 7;
            }

            if ( _value.getOfferedCapabilities() != null)
            {
                return 6;
            }

            if ( _value.getHandleMax() != null)
            {
                return 5;
            }

            if ( _value.getOutgoingWindow() != null)
            {
                return 4;
            }

            if ( _value.getIncomingWindow() != null)
            {
                return 3;
            }

            if ( _value.getNextOutgoingId() != null)
            {
                return 2;
            }

            if ( _value.getRemoteChannel() != null)
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
                case 0 -> _value.getRemoteChannel();
                case 1 -> _value.getNextOutgoingId();
                case 2 -> _value.getIncomingWindow();
                case 3 -> _value.getOutgoingWindow();
                case 4 -> _value.getHandleMax();
                case 5 -> _value.getOfferedCapabilities();
                case 6 -> _value.getDesiredCapabilities();
                case 7 -> _value.getProperties();
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
        registry.register(Begin.class, FACTORY);
    }
}
