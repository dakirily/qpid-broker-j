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
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public class TransferWriter extends AbstractDescribedTypeWriter<Transfer>
{
    private static final ValueWriter<UnsignedLong> DESCRIPTOR_WRITER = UnsignedLongWriter.getWriter((byte) 0x14);
    private static final Factory<Transfer> FACTORY = TransferWriter::new;

    private TransferWriter(final Registry registry, final Transfer object)
    {
        super(DESCRIPTOR_WRITER, new Writer(registry, object));
    }

    private static class Writer extends AbstractListWriter<Transfer>
    {
        private final Transfer _value;
        private final int _count;

        private int _field;

        public Writer(final Registry registry, final Transfer object)
        {
            super(registry);
            _value = object;
            _count = calculateCount();
        }

        private int calculateCount()
        {
            if (_value.getBatchable() != null)
            {
                return 11;
            }

            if (_value.getAborted() != null)
            {
                return 10;
            }

            if (_value.getResume() != null)
            {
                return 9;
            }

            if (_value.getState() != null)
            {
                return 8;
            }

            if (_value.getRcvSettleMode() != null)
            {
                return 7;
            }

            if (_value.getMore() != null)
            {
                return 6;
            }

            if (_value.getSettled() != null)
            {
                return 5;
            }

            if (_value.getMessageFormat() != null)
            {
                return 4;
            }

            if (_value.getDeliveryTag() != null)
            {
                return 3;
            }

            if (_value.getDeliveryId() != null)
            {
                return 2;
            }

            if (_value.getHandle() != null)
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
                case 0 -> _value.getHandle();
                case 1 -> _value.getDeliveryId();
                case 2 -> _value.getDeliveryTag();
                case 3 -> _value.getMessageFormat();
                case 4 -> _value.getSettled();
                case 5 -> _value.getMore();
                case 6 -> _value.getRcvSettleMode();
                case 7 -> _value.getState();
                case 8 -> _value.getResume();
                case 9 -> _value.getAborted();
                case 10 -> _value.getBatchable();
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
        registry.register(Transfer.class, FACTORY);
    }
}
