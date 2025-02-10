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

package org.apache.qpid.server.protocol.v0_10.transport;


import java.util.LinkedHashMap;
import java.util.Map;


public final class MessageRelease extends Method {

    public static final int TYPE = 1028;

    @Override
    public int getStructType() {
        return TYPE;
    }

    @Override
    public int getSizeWidth() {
        return 0;
    }

    @Override
    public int getPackWidth() {
        return 2;
    }

    @Override
    public boolean hasPayload() {
        return false;
    }

    @Override
    public byte getEncodedTrack() {
        return Frame.L4;
    }

    @Override
    public boolean isConnectionControl()
    {
        return false;
    }

    private short packing_flags = 0;
    private RangeSet transfers;


    public MessageRelease() {}


    public MessageRelease(RangeSet transfers, Option ... _options) {
        if(transfers != null) {
            setTransfers(transfers);
        }

        for (final Option option : _options)
        {
            switch (option)
            {
                case SET_REDELIVERED:
                    packing_flags |= 512;
                    break;
                case SYNC:
                    this.setSync(true);
                    break;
                case BATCH:
                    this.setBatch(true);
                    break;
                case UNRELIABLE:
                    this.setUnreliable(true);
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException("invalid option: " + option);
            }
        }

    }

    @Override
    public <C> void dispatch(C context, MethodDelegate<C> delegate) {
        delegate.messageRelease(context, this);
    }


    public boolean hasTransfers() {
        return (packing_flags & 256) != 0;
    }

    public MessageRelease clearTransfers() {
        packing_flags &= ~256;
        this.transfers = null;
        setDirty(true);
        return this;
    }

    public RangeSet getTransfers() {
        return transfers;
    }

    public MessageRelease setTransfers(RangeSet value) {
        this.transfers = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public MessageRelease transfers(RangeSet value) {
        return setTransfers(value);
    }

    public boolean hasSetRedelivered() {
        return (packing_flags & 512) != 0;
    }

    public MessageRelease clearSetRedelivered() {
        packing_flags &= ~512;

        setDirty(true);
        return this;
    }

    public boolean getSetRedelivered() {
        return hasSetRedelivered();
    }

    public MessageRelease setSetRedelivered(boolean value) {

        if (value)
        {
            packing_flags |= 512;
        }
        else
        {
            packing_flags &= ~512;
        }

        setDirty(true);
        return this;
    }

    public MessageRelease setRedelivered(boolean value) {
        return setSetRedelivered(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeSequenceSet(this.transfers);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.transfers = dec.readSequenceSet();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("transfers", getTransfers());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("setRedelivered", getSetRedelivered());
        }
        return result;
    }
}
