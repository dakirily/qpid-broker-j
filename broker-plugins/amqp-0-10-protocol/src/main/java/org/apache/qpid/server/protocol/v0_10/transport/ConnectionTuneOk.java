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


public final class ConnectionTuneOk extends Method {

    public static final int TYPE = 262;

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
        return Frame.L1;
    }

    @Override
    public boolean isConnectionControl()
    {
        return true;
    }

    private short packing_flags = 0;
    private int channelMax;
    private int maxFrameSize;
    private int heartbeat;


    public ConnectionTuneOk() {}


    public ConnectionTuneOk(int channelMax, int maxFrameSize, int heartbeat, Option ... _options) {
        setChannelMax(channelMax);
        setMaxFrameSize(maxFrameSize);
        setHeartbeat(heartbeat);

        for (final Option option : _options)
        {
            switch (option)
            {
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
        delegate.connectionTuneOk(context, this);
    }


    public boolean hasChannelMax() {
        return (packing_flags & 256) != 0;
    }

    public ConnectionTuneOk clearChannelMax() {
        packing_flags &= ~256;
        this.channelMax = 0;
        setDirty(true);
        return this;
    }

    public int getChannelMax() {
        return channelMax;
    }

    public ConnectionTuneOk setChannelMax(int value) {
        this.channelMax = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public ConnectionTuneOk channelMax(int value) {
        return setChannelMax(value);
    }

    public boolean hasMaxFrameSize() {
        return (packing_flags & 512) != 0;
    }

    public ConnectionTuneOk clearMaxFrameSize() {
        packing_flags &= ~512;
        this.maxFrameSize = 0;
        setDirty(true);
        return this;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public ConnectionTuneOk setMaxFrameSize(int value) {
        this.maxFrameSize = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public ConnectionTuneOk maxFrameSize(int value) {
        return setMaxFrameSize(value);
    }

    public boolean hasHeartbeat() {
        return (packing_flags & 1024) != 0;
    }

    public ConnectionTuneOk clearHeartbeat() {
        packing_flags &= ~1024;
        this.heartbeat = 0;
        setDirty(true);
        return this;
    }

    public int getHeartbeat() {
        return heartbeat;
    }

    public ConnectionTuneOk setHeartbeat(int value) {
        this.heartbeat = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public ConnectionTuneOk heartbeat(int value) {
        return setHeartbeat(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeUint16(this.channelMax);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeUint16(this.maxFrameSize);
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeUint16(this.heartbeat);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.channelMax = dec.readUint16();
        }
        if ((packing_flags & 512) != 0)
        {
            this.maxFrameSize = dec.readUint16();
        }
        if ((packing_flags & 1024) != 0)
        {
            this.heartbeat = dec.readUint16();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("channelMax", getChannelMax());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("maxFrameSize", getMaxFrameSize());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("heartbeat", getHeartbeat());
        }


        return result;
    }


}
