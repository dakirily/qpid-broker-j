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


public final class SessionCommandPoint extends Method {

    public static final int TYPE = 519;

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
        return Frame.L3;
    }

    @Override
    public boolean isConnectionControl()
    {
        return false;
    }

    private short packing_flags = 0;
    private int commandId;
    private long commandOffset;


    public SessionCommandPoint() {}


    public SessionCommandPoint(int commandId, long commandOffset, Option ... _options) {
        setCommandId(commandId);
        setCommandOffset(commandOffset);

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
        delegate.sessionCommandPoint(context, this);
    }


    public boolean hasCommandId() {
        return (packing_flags & 256) != 0;
    }

    public SessionCommandPoint clearCommandId() {
        packing_flags &= ~256;
        this.commandId = 0;
        setDirty(true);
        return this;
    }

    public int getCommandId() {
        return commandId;
    }

    public SessionCommandPoint setCommandId(int value) {
        this.commandId = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public SessionCommandPoint commandId(int value) {
        return setCommandId(value);
    }

    public boolean hasCommandOffset() {
        return (packing_flags & 512) != 0;
    }

    public SessionCommandPoint clearCommandOffset() {
        packing_flags &= ~512;
        this.commandOffset = 0;
        setDirty(true);
        return this;
    }

    public long getCommandOffset() {
        return commandOffset;
    }

    public SessionCommandPoint setCommandOffset(long value) {
        this.commandOffset = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public SessionCommandPoint commandOffset(long value) {
        return setCommandOffset(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeSequenceNo(this.commandId);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeUint64(this.commandOffset);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.commandId = dec.readSequenceNo();
        }
        if ((packing_flags & 512) != 0)
        {
            this.commandOffset = dec.readUint64();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("commandId", getCommandId());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("commandOffset", getCommandOffset());
        }
        return result;
    }
}
