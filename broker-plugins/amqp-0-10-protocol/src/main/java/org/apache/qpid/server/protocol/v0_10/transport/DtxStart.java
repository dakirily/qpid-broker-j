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


public final class DtxStart extends Method {

    public static final int TYPE = 1538;

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
    private Xid xid;


    public DtxStart() {}


    public DtxStart(Xid xid, Option ... _options) {
        if(xid != null) {
            setXid(xid);
        }

        for (final Option option : _options)
        {
            switch (option)
            {
                case JOIN:
                    packing_flags |= 512;
                    break;
                case RESUME:
                    packing_flags |= 1024;
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
        delegate.dtxStart(context, this);
    }


    public boolean hasXid() {
        return (packing_flags & 256) != 0;
    }

    public DtxStart clearXid() {
        packing_flags &= ~256;
        this.xid = null;
        setDirty(true);
        return this;
    }

    public Xid getXid() {
        return xid;
    }

    public DtxStart setXid(Xid value) {
        this.xid = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public DtxStart xid(Xid value) {
        return setXid(value);
    }

    public boolean hasJoin() {
        return (packing_flags & 512) != 0;
    }

    public DtxStart clearJoin() {
        packing_flags &= ~512;

        setDirty(true);
        return this;
    }

    public boolean getJoin() {
        return hasJoin();
    }

    public DtxStart setJoin(boolean value) {

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

    public DtxStart join(boolean value) {
        return setJoin(value);
    }

    public boolean hasResume() {
        return (packing_flags & 1024) != 0;
    }

    public DtxStart clearResume() {
        packing_flags &= ~1024;

        setDirty(true);
        return this;
    }

    public boolean getResume() {
        return hasResume();
    }

    public DtxStart setResume(boolean value) {

        if (value)
        {
            packing_flags |= 1024;
        }
        else
        {
            packing_flags &= ~1024;
        }

        setDirty(true);
        return this;
    }

    public DtxStart resume(boolean value) {
        return setResume(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeStruct(Xid.TYPE, this.xid);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.xid = (Xid)dec.readStruct(Xid.TYPE);
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("xid", getXid());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("join", getJoin());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("resume", getResume());
        }


        return result;
    }


}
