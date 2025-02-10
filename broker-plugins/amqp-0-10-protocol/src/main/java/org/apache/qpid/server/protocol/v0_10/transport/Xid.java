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


public final class Xid extends Struct {

    public static final int TYPE = 1540;

    @Override
    public int getStructType() {
        return TYPE;
    }

    @Override
    public int getSizeWidth() {
        return 4;
    }

    @Override
    public int getPackWidth() {
        return 2;
    }

    public boolean hasPayload() {
        return false;
    }

    public byte getEncodedTrack() {
        return -1;
    }

    public boolean isConnectionControl()
    {
        return false;
    }

    private short packing_flags = 0;
    private long format;
    private byte[] globalId;
    private byte[] branchId;


    public Xid() {}


    public Xid(long format, byte[] globalId, byte[] branchId) {
        setFormat(format);
        if(globalId != null) {
            setGlobalId(globalId);
        }
        if(branchId != null) {
            setBranchId(branchId);
        }

    }




    public boolean hasFormat() {
        return (packing_flags & 256) != 0;
    }

    public Xid clearFormat() {
        packing_flags &= ~256;
        this.format = 0;
        setDirty(true);
        return this;
    }

    public long getFormat() {
        return format;
    }

    public Xid setFormat(long value) {
        this.format = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public Xid format(long value) {
        return setFormat(value);
    }

    public boolean hasGlobalId() {
        return (packing_flags & 512) != 0;
    }

    public Xid clearGlobalId() {
        packing_flags &= ~512;
        this.globalId = null;
        setDirty(true);
        return this;
    }

    public byte[] getGlobalId() {
        return globalId;
    }

    public Xid setGlobalId(byte[] value) {
        this.globalId = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public Xid globalId(byte[] value) {
        return setGlobalId(value);
    }

    public boolean hasBranchId() {
        return (packing_flags & 1024) != 0;
    }

    public Xid clearBranchId() {
        packing_flags &= ~1024;
        this.branchId = null;
        setDirty(true);
        return this;
    }

    public byte[] getBranchId() {
        return branchId;
    }

    public Xid setBranchId(byte[] value) {
        this.branchId = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public Xid branchId(byte[] value) {
        return setBranchId(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeUint32(this.format);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeVbin8(this.globalId);
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeVbin8(this.branchId);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.format = dec.readUint32();
        }
        if ((packing_flags & 512) != 0)
        {
            this.globalId = dec.readVbin8();
        }
        if ((packing_flags & 1024) != 0)
        {
            this.branchId = dec.readVbin8();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("format", getFormat());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("globalId", getGlobalId());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("branchId", getBranchId());
        }
        return result;
    }

    @Override
    public int getEncodedLength()
    {
        throw new UnsupportedOperationException();
    }
}
