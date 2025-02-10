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


public final class XaResult extends Struct {

    public static final int TYPE = 1537;

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
    private DtxXaStatus status;


    public XaResult() {}


    public XaResult(DtxXaStatus status) {
        if(status != null) {
            setStatus(status);
        }

    }




    public boolean hasStatus() {
        return (packing_flags & 256) != 0;
    }

    public XaResult clearStatus() {
        packing_flags &= ~256;
        this.status = null;
        setDirty(true);
        return this;
    }

    public DtxXaStatus getStatus() {
        return status;
    }

    public XaResult setStatus(DtxXaStatus value) {
        this.status = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public XaResult status(DtxXaStatus value) {
        return setStatus(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeUint16(this.status.getValue());
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.status = DtxXaStatus.get(dec.readUint16());
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();
        if ((packing_flags & 256) != 0)
        {
            result.put("status", getStatus());
        }
        return result;
    }

    @Override
    public int getEncodedLength()
    {
        throw new UnsupportedOperationException();
    }
}