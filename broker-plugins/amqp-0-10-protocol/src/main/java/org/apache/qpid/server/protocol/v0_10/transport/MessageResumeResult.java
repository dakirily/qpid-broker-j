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


public final class MessageResumeResult extends Struct {

    public static final int TYPE = 1029;

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
    private long offset;


    public MessageResumeResult() {}


    public MessageResumeResult(long offset) {
        setOffset(offset);

    }




    public boolean hasOffset() {
        return (packing_flags & 256) != 0;
    }

    public MessageResumeResult clearOffset() {
        packing_flags &= ~256;
        this.offset = 0;
        setDirty(true);
        return this;
    }

    public long getOffset() {
        return offset;
    }

    public MessageResumeResult setOffset(long value) {
        this.offset = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public MessageResumeResult offset(long value) {
        return setOffset(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeUint64(this.offset);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.offset = dec.readUint64();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("offset", getOffset());
        }
        return result;
    }

    @Override
    public int getEncodedLength()
    {
        throw new UnsupportedOperationException();
    }
}
