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


public final class ConnectionClose extends Method {

    public static final int TYPE = 267;

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
    private ConnectionCloseCode replyCode;
    private String replyText;


    public ConnectionClose() {}


    public ConnectionClose(ConnectionCloseCode replyCode, String replyText, Option ... _options) {
        if(replyCode != null) {
            setReplyCode(replyCode);
        }
        if(replyText != null) {
            setReplyText(replyText);
        }

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
        delegate.connectionClose(context, this);
    }


    public boolean hasReplyCode() {
        return (packing_flags & 256) != 0;
    }

    public ConnectionClose clearReplyCode() {
        packing_flags &= ~256;
        this.replyCode = null;
        setDirty(true);
        return this;
    }

    public ConnectionCloseCode getReplyCode() {
        return replyCode;
    }

    public ConnectionClose setReplyCode(ConnectionCloseCode value) {
        this.replyCode = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public ConnectionClose replyCode(ConnectionCloseCode value) {
        return setReplyCode(value);
    }

    public boolean hasReplyText() {
        return (packing_flags & 512) != 0;
    }

    public ConnectionClose clearReplyText() {
        packing_flags &= ~512;
        this.replyText = null;
        setDirty(true);
        return this;
    }

    public String getReplyText() {
        return replyText;
    }

    public ConnectionClose setReplyText(String value) {
        this.replyText = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public ConnectionClose replyText(String value) {
        return setReplyText(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeUint16(this.replyCode.getValue());
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeStr8(this.replyText);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.replyCode = ConnectionCloseCode.get(dec.readUint16());
        }
        if ((packing_flags & 512) != 0)
        {
            this.replyText = dec.readStr8();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("replyCode", getReplyCode());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("replyText", getReplyText());
        }


        return result;
    }


}
