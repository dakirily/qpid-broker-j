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


public final class MessageReject extends Method {

    public static final int TYPE = 1027;

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
    private MessageRejectCode code;
    private String text;


    public MessageReject() {}


    public MessageReject(RangeSet transfers, MessageRejectCode code, String text, Option ... _options) {
        if(transfers != null) {
            setTransfers(transfers);
        }
        if(code != null) {
            setCode(code);
        }
        if(text != null) {
            setText(text);
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
        delegate.messageReject(context, this);
    }


    public boolean hasTransfers() {
        return (packing_flags & 256) != 0;
    }

    public MessageReject clearTransfers() {
        packing_flags &= ~256;
        this.transfers = null;
        setDirty(true);
        return this;
    }

    public RangeSet getTransfers() {
        return transfers;
    }

    public MessageReject setTransfers(RangeSet value) {
        this.transfers = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public MessageReject transfers(RangeSet value) {
        return setTransfers(value);
    }

    public boolean hasCode() {
        return (packing_flags & 512) != 0;
    }

    public MessageReject clearCode() {
        packing_flags &= ~512;
        this.code = null;
        setDirty(true);
        return this;
    }

    public MessageRejectCode getCode() {
        return code;
    }

    public MessageReject setCode(MessageRejectCode value) {
        this.code = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public MessageReject code(MessageRejectCode value) {
        return setCode(value);
    }

    public boolean hasText() {
        return (packing_flags & 1024) != 0;
    }

    public MessageReject clearText() {
        packing_flags &= ~1024;
        this.text = null;
        setDirty(true);
        return this;
    }

    public String getText() {
        return text;
    }

    public MessageReject setText(String value) {
        this.text = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public MessageReject text(String value) {
        return setText(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeSequenceSet(this.transfers);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeUint16(this.code.getValue());
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeStr8(this.text);
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
        if ((packing_flags & 512) != 0)
        {
            this.code = MessageRejectCode.get(dec.readUint16());
        }
        if ((packing_flags & 1024) != 0)
        {
            this.text = dec.readStr8();
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
            result.put("code", getCode());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("text", getText());
        }
        return result;
    }
}
