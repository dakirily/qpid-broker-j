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


public final class MessageSubscribe extends Method {

    public static final int TYPE = 1031;

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
    private String queue;
    private String destination;
    private MessageAcceptMode acceptMode;
    private MessageAcquireMode acquireMode;
    private String resumeId;
    private long resumeTtl;
    private Map<String,Object> arguments;


    public MessageSubscribe() {}


    public MessageSubscribe(String queue, String destination, MessageAcceptMode acceptMode, MessageAcquireMode acquireMode, String resumeId, long resumeTtl, Map<String,Object> arguments, Option ... _options) {
        if(queue != null) {
            setQueue(queue);
        }
        if(destination != null) {
            setDestination(destination);
        }
        if(acceptMode != null) {
            setAcceptMode(acceptMode);
        }
        if(acquireMode != null) {
            setAcquireMode(acquireMode);
        }
        if(resumeId != null) {
            setResumeId(resumeId);
        }
        setResumeTtl(resumeTtl);
        if(arguments != null) {
            setArguments(arguments);
        }

        for (final Option option : _options)
        {
            switch (option)
            {
                case EXCLUSIVE:
                    packing_flags |= 4096;
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
        delegate.messageSubscribe(context, this);
    }


    public boolean hasQueue() {
        return (packing_flags & 256) != 0;
    }

    public MessageSubscribe clearQueue() {
        packing_flags &= ~256;
        this.queue = null;
        setDirty(true);
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public MessageSubscribe setQueue(String value) {
        this.queue = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public MessageSubscribe queue(String value) {
        return setQueue(value);
    }

    public boolean hasDestination() {
        return (packing_flags & 512) != 0;
    }

    public MessageSubscribe clearDestination() {
        packing_flags &= ~512;
        this.destination = null;
        setDirty(true);
        return this;
    }

    public String getDestination() {
        return destination;
    }

    public MessageSubscribe setDestination(String value) {
        this.destination = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public MessageSubscribe destination(String value) {
        return setDestination(value);
    }

    public boolean hasAcceptMode() {
        return (packing_flags & 1024) != 0;
    }

    public MessageSubscribe clearAcceptMode() {
        packing_flags &= ~1024;
        this.acceptMode = null;
        setDirty(true);
        return this;
    }

    public MessageAcceptMode getAcceptMode() {
        return acceptMode;
    }

    public MessageSubscribe setAcceptMode(MessageAcceptMode value) {
        this.acceptMode = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public MessageSubscribe acceptMode(MessageAcceptMode value) {
        return setAcceptMode(value);
    }

    public boolean hasAcquireMode() {
        return (packing_flags & 2048) != 0;
    }

    public MessageSubscribe clearAcquireMode() {
        packing_flags &= ~2048;
        this.acquireMode = null;
        setDirty(true);
        return this;
    }

    public MessageAcquireMode getAcquireMode() {
        return acquireMode;
    }

    public MessageSubscribe setAcquireMode(MessageAcquireMode value) {
        this.acquireMode = value;
        packing_flags |= 2048;
        setDirty(true);
        return this;
    }

    public MessageSubscribe acquireMode(MessageAcquireMode value) {
        return setAcquireMode(value);
    }

    public boolean hasExclusive() {
        return (packing_flags & 4096) != 0;
    }

    public MessageSubscribe clearExclusive() {
        packing_flags &= ~4096;

        setDirty(true);
        return this;
    }

    public boolean getExclusive() {
        return hasExclusive();
    }

    public MessageSubscribe setExclusive(boolean value) {

        if (value)
        {
            packing_flags |= 4096;
        }
        else
        {
            packing_flags &= ~4096;
        }

        setDirty(true);
        return this;
    }

    public MessageSubscribe exclusive(boolean value) {
        return setExclusive(value);
    }

    public boolean hasResumeId() {
        return (packing_flags & 8192) != 0;
    }

    public MessageSubscribe clearResumeId() {
        packing_flags &= ~8192;
        this.resumeId = null;
        setDirty(true);
        return this;
    }

    public String getResumeId() {
        return resumeId;
    }

    public MessageSubscribe setResumeId(String value) {
        this.resumeId = value;
        packing_flags |= 8192;
        setDirty(true);
        return this;
    }

    public MessageSubscribe resumeId(String value) {
        return setResumeId(value);
    }

    public boolean hasResumeTtl() {
        return (packing_flags & 16384) != 0;
    }

    public MessageSubscribe clearResumeTtl() {
        packing_flags &= ~16384;
        this.resumeTtl = 0;
        setDirty(true);
        return this;
    }

    public long getResumeTtl() {
        return resumeTtl;
    }

    public MessageSubscribe setResumeTtl(long value) {
        this.resumeTtl = value;
        packing_flags |= 16384;
        setDirty(true);
        return this;
    }

    public MessageSubscribe resumeTtl(long value) {
        return setResumeTtl(value);
    }

    public boolean hasArguments() {
        return (packing_flags & 32768) != 0;
    }

    public MessageSubscribe clearArguments() {
        packing_flags &= ~32768;
        this.arguments = null;
        setDirty(true);
        return this;
    }

    public Map<String,Object> getArguments() {
        return arguments;
    }

    public MessageSubscribe setArguments(Map<String, Object> value) {
        this.arguments = value;
        packing_flags |= 32768;
        setDirty(true);
        return this;
    }

    public MessageSubscribe arguments(Map<String, Object> value) {
        return setArguments(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeStr8(this.queue);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeStr8(this.destination);
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeUint8(this.acceptMode.getValue());
        }
        if ((packing_flags & 2048) != 0)
        {
            enc.writeUint8(this.acquireMode.getValue());
        }
        if ((packing_flags & 8192) != 0)
        {
            enc.writeStr16(this.resumeId);
        }
        if ((packing_flags & 16384) != 0)
        {
            enc.writeUint64(this.resumeTtl);
        }
        if ((packing_flags & 32768) != 0)
        {
            enc.writeMap(this.arguments);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.queue = dec.readStr8();
        }
        if ((packing_flags & 512) != 0)
        {
            this.destination = dec.readStr8();
        }
        if ((packing_flags & 1024) != 0)
        {
            this.acceptMode = MessageAcceptMode.get(dec.readUint8());
        }
        if ((packing_flags & 2048) != 0)
        {
            this.acquireMode = MessageAcquireMode.get(dec.readUint8());
        }
        if ((packing_flags & 8192) != 0)
        {
            this.resumeId = dec.readStr16();
        }
        if ((packing_flags & 16384) != 0)
        {
            this.resumeTtl = dec.readUint64();
        }
        if ((packing_flags & 32768) != 0)
        {
            this.arguments = dec.readMap();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("queue", getQueue());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("destination", getDestination());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("acceptMode", getAcceptMode());
        }
        if ((packing_flags & 2048) != 0)
        {
            result.put("acquireMode", getAcquireMode());
        }
        if ((packing_flags & 4096) != 0)
        {
            result.put("exclusive", getExclusive());
        }
        if ((packing_flags & 8192) != 0)
        {
            result.put("resumeId", getResumeId());
        }
        if ((packing_flags & 16384) != 0)
        {
            result.put("resumeTtl", getResumeTtl());
        }
        if ((packing_flags & 32768) != 0)
        {
            result.put("arguments", getArguments());
        }
        return result;
    }
}
