/*
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


public final class QueueDelete extends Method {

    public static final int TYPE = 2050;

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


    public QueueDelete() {}


    public QueueDelete(String queue, Option ... _options) {
        if(queue != null) {
            setQueue(queue);
        }

        for (final Option option : _options)
        {
            switch (option)
            {
                case IF_UNUSED:
                    packing_flags |= 512;
                    break;
                case IF_EMPTY:
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
        delegate.queueDelete(context, this);
    }


    public boolean hasQueue() {
        return (packing_flags & 256) != 0;
    }

    public QueueDelete clearQueue() {
        packing_flags &= ~256;
        this.queue = null;
        setDirty(true);
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public QueueDelete setQueue(String value) {
        this.queue = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public QueueDelete queue(String value) {
        return setQueue(value);
    }

    public boolean hasIfUnused() {
        return (packing_flags & 512) != 0;
    }

    public QueueDelete clearIfUnused() {
        packing_flags &= ~512;

        setDirty(true);
        return this;
    }

    public boolean getIfUnused() {
        return hasIfUnused();
    }

    public QueueDelete setIfUnused(boolean value) {

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

    public QueueDelete ifUnused(boolean value) {
        return setIfUnused(value);
    }

    public boolean hasIfEmpty() {
        return (packing_flags & 1024) != 0;
    }

    public QueueDelete clearIfEmpty() {
        packing_flags &= ~1024;

        setDirty(true);
        return this;
    }

    public boolean getIfEmpty() {
        return hasIfEmpty();
    }

    public QueueDelete setIfEmpty(boolean value) {

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

    public QueueDelete ifEmpty(boolean value) {
        return setIfEmpty(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeStr8(this.queue);
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
            result.put("ifUnused", getIfUnused());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("ifEmpty", getIfEmpty());
        }
        return result;
    }
}
