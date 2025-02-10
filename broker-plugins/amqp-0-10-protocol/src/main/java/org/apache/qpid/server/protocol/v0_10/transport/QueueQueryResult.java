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


public final class QueueQueryResult extends Struct {

    public static final int TYPE = 2049;

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
    private String queue;
    private String alternateExchange;
    private Map<String,Object> arguments;
    private long messageCount;
    private long subscriberCount;


    public QueueQueryResult() {}


    public QueueQueryResult(String queue, String alternateExchange, Map<String,Object> arguments, long messageCount, long subscriberCount, Option ... _options) {
        if(queue != null) {
            setQueue(queue);
        }
        if(alternateExchange != null) {
            setAlternateExchange(alternateExchange);
        }
        if(arguments != null) {
            setArguments(arguments);
        }
        setMessageCount(messageCount);
        setSubscriberCount(subscriberCount);

        for (final Option option : _options)
        {
            switch (option)
            {
                case DURABLE:
                    packing_flags |= 1024;
                    break;
                case EXCLUSIVE:
                    packing_flags |= 2048;
                    break;
                case AUTO_DELETE:
                    packing_flags |= 4096;
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException("invalid option: " + option);
            }
        }

    }




    public boolean hasQueue() {
        return (packing_flags & 256) != 0;
    }

    public QueueQueryResult clearQueue() {
        packing_flags &= ~256;
        this.queue = null;
        setDirty(true);
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public QueueQueryResult setQueue(String value) {
        this.queue = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public QueueQueryResult queue(String value) {
        return setQueue(value);
    }

    public boolean hasAlternateExchange() {
        return (packing_flags & 512) != 0;
    }

    public QueueQueryResult clearAlternateExchange() {
        packing_flags &= ~512;
        this.alternateExchange = null;
        setDirty(true);
        return this;
    }

    public String getAlternateExchange() {
        return alternateExchange;
    }

    public QueueQueryResult setAlternateExchange(String value) {
        this.alternateExchange = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public QueueQueryResult alternateExchange(String value) {
        return setAlternateExchange(value);
    }

    public boolean hasDurable() {
        return (packing_flags & 1024) != 0;
    }

    public QueueQueryResult clearDurable() {
        packing_flags &= ~1024;

        setDirty(true);
        return this;
    }

    public boolean getDurable() {
        return hasDurable();
    }

    public QueueQueryResult setDurable(boolean value) {

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

    public QueueQueryResult durable(boolean value) {
        return setDurable(value);
    }

    public boolean hasExclusive() {
        return (packing_flags & 2048) != 0;
    }

    public QueueQueryResult clearExclusive() {
        packing_flags &= ~2048;

        setDirty(true);
        return this;
    }

    public boolean getExclusive() {
        return hasExclusive();
    }

    public QueueQueryResult setExclusive(boolean value) {

        if (value)
        {
            packing_flags |= 2048;
        }
        else
        {
            packing_flags &= ~2048;
        }

        setDirty(true);
        return this;
    }

    public QueueQueryResult exclusive(boolean value) {
        return setExclusive(value);
    }

    public boolean hasAutoDelete() {
        return (packing_flags & 4096) != 0;
    }

    public QueueQueryResult clearAutoDelete() {
        packing_flags &= ~4096;

        setDirty(true);
        return this;
    }

    public boolean getAutoDelete() {
        return hasAutoDelete();
    }

    public QueueQueryResult setAutoDelete(boolean value) {

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

    public QueueQueryResult autoDelete(boolean value) {
        return setAutoDelete(value);
    }

    public boolean hasArguments() {
        return (packing_flags & 8192) != 0;
    }

    public QueueQueryResult clearArguments() {
        packing_flags &= ~8192;
        this.arguments = null;
        setDirty(true);
        return this;
    }

    public Map<String,Object> getArguments() {
        return arguments;
    }

    public QueueQueryResult setArguments(Map<String, Object> value) {
        this.arguments = value;
        packing_flags |= 8192;
        setDirty(true);
        return this;
    }

    public QueueQueryResult arguments(Map<String, Object> value) {
        return setArguments(value);
    }

    public boolean hasMessageCount() {
        return (packing_flags & 16384) != 0;
    }

    public QueueQueryResult clearMessageCount() {
        packing_flags &= ~16384;
        this.messageCount = 0;
        setDirty(true);
        return this;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public QueueQueryResult setMessageCount(long value) {
        this.messageCount = value;
        packing_flags |= 16384;
        setDirty(true);
        return this;
    }

    public QueueQueryResult messageCount(long value) {
        return setMessageCount(value);
    }

    public boolean hasSubscriberCount() {
        return (packing_flags & 32768) != 0;
    }

    public QueueQueryResult clearSubscriberCount() {
        packing_flags &= ~32768;
        this.subscriberCount = 0;
        setDirty(true);
        return this;
    }

    public long getSubscriberCount() {
        return subscriberCount;
    }

    public QueueQueryResult setSubscriberCount(long value) {
        this.subscriberCount = value;
        packing_flags |= 32768;
        setDirty(true);
        return this;
    }

    public QueueQueryResult subscriberCount(long value) {
        return setSubscriberCount(value);
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
            enc.writeStr8(this.alternateExchange);
        }
        if ((packing_flags & 8192) != 0)
        {
            enc.writeMap(this.arguments);
        }
        if ((packing_flags & 16384) != 0)
        {
            enc.writeUint32(this.messageCount);
        }
        if ((packing_flags & 32768) != 0)
        {
            enc.writeUint32(this.subscriberCount);
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
            this.alternateExchange = dec.readStr8();
        }
        if ((packing_flags & 8192) != 0)
        {
            this.arguments = dec.readMap();
        }
        if ((packing_flags & 16384) != 0)
        {
            this.messageCount = dec.readUint32();
        }
        if ((packing_flags & 32768) != 0)
        {
            this.subscriberCount = dec.readUint32();
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
            result.put("alternateExchange", getAlternateExchange());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("durable", getDurable());
        }
        if ((packing_flags & 2048) != 0)
        {
            result.put("exclusive", getExclusive());
        }
        if ((packing_flags & 4096) != 0)
        {
            result.put("autoDelete", getAutoDelete());
        }
        if ((packing_flags & 8192) != 0)
        {
            result.put("arguments", getArguments());
        }
        if ((packing_flags & 16384) != 0)
        {
            result.put("messageCount", getMessageCount());
        }
        if ((packing_flags & 32768) != 0)
        {
            result.put("subscriberCount", getSubscriberCount());
        }
        return result;
    }

    @Override
    public int getEncodedLength()
    {
        throw new UnsupportedOperationException();
    }
}
