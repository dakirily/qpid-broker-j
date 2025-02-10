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


public final class DeliveryProperties extends Struct {

    public static final int TYPE = 1025;

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
    private MessageDeliveryPriority priority;
    private MessageDeliveryMode deliveryMode;
    private long ttl;
    private long timestamp;
    private long expiration;
    private String exchange;
    private String routingKey;
    private String resumeId;
    private long resumeTtl;


    public DeliveryProperties() {}

    public boolean hasDiscardUnroutable() {
        return (packing_flags & 256) != 0;
    }

    public DeliveryProperties clearDiscardUnroutable() {
        packing_flags &= ~256;

        setDirty(true);
        return this;
    }

    public boolean getDiscardUnroutable() {
        return hasDiscardUnroutable();
    }

    public DeliveryProperties setDiscardUnroutable(boolean value) {

        if (value)
        {
            packing_flags |= 256;
        }
        else
        {
            packing_flags &= ~256;
        }

        setDirty(true);
        return this;
    }

    public DeliveryProperties discardUnroutable(boolean value) {
        return setDiscardUnroutable(value);
    }

    public boolean hasImmediate() {
        return (packing_flags & 512) != 0;
    }

    public DeliveryProperties clearImmediate() {
        packing_flags &= ~512;

        setDirty(true);
        return this;
    }

    public boolean getImmediate() {
        return hasImmediate();
    }

    public DeliveryProperties setImmediate(boolean value) {

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

    public DeliveryProperties immediate(boolean value) {
        return setImmediate(value);
    }

    public boolean hasRedelivered() {
        return (packing_flags & 1024) != 0;
    }

    public DeliveryProperties clearRedelivered() {
        packing_flags &= ~1024;

        setDirty(true);
        return this;
    }

    public boolean getRedelivered() {
        return hasRedelivered();
    }

    public DeliveryProperties setRedelivered(boolean value) {

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

    public DeliveryProperties redelivered(boolean value) {
        return setRedelivered(value);
    }

    public boolean hasPriority() {
        return (packing_flags & 2048) != 0;
    }

    public DeliveryProperties clearPriority() {
        packing_flags &= ~2048;
        this.priority = null;
        setDirty(true);
        return this;
    }

    public MessageDeliveryPriority getPriority() {
        return priority;
    }

    public DeliveryProperties setPriority(MessageDeliveryPriority value) {
        this.priority = value;
        packing_flags |= 2048;
        setDirty(true);
        return this;
    }

    public DeliveryProperties priority(MessageDeliveryPriority value) {
        return setPriority(value);
    }

    public boolean hasDeliveryMode() {
        return (packing_flags & 4096) != 0;
    }

    public DeliveryProperties clearDeliveryMode() {
        packing_flags &= ~4096;
        this.deliveryMode = null;
        setDirty(true);
        return this;
    }

    public MessageDeliveryMode getDeliveryMode() {
        return deliveryMode;
    }

    public DeliveryProperties setDeliveryMode(MessageDeliveryMode value) {
        this.deliveryMode = value;
        packing_flags |= 4096;
        setDirty(true);
        return this;
    }

    public DeliveryProperties deliveryMode(MessageDeliveryMode value) {
        return setDeliveryMode(value);
    }

    public boolean hasTtl() {
        return (packing_flags & 8192) != 0;
    }

    public DeliveryProperties clearTtl() {
        packing_flags &= ~8192;
        this.ttl = 0;
        setDirty(true);
        return this;
    }

    public long getTtl() {
        return ttl;
    }

    public DeliveryProperties setTtl(long value) {
        this.ttl = value;
        packing_flags |= 8192;
        setDirty(true);
        return this;
    }

    public DeliveryProperties ttl(long value) {
        return setTtl(value);
    }

    public boolean hasTimestamp() {
        return (packing_flags & 16384) != 0;
    }

    public DeliveryProperties clearTimestamp() {
        packing_flags &= ~16384;
        this.timestamp = 0;
        setDirty(true);
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public DeliveryProperties setTimestamp(long value) {
        this.timestamp = value;
        packing_flags |= 16384;
        setDirty(true);
        return this;
    }

    public DeliveryProperties timestamp(long value) {
        return setTimestamp(value);
    }

    public boolean hasExpiration() {
        return (packing_flags & 32768) != 0;
    }

    public DeliveryProperties clearExpiration() {
        packing_flags &= ~32768;
        this.expiration = 0;
        setDirty(true);
        return this;
    }

    public long getExpiration() {
        return expiration;
    }

    public DeliveryProperties setExpiration(long value) {
        this.expiration = value;
        packing_flags |= 32768;
        setDirty(true);
        return this;
    }

    public DeliveryProperties expiration(long value) {
        return setExpiration(value);
    }

    public boolean hasExchange() {
        return (packing_flags & 1) != 0;
    }

    public DeliveryProperties clearExchange() {
        packing_flags &= ~1;
        this.exchange = null;
        setDirty(true);
        return this;
    }

    public String getExchange() {
        return exchange;
    }

    public DeliveryProperties setExchange(String value) {
        this.exchange = value;
        packing_flags |= 1;
        setDirty(true);
        return this;
    }

    public DeliveryProperties exchange(String value) {
        return setExchange(value);
    }

    public boolean hasRoutingKey() {
        return (packing_flags & 2) != 0;
    }

    public DeliveryProperties clearRoutingKey() {
        packing_flags &= ~2;
        this.routingKey = null;
        setDirty(true);
        return this;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public DeliveryProperties setRoutingKey(String value) {
        this.routingKey = value;
        packing_flags |= 2;
        setDirty(true);
        return this;
    }

    public DeliveryProperties routingKey(String value) {
        return setRoutingKey(value);
    }

    public boolean hasResumeId() {
        return (packing_flags & 4) != 0;
    }

    public DeliveryProperties clearResumeId() {
        packing_flags &= ~4;
        this.resumeId = null;
        setDirty(true);
        return this;
    }

    public String getResumeId() {
        return resumeId;
    }

    public DeliveryProperties setResumeId(String value) {
        this.resumeId = value;
        packing_flags |= 4;
        setDirty(true);
        return this;
    }

    public DeliveryProperties resumeId(String value) {
        return setResumeId(value);
    }

    public boolean hasResumeTtl() {
        return (packing_flags & 8) != 0;
    }

    public DeliveryProperties clearResumeTtl() {
        packing_flags &= ~8;
        this.resumeTtl = 0;
        setDirty(true);
        return this;
    }

    public long getResumeTtl() {
        return resumeTtl;
    }

    public DeliveryProperties setResumeTtl(long value) {
        this.resumeTtl = value;
        packing_flags |= 8;
        setDirty(true);
        return this;
    }

    public DeliveryProperties resumeTtl(long value) {
        return setResumeTtl(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 2048) != 0)
        {
            enc.writeUint8(this.priority.getValue());
        }
        if ((packing_flags & 4096) != 0)
        {
            enc.writeUint8(this.deliveryMode.getValue());
        }
        if ((packing_flags & 8192) != 0)
        {
            enc.writeUint64(this.ttl);
        }
        if ((packing_flags & 16384) != 0)
        {
            enc.writeDatetime(this.timestamp);
        }
        if ((packing_flags & 32768) != 0)
        {
            enc.writeDatetime(this.expiration);
        }
        if ((packing_flags & 1) != 0)
        {
            enc.writeStr8(this.exchange);
        }
        if ((packing_flags & 2) != 0)
        {
            enc.writeStr8(this.routingKey);
        }
        if ((packing_flags & 4) != 0)
        {
            enc.writeStr16(this.resumeId);
        }
        if ((packing_flags & 8) != 0)
        {
            enc.writeUint64(this.resumeTtl);
        }

    }

    @Override
    public int getEncodedLength()
    {
        int len = 0;

        len += 2; // packing_flags

        if ((packing_flags & 2048) != 0)
        {
            len += 1; // priority
        }
        if ((packing_flags & 4096) != 0)
        {
            len += 1; // deliveryMode
        }
        if ((packing_flags & 8192) != 0)
        {
            len += 8; // ttl
        }
        if ((packing_flags & 16384) != 0)
        {
            len += 8; // timestamp
        }
        if ((packing_flags & 32768) != 0)
        {
            len += 8; // expiration
        }
        if ((packing_flags & 1) != 0)
        {
            len += EncoderUtils.getStr8Length(this.exchange);
        }
        if ((packing_flags & 2) != 0)
        {
            len += EncoderUtils.getStr8Length(this.routingKey);
        }
        if ((packing_flags & 4) != 0)
        {
            len += EncoderUtils.getStr16Length(this.resumeId);
        }
        if ((packing_flags & 8) != 0)
        {
            len += 8; // resumeTtl
        }
        return len;
    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 2048) != 0)
        {
            this.priority = MessageDeliveryPriority.get(dec.readUint8());
        }
        if ((packing_flags & 4096) != 0)
        {
            this.deliveryMode = MessageDeliveryMode.get(dec.readUint8());
        }
        if ((packing_flags & 8192) != 0)
        {
            this.ttl = dec.readUint64();
        }
        if ((packing_flags & 16384) != 0)
        {
            this.timestamp = dec.readDatetime();
        }
        if ((packing_flags & 32768) != 0)
        {
            this.expiration = dec.readDatetime();
        }
        if ((packing_flags & 1) != 0)
        {
            this.exchange = dec.readStr8();
        }
        if ((packing_flags & 2) != 0)
        {
            this.routingKey = dec.readStr8();
        }
        if ((packing_flags & 4) != 0)
        {
            this.resumeId = dec.readStr16();
        }
        if ((packing_flags & 8) != 0)
        {
            this.resumeTtl = dec.readUint64();
        }

    }


    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("discardUnroutable", getDiscardUnroutable());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("immediate", getImmediate());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("redelivered", getRedelivered());
        }
        if ((packing_flags & 2048) != 0)
        {
            result.put("priority", getPriority());
        }
        if ((packing_flags & 4096) != 0)
        {
            result.put("deliveryMode", getDeliveryMode());
        }
        if ((packing_flags & 8192) != 0)
        {
            result.put("ttl", getTtl());
        }
        if ((packing_flags & 16384) != 0)
        {
            result.put("timestamp", getTimestamp());
        }
        if ((packing_flags & 32768) != 0)
        {
            result.put("expiration", getExpiration());
        }
        if ((packing_flags & 1) != 0)
        {
            result.put("exchange", getExchange());
        }
        if ((packing_flags & 2) != 0)
        {
            result.put("routingKey", getRoutingKey());
        }
        if ((packing_flags & 4) != 0)
        {
            result.put("resumeId", getResumeId());
        }
        if ((packing_flags & 8) != 0)
        {
            result.put("resumeTtl", getResumeTtl());
        }


        return result;
    }
}
