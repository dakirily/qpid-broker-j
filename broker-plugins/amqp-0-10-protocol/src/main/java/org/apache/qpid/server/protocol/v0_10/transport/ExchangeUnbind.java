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


public final class ExchangeUnbind extends Method {

    public static final int TYPE = 1797;

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
    private String exchange;
    private String bindingKey;


    public ExchangeUnbind() {}


    public ExchangeUnbind(String queue, String exchange, String bindingKey, Option ... _options) {
        if(queue != null) {
            setQueue(queue);
        }
        if(exchange != null) {
            setExchange(exchange);
        }
        if(bindingKey != null) {
            setBindingKey(bindingKey);
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
        delegate.exchangeUnbind(context, this);
    }


    public boolean hasQueue() {
        return (packing_flags & 256) != 0;
    }

    public ExchangeUnbind clearQueue() {
        packing_flags &= ~256;
        this.queue = null;
        setDirty(true);
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public ExchangeUnbind setQueue(String value) {
        this.queue = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public ExchangeUnbind queue(String value) {
        return setQueue(value);
    }

    public boolean hasExchange() {
        return (packing_flags & 512) != 0;
    }

    public ExchangeUnbind clearExchange() {
        packing_flags &= ~512;
        this.exchange = null;
        setDirty(true);
        return this;
    }

    public String getExchange() {
        return exchange;
    }

    public ExchangeUnbind setExchange(String value) {
        this.exchange = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public ExchangeUnbind exchange(String value) {
        return setExchange(value);
    }

    public boolean hasBindingKey() {
        return (packing_flags & 1024) != 0;
    }

    public ExchangeUnbind clearBindingKey() {
        packing_flags &= ~1024;
        this.bindingKey = null;
        setDirty(true);
        return this;
    }

    public String getBindingKey() {
        return bindingKey;
    }

    public ExchangeUnbind setBindingKey(String value) {
        this.bindingKey = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public ExchangeUnbind bindingKey(String value) {
        return setBindingKey(value);
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
            enc.writeStr8(this.exchange);
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeStr8(this.bindingKey);
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
            this.exchange = dec.readStr8();
        }
        if ((packing_flags & 1024) != 0)
        {
            this.bindingKey = dec.readStr8();
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
            result.put("exchange", getExchange());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("bindingKey", getBindingKey());
        }
        return result;
    }
}
