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


public final class ExchangeDeclare extends Method {

    public static final int TYPE = 1793;

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
    private String exchange;
    private String type;
    private String alternateExchange;
    private Map<String,Object> arguments;


    public ExchangeDeclare() {}


    public ExchangeDeclare(String exchange, String type, String alternateExchange, Map<String,Object> arguments, Option ... _options) {
        if(exchange != null) {
            setExchange(exchange);
        }
        if(type != null) {
            setType(type);
        }
        if(alternateExchange != null) {
            setAlternateExchange(alternateExchange);
        }
        if(arguments != null) {
            setArguments(arguments);
        }

        for (final Option option : _options)
        {
            switch (option)
            {
                case PASSIVE:
                    packing_flags |= 2048;
                    break;
                case DURABLE:
                    packing_flags |= 4096;
                    break;
                case AUTO_DELETE:
                    packing_flags |= 8192;
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
        delegate.exchangeDeclare(context, this);
    }


    public boolean hasExchange() {
        return (packing_flags & 256) != 0;
    }

    public ExchangeDeclare clearExchange() {
        packing_flags &= ~256;
        this.exchange = null;
        setDirty(true);
        return this;
    }

    public String getExchange() {
        return exchange;
    }

    public ExchangeDeclare setExchange(String value) {
        this.exchange = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public ExchangeDeclare exchange(String value) {
        return setExchange(value);
    }

    public boolean hasType() {
        return (packing_flags & 512) != 0;
    }

    public ExchangeDeclare clearType() {
        packing_flags &= ~512;
        this.type = null;
        setDirty(true);
        return this;
    }

    public String getType() {
        return type;
    }

    public ExchangeDeclare setType(String value) {
        this.type = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public ExchangeDeclare type(String value) {
        return setType(value);
    }

    public boolean hasAlternateExchange() {
        return (packing_flags & 1024) != 0;
    }

    public ExchangeDeclare clearAlternateExchange() {
        packing_flags &= ~1024;
        this.alternateExchange = null;
        setDirty(true);
        return this;
    }

    public String getAlternateExchange() {
        return alternateExchange;
    }

    public ExchangeDeclare setAlternateExchange(String value) {
        this.alternateExchange = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public ExchangeDeclare alternateExchange(String value) {
        return setAlternateExchange(value);
    }

    public boolean hasPassive() {
        return (packing_flags & 2048) != 0;
    }

    public ExchangeDeclare clearPassive() {
        packing_flags &= ~2048;

        setDirty(true);
        return this;
    }

    public boolean getPassive() {
        return hasPassive();
    }

    public ExchangeDeclare setPassive(boolean value) {

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

    public ExchangeDeclare passive(boolean value) {
        return setPassive(value);
    }

    public boolean hasDurable() {
        return (packing_flags & 4096) != 0;
    }

    public ExchangeDeclare clearDurable() {
        packing_flags &= ~4096;

        setDirty(true);
        return this;
    }

    public boolean getDurable() {
        return hasDurable();
    }

    public ExchangeDeclare setDurable(boolean value) {

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

    public ExchangeDeclare durable(boolean value) {
        return setDurable(value);
    }

    public boolean hasAutoDelete() {
        return (packing_flags & 8192) != 0;
    }

    public ExchangeDeclare clearAutoDelete() {
        packing_flags &= ~8192;

        setDirty(true);
        return this;
    }

    public boolean getAutoDelete() {
        return hasAutoDelete();
    }

    public ExchangeDeclare setAutoDelete(boolean value) {

        if (value)
        {
            packing_flags |= 8192;
        }
        else
        {
            packing_flags &= ~8192;
        }

        setDirty(true);
        return this;
    }

    public ExchangeDeclare autoDelete(boolean value) {
        return setAutoDelete(value);
    }

    public boolean hasArguments() {
        return (packing_flags & 16384) != 0;
    }

    public ExchangeDeclare clearArguments() {
        packing_flags &= ~16384;
        this.arguments = null;
        setDirty(true);
        return this;
    }

    public Map<String,Object> getArguments() {
        return arguments;
    }

    public ExchangeDeclare setArguments(Map<String, Object> value) {
        this.arguments = value;
        packing_flags |= 16384;
        setDirty(true);
        return this;
    }

    public ExchangeDeclare arguments(Map<String, Object> value) {
        return setArguments(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeStr8(this.exchange);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeStr8(this.type);
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeStr8(this.alternateExchange);
        }
        if ((packing_flags & 16384) != 0)
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
            this.exchange = dec.readStr8();
        }
        if ((packing_flags & 512) != 0)
        {
            this.type = dec.readStr8();
        }
        if ((packing_flags & 1024) != 0)
        {
            this.alternateExchange = dec.readStr8();
        }
        if ((packing_flags & 16384) != 0)
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
            result.put("exchange", getExchange());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("type", getType());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("alternateExchange", getAlternateExchange());
        }
        if ((packing_flags & 2048) != 0)
        {
            result.put("passive", getPassive());
        }
        if ((packing_flags & 4096) != 0)
        {
            result.put("durable", getDurable());
        }
        if ((packing_flags & 8192) != 0)
        {
            result.put("autoDelete", getAutoDelete());
        }
        if ((packing_flags & 16384) != 0)
        {
            result.put("arguments", getArguments());
        }
        return result;
    }
}
