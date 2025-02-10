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


public final class ExchangeQueryResult extends Struct {

    public static final int TYPE = 1793;

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
    private String type;
    private Map<String,Object> arguments;


    public ExchangeQueryResult() {}


    public ExchangeQueryResult(String type, Map<String,Object> arguments, Option ... _options) {
        if(type != null) {
            setType(type);
        }
        if(arguments != null) {
            setArguments(arguments);
        }

        for (final Option option : _options)
        {
            switch (option)
            {
                case DURABLE:
                    packing_flags |= 512;
                    break;
                case NOT_FOUND:
                    packing_flags |= 1024;
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException("invalid option: " + option);
            }
        }

    }




    public boolean hasType() {
        return (packing_flags & 256) != 0;
    }

    public ExchangeQueryResult clearType() {
        packing_flags &= ~256;
        this.type = null;
        setDirty(true);
        return this;
    }

    public String getType() {
        return type;
    }

    public ExchangeQueryResult setType(String value) {
        this.type = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public ExchangeQueryResult type(String value) {
        return setType(value);
    }

    public boolean hasDurable() {
        return (packing_flags & 512) != 0;
    }

    public ExchangeQueryResult clearDurable() {
        packing_flags &= ~512;

        setDirty(true);
        return this;
    }

    public boolean getDurable() {
        return hasDurable();
    }

    public ExchangeQueryResult setDurable(boolean value) {

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

    public ExchangeQueryResult durable(boolean value) {
        return setDurable(value);
    }

    public boolean hasNotFound() {
        return (packing_flags & 1024) != 0;
    }

    public ExchangeQueryResult clearNotFound() {
        packing_flags &= ~1024;

        setDirty(true);
        return this;
    }

    public boolean getNotFound() {
        return hasNotFound();
    }

    public ExchangeQueryResult setNotFound(boolean value) {

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

    public ExchangeQueryResult notFound(boolean value) {
        return setNotFound(value);
    }

    public boolean hasArguments() {
        return (packing_flags & 2048) != 0;
    }

    public ExchangeQueryResult clearArguments() {
        packing_flags &= ~2048;
        this.arguments = null;
        setDirty(true);
        return this;
    }

    public Map<String,Object> getArguments() {
        return arguments;
    }

    public ExchangeQueryResult setArguments(Map<String, Object> value) {
        this.arguments = value;
        packing_flags |= 2048;
        setDirty(true);
        return this;
    }

    public ExchangeQueryResult arguments(Map<String, Object> value) {
        return setArguments(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeStr8(this.type);
        }
        if ((packing_flags & 2048) != 0)
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
            this.type = dec.readStr8();
        }
        if ((packing_flags & 2048) != 0)
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
            result.put("type", getType());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("durable", getDurable());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("notFound", getNotFound());
        }
        if ((packing_flags & 2048) != 0)
        {
            result.put("arguments", getArguments());
        }
        return result;
    }

    @Override
    public int getEncodedLength()
    {
        throw new UnsupportedOperationException();
    }
}
