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


public final class ConnectionStart extends Method {

    public static final int TYPE = 257;

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
    private Map<String,Object> serverProperties;
    private java.util.List<Object> mechanisms;
    private java.util.List<Object> locales;


    public ConnectionStart() {}


    public ConnectionStart(Map<String,Object> serverProperties, java.util.List<Object> mechanisms, java.util.List<Object> locales, Option ... _options) {
        if(serverProperties != null) {
            setServerProperties(serverProperties);
        }
        if(mechanisms != null) {
            setMechanisms(mechanisms);
        }
        if(locales != null) {
            setLocales(locales);
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
        delegate.connectionStart(context, this);
    }


    public boolean hasServerProperties() {
        return (packing_flags & 256) != 0;
    }

    public ConnectionStart clearServerProperties() {
        packing_flags &= ~256;
        this.serverProperties = null;
        setDirty(true);
        return this;
    }

    public Map<String,Object> getServerProperties() {
        return serverProperties;
    }

    public ConnectionStart setServerProperties(Map<String, Object> value) {
        this.serverProperties = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public ConnectionStart serverProperties(Map<String, Object> value) {
        return setServerProperties(value);
    }

    public boolean hasMechanisms() {
        return (packing_flags & 512) != 0;
    }

    public ConnectionStart clearMechanisms() {
        packing_flags &= ~512;
        this.mechanisms = null;
        setDirty(true);
        return this;
    }

    public java.util.List<Object> getMechanisms() {
        return mechanisms;
    }

    public ConnectionStart setMechanisms(java.util.List<Object> value) {
        this.mechanisms = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public ConnectionStart mechanisms(java.util.List<Object> value) {
        return setMechanisms(value);
    }

    public boolean hasLocales() {
        return (packing_flags & 1024) != 0;
    }

    public ConnectionStart clearLocales() {
        packing_flags &= ~1024;
        this.locales = null;
        setDirty(true);
        return this;
    }

    public java.util.List<Object> getLocales() {
        return locales;
    }

    public ConnectionStart setLocales(java.util.List<Object> value) {
        this.locales = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public ConnectionStart locales(java.util.List<Object> value) {
        return setLocales(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeMap(this.serverProperties);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeArray(this.mechanisms);
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeArray(this.locales);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.serverProperties = dec.readMap();
        }
        if ((packing_flags & 512) != 0)
        {
            this.mechanisms = dec.readArray();
        }
        if ((packing_flags & 1024) != 0)
        {
            this.locales = dec.readArray();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("serverProperties", getServerProperties());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("mechanisms", getMechanisms());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("locales", getLocales());
        }
        return result;
    }
}
