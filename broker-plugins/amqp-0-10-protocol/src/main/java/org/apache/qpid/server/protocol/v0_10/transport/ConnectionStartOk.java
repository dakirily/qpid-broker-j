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


public final class ConnectionStartOk extends Method {

    public static final int TYPE = 258;

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
    private Map<String,Object> clientProperties;
    private String mechanism;
    private byte[] response;
    private String locale;


    public ConnectionStartOk() {}


    public ConnectionStartOk(Map<String,Object> clientProperties, String mechanism, byte[] response, String locale, Option ... _options) {
        if(clientProperties != null) {
            setClientProperties(clientProperties);
        }
        if(mechanism != null) {
            setMechanism(mechanism);
        }
        if(response != null) {
            setResponse(response);
        }
        if(locale != null) {
            setLocale(locale);
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
        delegate.connectionStartOk(context, this);
    }


    public boolean hasClientProperties() {
        return (packing_flags & 256) != 0;
    }

    public ConnectionStartOk clearClientProperties() {
        packing_flags &= ~256;
        this.clientProperties = null;
        setDirty(true);
        return this;
    }

    public Map<String,Object> getClientProperties() {
        return clientProperties;
    }

    public ConnectionStartOk setClientProperties(Map<String, Object> value) {
        this.clientProperties = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public ConnectionStartOk clientProperties(Map<String, Object> value) {
        return setClientProperties(value);
    }

    public boolean hasMechanism() {
        return (packing_flags & 512) != 0;
    }

    public ConnectionStartOk clearMechanism() {
        packing_flags &= ~512;
        this.mechanism = null;
        setDirty(true);
        return this;
    }

    public String getMechanism() {
        return mechanism;
    }

    public ConnectionStartOk setMechanism(String value) {
        this.mechanism = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public ConnectionStartOk mechanism(String value) {
        return setMechanism(value);
    }

    public boolean hasResponse() {
        return (packing_flags & 1024) != 0;
    }

    public ConnectionStartOk clearResponse() {
        packing_flags &= ~1024;
        this.response = null;
        setDirty(true);
        return this;
    }

    public byte[] getResponse() {
        return response;
    }

    public ConnectionStartOk setResponse(byte[] value) {
        this.response = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public ConnectionStartOk response(byte[] value) {
        return setResponse(value);
    }

    public boolean hasLocale() {
        return (packing_flags & 2048) != 0;
    }

    public ConnectionStartOk clearLocale() {
        packing_flags &= ~2048;
        this.locale = null;
        setDirty(true);
        return this;
    }

    public String getLocale() {
        return locale;
    }

    public ConnectionStartOk setLocale(String value) {
        this.locale = value;
        packing_flags |= 2048;
        setDirty(true);
        return this;
    }

    public ConnectionStartOk locale(String value) {
        return setLocale(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeMap(this.clientProperties);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeStr8(this.mechanism);
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeVbin32(this.response);
        }
        if ((packing_flags & 2048) != 0)
        {
            enc.writeStr8(this.locale);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.clientProperties = dec.readMap();
        }
        if ((packing_flags & 512) != 0)
        {
            this.mechanism = dec.readStr8();
        }
        if ((packing_flags & 1024) != 0)
        {
            this.response = dec.readVbin32();
        }
        if ((packing_flags & 2048) != 0)
        {
            this.locale = dec.readStr8();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("clientProperties", getClientProperties());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("mechanism", getMechanism());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("response", getResponse());
        }
        if ((packing_flags & 2048) != 0)
        {
            result.put("locale", getLocale());
        }
        return result;
    }
}
