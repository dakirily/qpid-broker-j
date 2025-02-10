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


public final class ExchangeBoundResult extends Struct {

    public static final int TYPE = 1794;

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


    public ExchangeBoundResult() {}


    public ExchangeBoundResult(Option ... _options) {

        for (final Option option : _options)
        {
            switch (option)
            {
                case EXCHANGE_NOT_FOUND:
                    packing_flags |= 256;
                    break;
                case QUEUE_NOT_FOUND:
                    packing_flags |= 512;
                    break;
                case QUEUE_NOT_MATCHED:
                    packing_flags |= 1024;
                    break;
                case KEY_NOT_MATCHED:
                    packing_flags |= 2048;
                    break;
                case ARGS_NOT_MATCHED:
                    packing_flags |= 4096;
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException("invalid option: " + option);
            }
        }

    }




    public boolean hasExchangeNotFound() {
        return (packing_flags & 256) != 0;
    }

    public ExchangeBoundResult clearExchangeNotFound() {
        packing_flags &= ~256;

        setDirty(true);
        return this;
    }

    public boolean getExchangeNotFound() {
        return hasExchangeNotFound();
    }

    public ExchangeBoundResult setExchangeNotFound(boolean value) {

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

    public ExchangeBoundResult exchangeNotFound(boolean value) {
        return setExchangeNotFound(value);
    }

    public boolean hasQueueNotFound() {
        return (packing_flags & 512) != 0;
    }

    public ExchangeBoundResult clearQueueNotFound() {
        packing_flags &= ~512;

        setDirty(true);
        return this;
    }

    public boolean getQueueNotFound() {
        return hasQueueNotFound();
    }

    public ExchangeBoundResult setQueueNotFound(boolean value) {

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

    public ExchangeBoundResult queueNotFound(boolean value) {
        return setQueueNotFound(value);
    }

    public boolean hasQueueNotMatched() {
        return (packing_flags & 1024) != 0;
    }

    public ExchangeBoundResult clearQueueNotMatched() {
        packing_flags &= ~1024;

        setDirty(true);
        return this;
    }

    public boolean getQueueNotMatched() {
        return hasQueueNotMatched();
    }

    public ExchangeBoundResult setQueueNotMatched(boolean value) {

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

    public ExchangeBoundResult queueNotMatched(boolean value) {
        return setQueueNotMatched(value);
    }

    public boolean hasKeyNotMatched() {
        return (packing_flags & 2048) != 0;
    }

    public ExchangeBoundResult clearKeyNotMatched() {
        packing_flags &= ~2048;

        setDirty(true);
        return this;
    }

    public boolean getKeyNotMatched() {
        return hasKeyNotMatched();
    }

    public ExchangeBoundResult setKeyNotMatched(boolean value) {

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

    public ExchangeBoundResult keyNotMatched(boolean value) {
        return setKeyNotMatched(value);
    }

    public boolean hasArgsNotMatched() {
        return (packing_flags & 4096) != 0;
    }

    public ExchangeBoundResult clearArgsNotMatched() {
        packing_flags &= ~4096;

        setDirty(true);
        return this;
    }

    public boolean getArgsNotMatched() {
        return hasArgsNotMatched();
    }

    public ExchangeBoundResult setArgsNotMatched(boolean value) {

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

    public ExchangeBoundResult argsNotMatched(boolean value) {
        return setArgsNotMatched(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("exchangeNotFound", getExchangeNotFound());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("queueNotFound", getQueueNotFound());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("queueNotMatched", getQueueNotMatched());
        }
        if ((packing_flags & 2048) != 0)
        {
            result.put("keyNotMatched", getKeyNotMatched());
        }
        if ((packing_flags & 4096) != 0)
        {
            result.put("argsNotMatched", getArgsNotMatched());
        }
        return result;
    }

    @Override
    public int getEncodedLength()
    {
        throw new UnsupportedOperationException();
    }
}
