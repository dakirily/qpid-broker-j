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


public final class FragmentProperties extends Struct {

    public static final int TYPE = 1026;

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
    private long fragmentSize;


    public FragmentProperties() {}


    public FragmentProperties(long fragmentSize, Option ... _options) {
        setFragmentSize(fragmentSize);

        for (final Option option : _options)
        {
            switch (option)
            {
                case FIRST:
                    packing_flags |= 256;
                    break;
                case LAST:
                    packing_flags |= 512;
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException("invalid option: " + option);
            }
        }

    }




    public boolean hasFirst() {
        return (packing_flags & 256) != 0;
    }

    public FragmentProperties clearFirst() {
        packing_flags &= ~256;

        setDirty(true);
        return this;
    }

    public boolean getFirst() {
        return hasFirst();
    }

    public FragmentProperties setFirst(boolean value) {

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

    public FragmentProperties first(boolean value) {
        return setFirst(value);
    }

    public boolean hasLast() {
        return (packing_flags & 512) != 0;
    }

    public FragmentProperties clearLast() {
        packing_flags &= ~512;

        setDirty(true);
        return this;
    }

    public boolean getLast() {
        return hasLast();
    }

    public FragmentProperties setLast(boolean value) {

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

    public FragmentProperties last(boolean value) {
        return setLast(value);
    }

    public boolean hasFragmentSize() {
        return (packing_flags & 1024) != 0;
    }

    public FragmentProperties clearFragmentSize() {
        packing_flags &= ~1024;
        this.fragmentSize = 0;
        setDirty(true);
        return this;
    }

    public long getFragmentSize() {
        return fragmentSize;
    }

    public FragmentProperties setFragmentSize(long value) {
        this.fragmentSize = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public FragmentProperties fragmentSize(long value) {
        return setFragmentSize(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 1024) != 0)
        {
            enc.writeUint64(this.fragmentSize);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 1024) != 0)
        {
            this.fragmentSize = dec.readUint64();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("first", getFirst());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("last", getLast());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("fragmentSize", getFragmentSize());
        }
        return result;
    }

    @Override
    public int getEncodedLength()
    {
        throw new UnsupportedOperationException();
    }
}
