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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;


public final class MessageTransfer extends Method {

    public static final int TYPE = 1025;
    private int _bodySize;

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
        return true;
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
    private String destination;
    private MessageAcceptMode acceptMode;
    private MessageAcquireMode acquireMode;
    private Header header;
    private QpidByteBuffer _body;


    public MessageTransfer() {}

    public MessageTransfer(String destination, MessageAcceptMode acceptMode, MessageAcquireMode acquireMode, Header header, java.nio.ByteBuffer body, Option ... options)
    {
        this(destination,
             acceptMode,
             acquireMode,
             header,
             QpidByteBuffer.wrap(body),
             options);
    }

    public MessageTransfer(String destination, MessageAcceptMode acceptMode, MessageAcquireMode acquireMode, Header header, QpidByteBuffer body, Option ... _options) {
        if(destination != null) {
            setDestination(destination);
        }
        if(acceptMode != null) {
            setAcceptMode(acceptMode);
        }
        if(acquireMode != null) {
            setAcquireMode(acquireMode);
        }
        setHeader(header);
        setBody(body);

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
        delegate.messageTransfer(context, this);
    }


    public boolean hasDestination() {
        return (packing_flags & 256) != 0;
    }

    public MessageTransfer clearDestination() {
        packing_flags &= ~256;
        this.destination = null;
        setDirty(true);
        return this;
    }

    public String getDestination() {
        return destination;
    }

    public MessageTransfer setDestination(String value) {
        this.destination = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public MessageTransfer destination(String value) {
        return setDestination(value);
    }

    public boolean hasAcceptMode() {
        return (packing_flags & 512) != 0;
    }

    public MessageTransfer clearAcceptMode() {
        packing_flags &= ~512;
        this.acceptMode = null;
        setDirty(true);
        return this;
    }

    public MessageAcceptMode getAcceptMode() {
        return acceptMode;
    }

    public MessageTransfer setAcceptMode(MessageAcceptMode value) {
        this.acceptMode = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public MessageTransfer acceptMode(MessageAcceptMode value) {
        return setAcceptMode(value);
    }

    public boolean hasAcquireMode() {
        return (packing_flags & 1024) != 0;
    }

    public MessageTransfer clearAcquireMode() {
        packing_flags &= ~1024;
        this.acquireMode = null;
        setDirty(true);
        return this;
    }

    public MessageAcquireMode getAcquireMode() {
        return acquireMode;
    }

    public MessageTransfer setAcquireMode(MessageAcquireMode value) {
        this.acquireMode = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public MessageTransfer acquireMode(MessageAcquireMode value) {
        return setAcquireMode(value);
    }


    @Override
    public Header getHeader() {
        return this.header;
    }

    @Override
    public void setHeader(Header header) {
        this.header = header;
    }

    public MessageTransfer header(Header header)
    {
        setHeader(header);
        return this;
    }

    @Override
    public QpidByteBuffer getBody()
    {
        return _body;
    }

    @Override
    public void setBody(QpidByteBuffer body)
    {
        if (body == null)
        {
            _bodySize = 0;
            if (_body != null)
            {
                _body.dispose();
            }
            _body = null;
        }
        else
        {
            _body = body.duplicate();
            _bodySize = _body.remaining();
        }
    }

    @Override
    public int getBodySize()
    {
        return _bodySize;
    }

    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeStr8(this.destination);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeUint8(this.acceptMode.getValue());
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeUint8(this.acquireMode.getValue());
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.destination = dec.readStr8();
        }
        if ((packing_flags & 512) != 0)
        {
            this.acceptMode = MessageAcceptMode.get(dec.readUint8());
        }
        if ((packing_flags & 1024) != 0)
        {
            this.acquireMode = MessageAcquireMode.get(dec.readUint8());
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("destination", getDestination());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("acceptMode", getAcceptMode());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("acquireMode", getAcquireMode());
        }
        return result;
    }

    public void dispose()
    {
        if (_body != null)
        {
            _body.dispose();
            _body = null;
        }
    }
}
