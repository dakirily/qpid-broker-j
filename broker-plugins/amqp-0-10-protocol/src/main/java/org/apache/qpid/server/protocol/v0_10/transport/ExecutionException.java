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


public final class ExecutionException extends Method {

    public static final int TYPE = 771;

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
    private ExecutionErrorCode errorCode;
    private int commandId;
    private short classCode;
    private short commandCode;
    private short fieldIndex;
    private String description;
    private Map<String,Object> errorInfo;


    public ExecutionException() {}


    public ExecutionException(ExecutionErrorCode errorCode, int commandId, short classCode, short commandCode, short fieldIndex, String description, Map<String,Object> errorInfo, Option ... _options) {
        if(errorCode != null) {
            setErrorCode(errorCode);
        }
        setCommandId(commandId);
        setClassCode(classCode);
        setCommandCode(commandCode);
        setFieldIndex(fieldIndex);
        if(description != null) {
            setDescription(description);
        }
        if(errorInfo != null) {
            setErrorInfo(errorInfo);
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
        delegate.executionException(context, this);
    }


    public boolean hasErrorCode() {
        return (packing_flags & 256) != 0;
    }

    public ExecutionException clearErrorCode() {
        packing_flags &= ~256;
        this.errorCode = null;
        setDirty(true);
        return this;
    }

    public ExecutionErrorCode getErrorCode() {
        return errorCode;
    }

    public ExecutionException setErrorCode(ExecutionErrorCode value) {
        this.errorCode = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public ExecutionException errorCode(ExecutionErrorCode value) {
        return setErrorCode(value);
    }

    public boolean hasCommandId() {
        return (packing_flags & 512) != 0;
    }

    public ExecutionException clearCommandId() {
        packing_flags &= ~512;
        this.commandId = 0;
        setDirty(true);
        return this;
    }

    public int getCommandId() {
        return commandId;
    }

    public ExecutionException setCommandId(int value) {
        this.commandId = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public ExecutionException commandId(int value) {
        return setCommandId(value);
    }

    public boolean hasClassCode() {
        return (packing_flags & 1024) != 0;
    }

    public ExecutionException clearClassCode() {
        packing_flags &= ~1024;
        this.classCode = 0;
        setDirty(true);
        return this;
    }

    public short getClassCode() {
        return classCode;
    }

    public ExecutionException setClassCode(short value) {
        this.classCode = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public ExecutionException classCode(short value) {
        return setClassCode(value);
    }

    public boolean hasCommandCode() {
        return (packing_flags & 2048) != 0;
    }

    public ExecutionException clearCommandCode() {
        packing_flags &= ~2048;
        this.commandCode = 0;
        setDirty(true);
        return this;
    }

    public short getCommandCode() {
        return commandCode;
    }

    public ExecutionException setCommandCode(short value) {
        this.commandCode = value;
        packing_flags |= 2048;
        setDirty(true);
        return this;
    }

    public ExecutionException commandCode(short value) {
        return setCommandCode(value);
    }

    public boolean hasFieldIndex() {
        return (packing_flags & 4096) != 0;
    }

    public ExecutionException clearFieldIndex() {
        packing_flags &= ~4096;
        this.fieldIndex = 0;
        setDirty(true);
        return this;
    }

    public short getFieldIndex() {
        return fieldIndex;
    }

    public ExecutionException setFieldIndex(short value) {
        this.fieldIndex = value;
        packing_flags |= 4096;
        setDirty(true);
        return this;
    }

    public ExecutionException fieldIndex(short value) {
        return setFieldIndex(value);
    }

    public boolean hasDescription() {
        return (packing_flags & 8192) != 0;
    }

    public ExecutionException clearDescription() {
        packing_flags &= ~8192;
        this.description = null;
        setDirty(true);
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ExecutionException setDescription(String value) {
        this.description = value;
        packing_flags |= 8192;
        setDirty(true);
        return this;
    }

    public ExecutionException description(String value) {
        return setDescription(value);
    }

    public boolean hasErrorInfo() {
        return (packing_flags & 16384) != 0;
    }

    public ExecutionException clearErrorInfo() {
        packing_flags &= ~16384;
        this.errorInfo = null;
        setDirty(true);
        return this;
    }

    public Map<String,Object> getErrorInfo() {
        return errorInfo;
    }

    public ExecutionException setErrorInfo(Map<String, Object> value) {
        this.errorInfo = value;
        packing_flags |= 16384;
        setDirty(true);
        return this;
    }

    public ExecutionException errorInfo(Map<String, Object> value) {
        return setErrorInfo(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeUint16(this.errorCode.getValue());
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeSequenceNo(this.commandId);
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeUint8(this.classCode);
        }
        if ((packing_flags & 2048) != 0)
        {
            enc.writeUint8(this.commandCode);
        }
        if ((packing_flags & 4096) != 0)
        {
            enc.writeUint8(this.fieldIndex);
        }
        if ((packing_flags & 8192) != 0)
        {
            enc.writeStr16(this.description);
        }
        if ((packing_flags & 16384) != 0)
        {
            enc.writeMap(this.errorInfo);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.errorCode = ExecutionErrorCode.get(dec.readUint16());
        }
        if ((packing_flags & 512) != 0)
        {
            this.commandId = dec.readSequenceNo();
        }
        if ((packing_flags & 1024) != 0)
        {
            this.classCode = dec.readUint8();
        }
        if ((packing_flags & 2048) != 0)
        {
            this.commandCode = dec.readUint8();
        }
        if ((packing_flags & 4096) != 0)
        {
            this.fieldIndex = dec.readUint8();
        }
        if ((packing_flags & 8192) != 0)
        {
            this.description = dec.readStr16();
        }
        if ((packing_flags & 16384) != 0)
        {
            this.errorInfo = dec.readMap();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("errorCode", getErrorCode());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("commandId", getCommandId());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("classCode", getClassCode());
        }
        if ((packing_flags & 2048) != 0)
        {
            result.put("commandCode", getCommandCode());
        }
        if ((packing_flags & 4096) != 0)
        {
            result.put("fieldIndex", getFieldIndex());
        }
        if ((packing_flags & 8192) != 0)
        {
            result.put("description", getDescription());
        }
        if ((packing_flags & 16384) != 0)
        {
            result.put("errorInfo", getErrorInfo());
        }
        return result;
    }
}
