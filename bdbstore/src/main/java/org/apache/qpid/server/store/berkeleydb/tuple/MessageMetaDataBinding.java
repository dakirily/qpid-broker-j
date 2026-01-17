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

package org.apache.qpid.server.store.berkeleydb.tuple;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.store.MessageMetaDataTypeRegistry;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;

/**
 * Berkeley DB binding for {@link StorableMessageMetaData}.
 *
 * <p>Serialized format of the value:</p>
 * <ol>
 *   <li>
 *     <b>Body size</b> (4 bytes, big-endian int): {@code (1 + metaData.getStorableSize()) ^ 0x80000000}.
 *     This matches JE tuple integer encoding (sign bit flipped) and represents the number of bytes
 *     that follow in the body (type byte + metadata payload).
 *   </li>
 *   <li>
 *     <b>Meta-data type</b> (1 byte, unsigned): ordinal of a {@link MessageMetaDataType} registered in
 *     {@link MessageMetaDataTypeRegistry}.
 *   </li>
 *   <li>
 *     <b>Meta-data payload</b> ({@code metaData.getStorableSize()} bytes): bytes written by
 *     {@link StorableMessageMetaData#writeToBuffer(org.apache.qpid.server.bytebuffer.QpidByteBuffer)}.
 *   </li>
 * </ol>
 *
 * <p>The reader currently derives payload length from {@link com.sleepycat.je.DatabaseEntry#getSize()};
 * the size field is retained for compatibility and may be used for integrity checks.</p>
 */
public class MessageMetaDataBinding implements EntryBinding<StorableMessageMetaData>
{
    private static final MessageMetaDataBinding INSTANCE = new MessageMetaDataBinding();

    public static MessageMetaDataBinding getInstance()
    {
        return INSTANCE;
    }

    /** private constructor forces getInstance instead */
    private MessageMetaDataBinding()
    {

    }

    @Override
    public StorableMessageMetaData entryToObject(final DatabaseEntry entry)
    {
        final byte[] data = entry.getData();
        final int size = entry.getSize();

        if (data == null)
        {
            throw new StoreException("Unable to convert entry '%s' to metadata, data is null".formatted(entry));
        }

        if (size < 5)
        {
            throw new StoreException("Unable to convert entry '%s' to metadata, entry too short".formatted(entry));
        }

        final int offset = entry.getOffset();

        if (offset + size > data.length)
        {
            throw new StoreException("Unable to convert database entry to metadata, entry offset + size > data.length");
        }

        // read next byte for metaDataType
        final int metaDataType = data[offset + 4] & 0xFF;
        final MessageMetaDataType<?> type = MessageMetaDataTypeRegistry.fromOrdinal(metaDataType);

        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(data, offset + 5, size - 5))
        {
            return type.createMetaData(buf);
        }
        catch (RuntimeException e)
        {
            throw new StoreException("Unable to convert entry %s to metadata".formatted(entry), e);
        }
    }

    @Override
    public void objectToEntry(final StorableMessageMetaData metaData, final DatabaseEntry entry)
    {
        final int bodySize = 1 + metaData.getStorableSize();
        final byte[] underlying = new byte[4 + bodySize];

        // write bodySize ^ 0x80000000 as 4 bytes
        int flippedSize = bodySize ^ 0x80000000;
        underlying[0] = (byte) (flippedSize >>> 24);
        underlying[1] = (byte) (flippedSize >>> 16);
        underlying[2] = (byte) (flippedSize >>> 8);
        underlying[3] = (byte) (flippedSize & 0xFF);

        // write metaData type
        underlying[4] = (byte) metaData.getType().ordinal();

        // write rest of the metaData
        try (final QpidByteBuffer buf = QpidByteBuffer.wrap(underlying, 5, bodySize - 1))
        {
            metaData.writeToBuffer(buf);
        }
        entry.setData(underlying);
    }
}
