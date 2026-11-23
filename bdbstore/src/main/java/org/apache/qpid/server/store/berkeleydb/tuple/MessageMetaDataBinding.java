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

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.store.MessageMetaDataTypeRegistry;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;

/**
 * Handles the mapping to and from message meta data
 */
public class MessageMetaDataBinding implements EntryBinding<StorableMessageMetaData>
{

    private static final int HEADER_SIZE = Integer.BYTES + Byte.BYTES;
    private static final MessageMetaDataBinding INSTANCE = new MessageMetaDataBinding();

    public static MessageMetaDataBinding getInstance()
    {
        return INSTANCE;
    }

    /** private constructor forces getInstance instead */
    private MessageMetaDataBinding() { }

    @Override
    public StorableMessageMetaData entryToObject(final DatabaseEntry entry)
    {
        final byte[] data = entry == null ? null : entry.getData();
        if (data == null)
        {
            throw new StoreException("Cannot decode message metadata: missing entry data");
        }

        final int size = entry.getSize();
        if (size < HEADER_SIZE)
        {
            throw new StoreException("Cannot decode message metadata: entry is too short (size %d)".formatted(size));
        }

        final int offset = entry.getOffset();
        if (offset < 0 || offset > data.length - size)
        {
            throw new StoreException("Cannot decode message metadata: invalid entry bounds (size %d)".formatted(size));
        }

        final int metaDataType = data[offset + Integer.BYTES] & 0xff;
        final int payloadSize = size - HEADER_SIZE;
        try
        {
            final MessageMetaDataType<?> type = MessageMetaDataTypeRegistry.fromOrdinal(metaDataType);
            // Codecs can retain slices of the payload after the JE entry is reused.
            try (final QpidByteBuffer buf = payloadSize == 0 ?
                    QpidByteBuffer.emptyQpidByteBuffer() : QpidByteBuffer.allocateDirect(payloadSize))
            {
                buf.put(data, offset + HEADER_SIZE, payloadSize).flip();
                return type.createMetaData(buf);
            }
        }
        catch (final RuntimeException e)
        {
            // Codec exception messages can include data from the entry.
            throw new StoreException("Cannot decode message metadata (type %d, size %d, %s)"
                    .formatted(metaDataType, size, e.getClass().getSimpleName()));
        }
    }

    @Override
    public void objectToEntry(final StorableMessageMetaData metaData, final DatabaseEntry entry)
    {
        final int size = getEntrySize(metaData);
        final byte[] data = new byte[size];
        writeToBuffer(metaData, data, 0, size);
        entry.setData(data);
    }

    public int getEntrySize(final StorableMessageMetaData metaData)
    {
        final int payloadSize = metaData.getStorableSize();
        if (payloadSize < 0 || payloadSize > Integer.MAX_VALUE - HEADER_SIZE)
        {
            throw new StoreException("Invalid message metadata size: " + payloadSize);
        }
        return HEADER_SIZE + payloadSize;
    }

    /**
     * Serializes into a caller-owned entry prepared with {@link #getEntrySize}.
     * Keep the backing array leased through the final database put attempt.
     */
    public void writeToEntry(final StorableMessageMetaData metaData, final DatabaseEntry entry)
    {
        final byte[] data = entry.getData();
        final int offset = entry.getOffset();
        final int size = entry.getSize();
        if (data == null || size < HEADER_SIZE || offset < 0 || offset > data.length - size)
        {
            throw new IllegalArgumentException("Invalid borrowed message metadata entry");
        }

        entry.setPartial(0, 0, false);
        // A serializer can skip reserved bytes that would be zero in a fresh entry.
        Arrays.fill(data, offset + HEADER_SIZE, offset + size, (byte) 0);
        if (writeToBuffer(metaData, data, offset, size) != size - HEADER_SIZE)
        {
            throw new StoreException("Message metadata serializer did not write its declared size");
        }
    }

    private static int writeToBuffer(final StorableMessageMetaData metaData, final byte[] data,
                                     final int offset, final int size)
    {
        final ByteBuffer value = ByteBuffer.wrap(data, offset, size);
        value.putInt((size - Integer.BYTES) ^ Integer.MIN_VALUE);
        value.put((byte) metaData.getType().ordinal());

        try (final QpidByteBuffer payload = QpidByteBuffer.wrap(value.slice()))
        {
            metaData.writeToBuffer(payload);
            return payload.position();
        }
    }
}
