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

package org.apache.qpid.server.store.berkeleydb;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseEntry;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.store.berkeleydb.tuple.QueueEntryBinding;

/** A thread-confined input buffer borrowed until the complete database operation has finished. */
final class BDBWriteBuffer implements AutoCloseable
{
    static final int MAX_RETAINED_CAPACITY = 64 * 1024;

    private static final byte[] EMPTY = new byte[0];
    private static final ThreadLocal<BDBWriteBuffer> WRITE_BUFFER = ThreadLocal.withInitial(BDBWriteBuffer::new);

    private final DatabaseEntry _key = new DatabaseEntry(new byte[Long.BYTES]);
    private final DatabaseEntry _value = new DatabaseEntry();
    private byte[] _buffer = EMPTY;
    private ByteBuffer _copyBuffer = ByteBuffer.wrap(EMPTY);
    private boolean _inUse;

    private BDBWriteBuffer()
    {
        // utility class has private constructor
    }

    static BDBWriteBuffer acquire()
    {
        final BDBWriteBuffer cached = WRITE_BUFFER.get();
        final BDBWriteBuffer buffer = cached._inUse ? new BDBWriteBuffer() : cached;
        buffer._inUse = true;
        return buffer;
    }

    void prepare(final long messageId, final int size)
    {
        if (size < 0)
        {
            throw new IllegalArgumentException("Write size must not be negative");
        }

        final byte[] data;
        if (size > MAX_RETAINED_CAPACITY)
        {
            data = new byte[size];
        }
        else
        {
            if (_buffer.length < size)
            {
                _buffer = new byte[Math.min(MAX_RETAINED_CAPACITY, Math.max(size, _buffer.length * 2))];
                _copyBuffer = ByteBuffer.wrap(_buffer);
            }
            data = _buffer;
        }

        QueueEntryBinding.writeMessageId(messageId, _key);
        _key.setPartial(0, 0, false);
        _key.setData(_key.getData(), 0, Long.BYTES);
        _value.setPartial(0, 0, false);
        _value.setData(data, 0, size);
    }

    void copyContent(final QpidByteBuffer content)
    {
        final byte[] data = _value.getData();
        if (data.length == _value.getSize())
        {
            content.copyTo(data);
        }
        else
        {
            _copyBuffer.clear().limit(_value.getSize());
            content.copyTo(_copyBuffer);
        }
    }

    DatabaseEntry getKey()
    {
        return _key;
    }

    DatabaseEntry getValue()
    {
        return _value;
    }

    @Override
    public void close()
    {
        _value.setData(EMPTY);
        _inUse = false;
    }
}
