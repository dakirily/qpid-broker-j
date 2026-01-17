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

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class ByteBufferBinding extends TupleBinding<QpidByteBuffer>
{
    private static final int COPY_BUFFER_SIZE = 8192;

    private static final ThreadLocal<byte[]> COPY_BUFFER = ThreadLocal.withInitial(() -> new byte[COPY_BUFFER_SIZE]);

    private static final ByteBufferBinding INSTANCE = new ByteBufferBinding();

    public static ByteBufferBinding getInstance()
    {
        return INSTANCE;
    }

    /** private constructor forces getInstance instead */
    private ByteBufferBinding() { }

    @Override
    public QpidByteBuffer entryToObject(final TupleInput input)
    {
        int available = input.available();
        final QpidByteBuffer buf = QpidByteBuffer.allocateDirect(available);
        final byte[] copyBuf = COPY_BUFFER.get();
        while (available > 0)
        {
            final int read = input.read(copyBuf);
            buf.put(copyBuf,0,read);
            available = input.available();
        }
        buf.flip();
        return buf;
    }

    @Override
    public void objectToEntry(final QpidByteBuffer data, final TupleOutput output)
    {
        try (final QpidByteBuffer dup = data.duplicate())
        {
            final byte[] copyBuf = COPY_BUFFER.get();
            while (dup.hasRemaining())
            {
                final int length = Math.min(COPY_BUFFER_SIZE, dup.remaining());
                dup.get(copyBuf, 0, length);
                output.write(copyBuf, 0, length);
            }
        }
    }

    public ByteBuffer readByteBuffer(final TupleInput input, int length)
    {
        final ByteBuffer buf = ByteBuffer.allocateDirect(length);
        final byte[] copyBuf = COPY_BUFFER.get();
        while (length > 0)
        {
            final int read = input.read(copyBuf, 0, Math.min(COPY_BUFFER_SIZE, length));
            buf.put(copyBuf,0,read);
            length -= read;
        }
        buf.flip();
        return buf;
    }
}
