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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.BufferOverflowException;
import java.util.Arrays;
import java.util.function.Consumer;

import com.sleepycat.je.DatabaseEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.test.utils.UnitTestBase;

class BorrowedMessageMetaDataBindingTest extends UnitTestBase
{
    private final MessageMetaDataBinding _binding = MessageMetaDataBinding.getInstance();

    @ParameterizedTest
    @ValueSource(ints = {-1, Integer.MAX_VALUE, Integer.MAX_VALUE - 4})
    void testInvalidSerializedSizeIsRejected(final int size)
    {
        assertThrows(StoreException.class, () -> _binding.getEntrySize(metadata(size, buffer ->
        {
            // size validation must reject the record before serialization
        })));
    }

    @Test
    void testUnderwriteCannotPersistStalePayload()
    {
        final byte[] original = new byte[]{99};
        final DatabaseEntry independent = new DatabaseEntry(original);
        final StorableMessageMetaData metadata = metadata(4, buffer -> buffer.put((byte) 1));
        assertThrows(StoreException.class, () -> _binding.objectToEntry(metadata, independent));
        assertSame(original, independent.getData());
        final DatabaseEntry borrowed = new DatabaseEntry(new byte[100], 10, 9);
        assertThrows(StoreException.class, () -> _binding.writeToEntry(metadata, borrowed));
    }

    @Test
    void testSerializerCannotExpandIntoOtherScratchBytes()
    {
        final byte[] scratch = new byte[100];
        Arrays.fill(scratch, (byte) 42);
        final StorableMessageMetaData metadata = metadata(4, buffer ->
        {
            assertEquals(0, buffer.position());
            assertEquals(4, buffer.capacity());
            buffer.clear();
            buffer.put(new byte[5]);
        });
        assertThrows(BufferOverflowException.class, () ->
                _binding.writeToEntry(metadata, new DatabaseEntry(scratch, 10, 9)));
        assertEquals(42, scratch[9]);
        assertEquals(42, scratch[19]);
    }

    @Test
    void testInvalidBorrowedRangeIsRejected()
    {
        final StorableMessageMetaData metadata = metadata(4, buffer -> buffer.putInt(1));
        assertThrows(IllegalArgumentException.class, () ->
                _binding.writeToEntry(metadata, new DatabaseEntry()));
        assertThrows(IllegalArgumentException.class, () ->
                _binding.writeToEntry(metadata, new DatabaseEntry(new byte[9], 1, 9)));
        assertThrows(IllegalArgumentException.class, () ->
                _binding.writeToEntry(metadata, new DatabaseEntry(new byte[9], 0, 4)));
    }

    private StorableMessageMetaData metadata(final int size, final Consumer<QpidByteBuffer> writer)
    {
        return new InternalMessageMetaData(true, null, 0)
        {
            @Override
            public int getStorableSize()
            {
                return size;
            }

            @Override
            public void writeToBuffer(final QpidByteBuffer buffer)
            {
                writer.accept(buffer);
            }
        };
    }
}
