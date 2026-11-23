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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.BufferOverflowException;
import java.util.Arrays;
import java.util.function.Consumer;

import com.sleepycat.je.DatabaseEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.internal.InternalMessageMetaData;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.test.utils.UnitTestBase;

class BorrowedMessageMetaDataBindingTest extends UnitTestBase
{
    private static final MessageMetaDataBinding BINDING = MessageMetaDataBinding.getInstance();

    @ParameterizedTest
    @ValueSource(ints = {-1, Integer.MAX_VALUE, Integer.MAX_VALUE - Integer.BYTES})
    void testInvalidSerializedSizeIsRejected(final int size)
    {
        final StorableMessageMetaData metadata = metadata(size, buffer -> buffer.put((byte) 1));
        assertThrows(StoreException.class, () -> BINDING.getEntrySize(metadata));
    }

    @Test
    void testUnderwriteCannotPersistStalePayload()
    {
        final StorableMessageMetaData metadata = metadata(4, buffer -> buffer.put((byte) 1));
        final byte[] scratch = new byte[100];
        Arrays.fill(scratch, (byte) 42);
        final DatabaseEntry borrowed = new DatabaseEntry(scratch, 10, 9);

        assertThrows(StoreException.class, () -> BINDING.writeToEntry(metadata, borrowed));
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
                BINDING.writeToEntry(metadata, new DatabaseEntry(scratch, 10, 9)));
        assertEquals(42, scratch[9]);
        assertEquals(42, scratch[19]);
    }

    @Test
    void testBorrowedEntryUsesLogicalRangeAndClearsPartialFlag()
    {
        final StorableMessageMetaData metadata = metadata(4, buffer -> buffer.putInt(0x01020304));
        final byte[] scratch = new byte[30];
        Arrays.fill(scratch, (byte) 42);
        final int offset = 10;
        final int size = BINDING.getEntrySize(metadata);
        final DatabaseEntry borrowed = new DatabaseEntry(scratch, offset, size);
        borrowed.setPartial(1, 2, true);

        BINDING.writeToEntry(metadata, borrowed);

        final byte[] expected = new byte[scratch.length];
        Arrays.fill(expected, (byte) 42);
        final byte[] record = {(byte) 0x80, 0, 0, 5, (byte) metadata.getType().ordinal(), 1, 2, 3, 4};
        System.arraycopy(record, 0, expected, offset, record.length);
        assertArrayEquals(expected, scratch);
        assertSame(scratch, borrowed.getData());
        assertEquals(offset, borrowed.getOffset());
        assertEquals(size, borrowed.getSize());
        assertFalse(borrowed.getPartial());
    }

    @Test
    void testOrdinaryBindingReturnsIndependentEntries()
    {
        final DatabaseEntry first = new DatabaseEntry();
        final DatabaseEntry second = new DatabaseEntry();
        BINDING.objectToEntry(metadata(1, buffer -> buffer.put((byte) 1)), first);
        BINDING.objectToEntry(metadata(1, buffer -> buffer.put((byte) 2)), second);

        assertNotSame(first.getData(), second.getData());
        assertEquals(1, first.getData()[first.getSize() - 1]);
        assertEquals(2, second.getData()[second.getSize() - 1]);
    }

    @Test
    void testMissingBorrowedDataIsRejected()
    {
        assertThrows(IllegalArgumentException.class, () ->
                BINDING.writeToEntry(metadata(4, buffer -> buffer.putInt(1)), new DatabaseEntry()));
    }

    @ParameterizedTest
    @CsvSource({"-1, 9", "1, 9", "0, 4", "2147483647, 9", "4, 2147483647"})
    void testInvalidBorrowedRangeIsRejected(final int offset, final int size)
    {
        final DatabaseEntry borrowed = new DatabaseEntry(new byte[9]);
        borrowed.setOffset(offset);
        borrowed.setSize(size);
        assertThrows(IllegalArgumentException.class, () ->
                BINDING.writeToEntry(metadata(4, buffer -> buffer.putInt(1)), borrowed));
    }

    private static StorableMessageMetaData metadata(final int size, final Consumer<QpidByteBuffer> writer)
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
