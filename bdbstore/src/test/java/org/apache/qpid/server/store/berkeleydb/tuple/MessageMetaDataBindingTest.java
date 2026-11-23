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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import com.sleepycat.je.DatabaseEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.store.MessageMetaDataTypeRegistry;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.test.utils.UnitTestBase;

class MessageMetaDataBindingTest extends UnitTestBase
{
    private static final MessageMetaDataBinding BINDING = MessageMetaDataBinding.getInstance();
    private static final byte TYPE_ORDINAL = 42;

    private boolean _initialisedPool;

    @BeforeEach
    void setUp()
    {
        if (QpidByteBuffer.getPooledBufferSize() <= 0)
        {
            QpidByteBuffer.initialisePool(1024, 16, 0.5);
            _initialisedPool = true;
        }
    }

    @AfterEach
    void tearDown()
    {
        if (_initialisedPool)
        {
            QpidByteBuffer.deinitialisePool();
        }
    }

    @Test
    void testEntryWithMissingData()
    {
        final String expected = "Cannot decode message metadata: missing entry data";
        assertEquals(expected, assertThrows(StoreException.class, () -> BINDING.entryToObject(null)).getMessage());
        assertEquals(expected,
                     assertThrows(StoreException.class, () -> BINDING.entryToObject(new DatabaseEntry())).getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 4})
    void testEntryWithIncompleteHeader(final int size)
    {
        final DatabaseEntry entry = new DatabaseEntry(new byte[4]);
        entry.setSize(size);
        final StoreException exception = assertThrows(StoreException.class, () -> BINDING.entryToObject(entry));
        assertEquals("Cannot decode message metadata: entry is too short (size %d)".formatted(size),
                     exception.getMessage());
    }

    @ParameterizedTest
    @CsvSource({"-1, 6", "1, 6", "2147483647, 6", "4, 2147483647"})
    void testEntryWithInvalidBounds(final int offset, final int size)
    {
        final DatabaseEntry entry = new DatabaseEntry(new byte[] {(byte) 0x80, 0, 0, 2, TYPE_ORDINAL, 0x11});
        entry.setOffset(offset);
        entry.setSize(size);
        final StoreException exception = assertThrows(StoreException.class, () -> BINDING.entryToObject(entry));
        assertEquals("Cannot decode message metadata: invalid entry bounds (size %d)".formatted(size),
                     exception.getMessage());
    }

    @Test
    void testUnknownOrdinalReportsUnsignedType()
    {
        final DatabaseEntry entry = new DatabaseEntry(new byte[] {(byte) 0x80, 0, 0, 2, (byte) 0xff, 0x11});
        try (final MockedStatic<MessageMetaDataTypeRegistry> registry = mockStatic(MessageMetaDataTypeRegistry.class))
        {
            registry.when(() -> MessageMetaDataTypeRegistry.fromOrdinal(255))
                    .thenThrow(new IllegalArgumentException("Registry detail"));
            final StoreException exception = assertThrows(StoreException.class, () -> BINDING.entryToObject(entry));
            assertEquals("Cannot decode message metadata (type 255, size 6, IllegalArgumentException)",
                         exception.getMessage());
            assertNull(exception.getCause());
        }
    }

    @Test
    void testDecodeReportsOnlyMetadataTypeSizeAndExceptionType()
    {
        final DatabaseEntry entry = new DatabaseEntry(new byte[] {(byte) 0x80, 0, 0, 2, TYPE_ORDINAL, 0x11});
        final MessageMetaDataType<?> type = mock(MessageMetaDataType.class);
        when(type.createMetaData(any(QpidByteBuffer.class))).thenThrow(new IllegalArgumentException("Codec detail"));

        try (final MockedStatic<MessageMetaDataTypeRegistry> registry = mockStatic(MessageMetaDataTypeRegistry.class))
        {
            registry.when(() -> MessageMetaDataTypeRegistry.fromOrdinal(TYPE_ORDINAL)).thenReturn(type);
            final StoreException exception = assertThrows(StoreException.class, () -> BINDING.entryToObject(entry));
            assertEquals("Cannot decode message metadata (type 42, size 6, IllegalArgumentException)",
                         exception.getMessage());
            assertNull(exception.getCause());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 13})
    void testEntrySliceGivesDecoderOnlyTheZeroBasedPayload(final int offset)
    {
        // Legacy decoding consumes the size field and uses the JE entry's bounds for the payload.
        final byte[] record = {(byte) 0x80, 0, 0, 1, TYPE_ORDINAL, 0x11, 0x22, 0x33};
        final byte[] backing = new byte[offset + record.length + 7];
        Arrays.fill(backing, (byte) 0x5a);
        System.arraycopy(record, 0, backing, offset, record.length);
        final DatabaseEntry entry = new DatabaseEntry(backing, offset, record.length);
        final MessageMetaDataType<?> type = mock(MessageMetaDataType.class);
        final StorableMessageMetaData decoded = mock(StorableMessageMetaData.class);
        when(type.createMetaData(any(QpidByteBuffer.class))).thenAnswer(invocation ->
        {
            final QpidByteBuffer payload = invocation.getArgument(0);
            assertEquals(0, payload.position());
            assertEquals(3, payload.remaining());
            final byte[] actual = new byte[3];
            payload.copyTo(actual);
            assertArrayEquals(new byte[] {0x11, 0x22, 0x33}, actual);
            return decoded;
        });

        try (final MockedStatic<MessageMetaDataTypeRegistry> registry = mockStatic(MessageMetaDataTypeRegistry.class))
        {
            registry.when(() -> MessageMetaDataTypeRegistry.fromOrdinal(TYPE_ORDINAL)).thenReturn(type);
            assertSame(decoded, BINDING.entryToObject(entry));
        }
    }
}
