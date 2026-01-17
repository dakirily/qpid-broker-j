/*
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
 */

package org.apache.qpid.server.store.berkeleydb.tuple;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
import java.util.Map;

import com.sleepycat.je.DatabaseEntry;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.MessageMetaData_0_10;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryPriority;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.MessageMetaData;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v1_0.MessageMetaData_1_0;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedByte;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationPropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.store.MessageMetaDataTypeRegistry;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.junit.jupiter.api.Test;

class MessageMetaDataBindingTest
{
    private final MessageMetaDataBinding binding = MessageMetaDataBinding.getInstance();

    @Test
    void roundTripObjectToEntryThenEntryToObjectPreservesPayloadBytesAmqp_0_8()
    {
        final StorableMessageMetaData original = createAmqp_0_8_Metadata();
        final byte[] expectedPayload = bytes(original);
        try
        {
            final DatabaseEntry entry = new DatabaseEntry();
            binding.objectToEntry(original, entry);

            final StorableMessageMetaData result = binding.entryToObject(entry);
            try
            {
                assertEquals(original.getType(), result.getType());
                assertArrayEquals(expectedPayload, bytes(result));
            }
            finally
            {
                result.dispose();
            }
        }
        finally
        {
            original.dispose();
        }
    }

    @Test
    void roundTripObjectToEntryThenEntryToObjectPreservesPayloadBytesAmqp_0_10()
    {
        final StorableMessageMetaData original = createAmqp_0_10_Metadata();
        final byte[] expectedPayload = bytes(original);
        try
        {
            final DatabaseEntry entry = new DatabaseEntry();
            binding.objectToEntry(original, entry);

            final StorableMessageMetaData result = binding.entryToObject(entry);
            try
            {
                assertEquals(original.getType(), result.getType());
                assertArrayEquals(expectedPayload, bytes(result));
            }
            finally
            {
                result.dispose();
            }
        }
        finally
        {
            original.dispose();
        }
    }

    @Test
    void roundTripObjectToEntryThenEntryToObjectPreservesPayloadBytesAmqp_1_0()
    {
        final StorableMessageMetaData original = createAmqp_1_0_Metadata();
        final byte[] expectedPayload = bytes(original);
        try
        {
            final DatabaseEntry entry = new DatabaseEntry();
            binding.objectToEntry(original, entry);

            final StorableMessageMetaData result = binding.entryToObject(entry);
            try
            {
                assertEquals(original.getType(), result.getType());
                assertArrayEquals(expectedPayload, bytes(result));
            }
            finally
            {
                result.dispose();
            }
        }
        finally
        {
            original.dispose();
        }
    }

    @Test
    void objectToEntryAmqp_0_8()
    {
        final StorableMessageMetaData metaData = createAmqp_0_8_Metadata();
        try
        {
            final DatabaseEntry entry = new DatabaseEntry(new byte[metaData.getStorableSize() + 5]);
            binding.objectToEntry(metaData, entry);
            checkSizeAndType(metaData, entry);
        }
        finally
        {
            metaData.dispose();
        }
    }

    @Test
    void objectToEntryAmqp_0_10()
    {
        final StorableMessageMetaData metaData = createAmqp_0_10_Metadata();
        try
        {
            final DatabaseEntry entry = new DatabaseEntry(new byte[metaData.getStorableSize() + 5]);
            binding.objectToEntry(metaData, entry);
            checkSizeAndType(metaData, entry);
        }
        finally
        {
            metaData.dispose();
        }
    }

    @Test
    void objectToEntryAmqp_1_0()
    {
        final StorableMessageMetaData metaData = createAmqp_1_0_Metadata();
        try
        {
            final DatabaseEntry entry = new DatabaseEntry(new byte[metaData.getStorableSize() + 5]);
            binding.objectToEntry(metaData, entry);
            checkSizeAndType(metaData, entry);
        }
        finally
        {
            metaData.dispose();
        }
    }

    @Test
    void entryToObjectWithNullData()
    {
        final byte[] invalidData = null;
        final DatabaseEntry invalidEntry = new DatabaseEntry(invalidData);
        assertThrows(StoreException.class, () -> binding.entryToObject(invalidEntry));
    }

    @Test
    void entryToObjectWithSmallData()
    {
        final byte[] invalidData = new byte[4];
        final DatabaseEntry invalidEntry = new DatabaseEntry(invalidData);
        assertThrows(StoreException.class, () -> binding.entryToObject(invalidEntry));
    }

    @Test
    void entryToObjectWithNonZeroOffsetReadsCorrectly()
    {
        final StorableMessageMetaData original = createAmqp_0_10_Metadata();
        final byte[] expectedPayload = bytes(original);
        try
        {
            final DatabaseEntry storedEntry = new DatabaseEntry();
            binding.objectToEntry(original, storedEntry);

            final DatabaseEntry entry = entryWithOffsetAndSize(storedEntry.getData(), 13, storedEntry.getData().length);
            final StorableMessageMetaData result = binding.entryToObject(entry);
            try
            {
                assertEquals(original.getType(), result.getType());
                assertArrayEquals(expectedPayload, bytes(result));
            }
            finally
            {
                result.dispose();
            }
        }
        finally
        {
            original.dispose();
        }
    }

    @Test
    void entryToObjectWithSizeSmallerThanDataLengthUsesEntrySizeNotArrayLength()
    {
        final StorableMessageMetaData original = createAmqp_0_10_Metadata();
        final byte[] expectedPayload = bytes(original);
        try
        {
            final DatabaseEntry storedEntry = new DatabaseEntry();
            binding.objectToEntry(original, storedEntry);
            final byte[] stored = storedEntry.getData();

            // Backing array is larger than entry size. The binding must honour entry.getSize().
            final byte[] backing = new byte[stored.length + 32];
            Arrays.fill(backing, (byte) 0x5A);
            System.arraycopy(stored, 0, backing, 0, stored.length);

            final DatabaseEntry entry = new DatabaseEntry();
            entry.setData(backing, 0, stored.length);

            final StorableMessageMetaData result = binding.entryToObject(entry);
            try
            {
                assertEquals(original.getType(), result.getType());
                assertArrayEquals(expectedPayload, bytes(result));
            }
            finally
            {
                result.dispose();
            }
        }
        finally
        {
            original.dispose();
        }
    }

    @Test
    void entryToObjectWithOffsetPlusSizeBeyondArrayLengthThrowsStoreException()
    {
        final StorableMessageMetaData original = createAmqp_1_0_Metadata();
        try
        {
            final DatabaseEntry entry = new DatabaseEntry();
            binding.objectToEntry(original, entry);

            entry.setOffset(1);
            entry.setSize(entry.getData().length);

            assertThrows(StoreException.class, () -> binding.entryToObject(entry));
        }
        finally
        {
            original.dispose();
        }
    }

    @Test
    void entryToObjectWithTruncatedPayloadThrowsStoreException()
    {
        final StorableMessageMetaData original = createAmqp_0_10_Metadata();
        try
        {
            final DatabaseEntry storedEntry = new DatabaseEntry();
            binding.objectToEntry(original, storedEntry);

            final byte[] stored = storedEntry.getData();
            final DatabaseEntry truncated = new DatabaseEntry();
            truncated.setData(stored, 0, stored.length - 1);

            assertThrows(StoreException.class, () -> binding.entryToObject(truncated));
        }
        finally
        {
            original.dispose();
        }
    }

    @Test
    void entryToObjectWithUnregisteredTypeOrdinalThrowsStoreException()
    {
        final Integer unregisteredOrdinal = findUnregisteredOrdinal();
        assumeTrue(unregisteredOrdinal != null, "No unregistered meta-data type ordinals are present");

        final StorableMessageMetaData original = createAmqp_0_10_Metadata();
        try
        {
            final DatabaseEntry entry = new DatabaseEntry();
            binding.objectToEntry(original, entry);

            // Force an unregistered type ordinal while keeping the rest of the record valid.
            entry.getData()[4] = (byte) (unregisteredOrdinal & 0xFF);

            assertThrows(StoreException.class, () -> binding.entryToObject(entry));
        }
        finally
        {
            original.dispose();
        }
    }

    @Test
    void entryToObjectWithTypeOrdinalOutOfRangeIsWrappedIntoStoreException()
    {
        final int maxOrdinal = maxRegisteredMetaDataTypeOrdinal();

        final Integer invalidOrdinal;

        if (maxOrdinal < 255)
        {
            invalidOrdinal = maxOrdinal + 1;
        }
        else
        {
            invalidOrdinal = findUnregisteredOrdinal();
        }

        assumeTrue(invalidOrdinal != null, "No representable invalid meta-data type ordinal is available");

        final StorableMessageMetaData original = createAmqp_0_10_Metadata();
        try
        {
            final DatabaseEntry entry = new DatabaseEntry();
            binding.objectToEntry(original, entry);

            // This test documents the expected contract: invalid type ordinals should be wrapped into StoreException.
            entry.getData()[4] = (byte) (invalidOrdinal & 0xFF);

            assertThrows(StoreException.class, () -> binding.entryToObject(entry));
        }
        finally
        {
            original.dispose();
        }
    }

    @Test
    void objectToEntryWritesFlippedBodySizeAndTypeAndPayloadLengthMatches()
    {
        final StorableMessageMetaData metaData = createAmqp_0_10_Metadata();
        try
        {
            final DatabaseEntry entry = new DatabaseEntry();
            binding.objectToEntry(metaData, entry);

            checkSizeAndType(metaData, entry);

            final byte[] data = entry.getData();
            assertEquals(5 + metaData.getStorableSize(), data.length);

            final byte[] expectedPayload = bytes(metaData);
            final byte[] actualPayload = Arrays.copyOfRange(data, 5, data.length);
            assertArrayEquals(expectedPayload, actualPayload);
        }
        finally
        {
            metaData.dispose();
        }
    }

    private DatabaseEntry entryWithOffsetAndSize(final byte[] stored, final int offset, final int size)
    {
        final byte[] backing = new byte[offset + size + 7];
        Arrays.fill(backing, (byte) 0x5A);
        System.arraycopy(stored, 0, backing, offset, size);

        final DatabaseEntry entry = new DatabaseEntry();
        entry.setData(backing, offset, size);
        return entry;
    }

    private StorableMessageMetaData createAmqp_0_8_Metadata()
    {
        final AMQShortString routingKey = AMQShortString.valueOf("routingkey");
        final AMQShortString exchange = AMQShortString.valueOf("exchange");
        final MessagePublishInfo publishBody = new MessagePublishInfo(exchange, false, false, routingKey);
        final BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setContentType("content/type");
        final ContentHeaderBody contentHeaderBody = new ContentHeaderBody(props);
        return new MessageMetaData(publishBody, contentHeaderBody, System.currentTimeMillis());
    }

    private StorableMessageMetaData createAmqp_0_10_Metadata()
    {
        final DeliveryProperties deliveryProperties = new DeliveryProperties();
        deliveryProperties.setPriority(MessageDeliveryPriority.get((short) 1));
        final MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType("content/type");
        messageProperties.setApplicationHeaders(Map.of("key", "value"));
        final Header header = new Header(deliveryProperties, messageProperties);
        try (final QpidByteBuffer content = QpidByteBuffer.wrap("content/type".getBytes(UTF_8)))
        {
            return new MessageMetaData_0_10(header, content.remaining(), System.currentTimeMillis());
        }
    }

    private StorableMessageMetaData createAmqp_1_0_Metadata()
    {
        try (final QpidByteBuffer content = new AmqpValue("test").createEncodingRetainingSection().getEncodedForm())
        {
            final long contentSize = content.remaining();
            final org.apache.qpid.server.protocol.v1_0.type.messaging.Header header =
                    new org.apache.qpid.server.protocol.v1_0.type.messaging.Header();
            header.setPriority(UnsignedByte.valueOf((byte) 1));
            final HeaderSection headerSection = header.createEncodingRetainingSection();
            final Properties properties = new Properties();
            properties.setContentType(Symbol.valueOf("content/type"));
            final PropertiesSection propertiesSection = properties.createEncodingRetainingSection();
            final ApplicationPropertiesSection applicationPropertiesSection =
                    new ApplicationProperties(Map.of("key", "value")).createEncodingRetainingSection();
            return new MessageMetaData_1_0(headerSection, null, null, propertiesSection, applicationPropertiesSection,
                    null, System.currentTimeMillis(), contentSize);
        }
    }

    private byte[] bytes(final StorableMessageMetaData metaData)
    {
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(metaData.getStorableSize()))
        {
            metaData.writeToBuffer(buffer);
            return buffer.array();
        }
    }

    private void checkSizeAndType(final StorableMessageMetaData metaData, final DatabaseEntry entry)
    {
        final byte[] data = entry.getData();

        final int expectedBodySize = 1 + metaData.getStorableSize();
        final int actualBodySize = (((data[0] & 0xFF) << 24)
                | ((data[1] & 0xFF) << 16)
                | ((data[2] & 0xFF) << 8)
                | (data[3] & 0xFF)) ^ 0x80000000;

        assertEquals(expectedBodySize, actualBodySize);
        assertEquals(metaData.getType().ordinal(), data[4] & 0xFF);
    }

    private int maxRegisteredMetaDataTypeOrdinal()
    {
        int ordinal = 0;
        while (true)
        {
            try
            {
                MessageMetaDataTypeRegistry.fromOrdinal(ordinal);
                ordinal++;
            }
            catch (ArrayIndexOutOfBoundsException e)
            {
                return ordinal - 1;
            }
        }
    }

    private Integer findUnregisteredOrdinal()
    {
        final int max = maxRegisteredMetaDataTypeOrdinal();
        for (int ordinal = 0; ordinal <= max; ordinal++)
        {
            if (MessageMetaDataTypeRegistry.fromOrdinal(ordinal) == null)
            {
                return ordinal;
            }
        }
        return null;
    }
}
