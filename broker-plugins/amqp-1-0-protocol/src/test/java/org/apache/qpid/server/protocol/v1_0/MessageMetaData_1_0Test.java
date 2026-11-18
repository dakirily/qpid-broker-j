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
package org.apache.qpid.server.protocol.v1_0;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.test.utils.UnitTestBase;

class MessageMetaData_1_0Test extends UnitTestBase
{
    @ParameterizedTest
    @MethodSource("legacyBodies")
    void testLegacyMetadataRetainsExactSectionEncodings(final byte[] body)
    {
        // An empty list can also be encoded as LIST0. Keep this original LIST8 representation.
        final byte[] header = {0, 0x53, 0x70, (byte) 0xc0, 1, 0};
        final byte[] deliveryAnnotations = emptyMapSection(0x71);
        final byte[] messageAnnotations = emptyMapSection(0x72);
        final byte[] properties = {0, 0x53, 0x73, 0x45};
        final byte[] applicationProperties = emptyMapSection(0x74);
        final byte[] footer = emptyMapSection(0x78);
        final int metadataSize = header.length + deliveryAnnotations.length + messageAnnotations.length +
                properties.length + applicationProperties.length + footer.length;

        final int split = 2;
        final ByteBuffer tail = ByteBuffer.allocateDirect(metadataSize + body.length - split);
        tail.put(header, split, header.length - split).put(deliveryAnnotations).put(messageAnnotations)
                .put(properties).put(applicationProperties).put(body).put(footer).flip();
        final MessageMetaData_1_0 metadata;
        try (final QpidByteBuffer first = QpidByteBuffer.wrap(header, 0, split);
             final QpidByteBuffer second = QpidByteBuffer.wrap(tail);
             final QpidByteBuffer input = QpidByteBuffer.concatenate(List.of(first, second)))
        {
            metadata = MessageMetaData_1_0.FACTORY.createMetaData(input);
        }

        try
        {
            assertEquals(0, metadata.getVersion());
            assertEquals(body.length, metadata.getContentSize());
            assertEquals(17 + metadataSize, metadata.getStorableSize());
            assertSectionEncoding(header, metadata.getHeaderSection());
            assertSectionEncoding(deliveryAnnotations, metadata.getDeliveryAnnotationsSection());
            assertSectionEncoding(messageAnnotations, metadata.getMessageAnnotationsSection());
            assertSectionEncoding(properties, metadata.getPropertiesSection());
            assertSectionEncoding(applicationProperties, metadata.getApplicationPropertiesSection());
            assertSectionEncoding(footer, metadata.getFooterSection());
        }
        finally
        {
            metadata.dispose();
        }
    }

    private static List<byte[]> legacyBodies()
    {
        return List.of(new byte[]{0, 0x53, 0x75, (byte) 0xa0, 1, 42, 0, 0x53, 0x75, (byte) 0xa0, 0}, // two Data
                       new byte[]{0, 0x53, 0x76, 0x45, 0, 0x53, 0x76, 0x45}, // two AMQP Sequences
                       new byte[]{0, 0x53, 0x77, 0x41}, // AMQP Value
                       new byte[0]);
    }

    private static byte[] emptyMapSection(final int descriptor)
    {
        return new byte[]{0, 0x53, (byte) descriptor, (byte) 0xc1, 1, 0};
    }

    private static void assertSectionEncoding(final byte[] expected, final EncodingRetainingSection<?> section)
    {
        try (final QpidByteBuffer encoding = section.getEncodedForm())
        {
            assertTrue(encoding.hasArray());
            assertEquals(expected.length, encoding.remaining());
            assertArrayEquals(expected, encoding.array());
        }
        finally
        {
            section.dispose();
        }
    }
}
