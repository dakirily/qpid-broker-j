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

package org.apache.qpid.server.store.berkeleydb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.qpid.server.model.ConfiguredObject;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.store.StorableMessageMetaData;

/**
 * Unit tests for the content fragment accumulation and consolidation logic within
 * {@link AbstractBDBMessageStore.StoredBDBMessage}.
 */
class StoredBDBMessageTest
{
    @Test
    void addContentAccumulatesFragmentsAndConsolidatesOnGetContent() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        final TestMetaData metaData = new TestMetaData(3, 6);

        final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, false);

        try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2});
             QpidByteBuffer f2 = QpidByteBuffer.wrap(new byte[]{3, 4, 5, 6}))
        {
            message.addContent(f1);
            message.addContent(f2);
        }

        // Verify that content is accumulated as fragments before consolidation.
        assertEquals(2, fragmentCount(message));
        assertNull(getMessageData(message));

        try (QpidByteBuffer content = message.getContent(0, Integer.MAX_VALUE))
        {
            final byte[] actual = new byte[content.remaining()];
            content.get(actual);
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6}, actual);
        }

        // Verify that getContent consolidates the fragments into the message data.
        assertEquals(0, fragmentCount(message));
        assertNotNull(getMessageData(message));
    }

    @Test
    void allContentAddedConsolidatesAndUpdatesInMemorySize() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        final int metadataSize = 11;
        final int contentSize = 5;
        final TestMetaData metaData = new TestMetaData(metadataSize, contentSize);

        final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, false);

        assertEquals(metadataSize, store.getInMemorySize());

        try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2, 3});
             QpidByteBuffer f2 = QpidByteBuffer.wrap(new byte[]{4, 5}))
        {
            message.addContent(f1);
            message.addContent(f2);
        }

        message.allContentAdded();

        assertEquals(metadataSize + contentSize, store.getInMemorySize());

        try (QpidByteBuffer content = message.getContent(0, Integer.MAX_VALUE))
        {
            final byte[] actual = new byte[content.remaining()];
            content.get(actual);
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, actual);
        }

        assertEquals(0, fragmentCount(message));
        assertNotNull(getMessageData(message));
    }

    @Test
    void reallocateConsolidatesFragments() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        final TestMetaData metaData = new TestMetaData(3, 4);

        final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, false);

        try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2});
             QpidByteBuffer f2 = QpidByteBuffer.wrap(new byte[]{3, 4}))
        {
            message.addContent(f1);
            message.addContent(f2);
        }

        assertEquals(2, fragmentCount(message));
        assertNull(getMessageData(message));

        message.reallocate();

        assertEquals(0, fragmentCount(message));
        assertNotNull(getMessageData(message));

        try (QpidByteBuffer content = message.getContent(0, Integer.MAX_VALUE))
        {
            final byte[] actual = new byte[content.remaining()];
            content.get(actual);
            assertArrayEquals(new byte[]{1, 2, 3, 4}, actual);
        }
    }

    @Test
    void addContentAfterConsolidationPreservesExistingContent() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        final int metadataSize = 3;
        final int contentSize = 8;
        final TestMetaData metaData = new TestMetaData(metadataSize, contentSize);

        final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, false);

        try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2});
             QpidByteBuffer f2 = QpidByteBuffer.wrap(new byte[]{3, 4}))
        {
            message.addContent(f1);
            message.addContent(f2);
        }

        // Force an early consolidation.
        try (QpidByteBuffer partial = message.getContent(0, 4))
        {
            final byte[] actual = new byte[partial.remaining()];
            partial.get(actual);
            assertArrayEquals(new byte[]{1, 2, 3, 4}, actual);
        }

        // Add more content - this must preserve the already consolidated data.
        try (QpidByteBuffer f3 = QpidByteBuffer.wrap(new byte[]{5, 6});
             QpidByteBuffer f4 = QpidByteBuffer.wrap(new byte[]{7, 8}))
        {
            message.addContent(f3);
            message.addContent(f4);
        }

        // Existing data is expected to be moved into the fragment list alongside new fragments.
        assertEquals(3, fragmentCount(message));
        assertNull(getMessageData(message));

        message.allContentAdded();

        assertEquals(metadataSize + contentSize, store.getInMemorySize());

        try (QpidByteBuffer content = message.getContent(0, Integer.MAX_VALUE))
        {
            final byte[] actual = new byte[content.remaining()];
            content.get(actual);
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, actual);
        }
    }

    private static int fragmentCount(final Object message) throws Exception
    {
        final Field fragmentsField = message.getClass().getDeclaredField("_contentFragments");
        fragmentsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        final List<QpidByteBuffer> fragments = (List<QpidByteBuffer>) fragmentsField.get(message);
        return fragments == null ? 0 : fragments.size();
    }

    private static QpidByteBuffer getMessageData(final Object message) throws Exception
    {
        final Field messageDataRefField = message.getClass().getDeclaredField("_messageDataRef");
        messageDataRefField.setAccessible(true);
        final Object messageDataRef = messageDataRefField.get(message);
        if (messageDataRef == null)
        {
            return null;
        }

        final Method getData = messageDataRef.getClass().getDeclaredMethod("getData");
        getData.setAccessible(true);
        return (QpidByteBuffer) getData.invoke(messageDataRef);
    }

    private static final class TestBDBMessageStore extends AbstractBDBMessageStore
    {
        @Override
        protected void doOpen(final org.apache.qpid.server.model.ConfiguredObject<?> parent)
        {
            // Not used by these unit tests.
        }

        @Override
        protected void doClose()
        {
            // Not used by these unit tests.
        }

        @Override
        protected EnvironmentFacade getEnvironmentFacade()
        {
            // Not used by these unit tests.
            return Mockito.mock(EnvironmentFacade.class);
        }

        @Override
        protected Logger getLogger()
        {
            return Mockito.mock(Logger.class);
        }

        @Override
        public String getStoreLocation()
        {
            return "";
        }

        @Override
        public File getStoreLocationAsFile()
        {
            return null;
        }

        @Override
        public void onDelete(ConfiguredObject<?> parent)
        {

        }
    }

    private static final class TestMetaData implements StorableMessageMetaData
    {
        private final int _storableSize;
        private final int _contentSize;

        private TestMetaData(final int storableSize, final int contentSize)
        {
            _storableSize = storableSize;
            _contentSize = contentSize;
        }

        @Override
        public MessageMetaDataType getType()
        {
            return null;
        }

        @Override
        public int getStorableSize()
        {
            return _storableSize;
        }

        @Override
        public void writeToBuffer(final QpidByteBuffer dest)
        {
            // Not used by these unit tests.
        }

        @Override
        public int getContentSize()
        {
            return _contentSize;
        }

        @Override
        public boolean isPersistent()
        {
            return true;
        }

        @Override
        public void dispose()
        {
            // Not used by these unit tests.
        }

        @Override
        public void clearEncodedForm()
        {
            // Not used by these unit tests.
        }

        @Override
        public void reallocate()
        {
            // Not used by these unit tests.
        }
    }
}
