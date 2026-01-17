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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.store.SizeMonitoringSettings;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

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
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6}, readRemaining(content));
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
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, readRemaining(content));
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
            assertArrayEquals(new byte[]{1, 2, 3, 4}, readRemaining(content));
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
            assertArrayEquals(new byte[]{1, 2, 3, 4}, readRemaining(partial));
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
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, readRemaining(content));
        }
    }

    @Test
    void getContentReturnsViewForOffsetAndLength() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        final TestMetaData metaData = new TestMetaData(3, 6);

        final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, false);

        try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6}))
        {
            message.addContent(f1);
        }

        try (QpidByteBuffer view = message.getContent(2, 3))
        {
            assertArrayEquals(new byte[]{3, 4, 5}, readRemaining(view));
        }

        try (QpidByteBuffer full = message.getContent(0, Integer.MAX_VALUE))
        {
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6}, readRemaining(full));
        }
    }

    @Test
    void getContentWithMaxValueLengthHonoursOffset() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        final TestMetaData metaData = new TestMetaData(3, 6);

        final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, false);

        try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6}))
        {
            message.addContent(f1);
        }

        try (QpidByteBuffer view = message.getContent(2, Integer.MAX_VALUE))
        {
            assertArrayEquals(new byte[]{3, 4, 5, 6}, readRemaining(view));
        }
    }

    @Test
    void getContentWhenNoContentReturnsEmpty() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        final TestMetaData metaData = new TestMetaData(3, 0);

        final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, false);

        try (QpidByteBuffer content = message.getContent(0, Integer.MAX_VALUE))
        {
            assertEquals(0, content.remaining());
        }
    }

    @Test
    void singleFragmentConsolidationMovesFragmentIntoMessageData() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        final TestMetaData metaData = new TestMetaData(3, 3);

        final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, false);

        try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2, 3}))
        {
            message.addContent(f1);
        }

        assertEquals(1, fragmentCount(message));
        assertNull(getMessageData(message));

        message.allContentAdded();

        assertEquals(0, fragmentCount(message));
        assertNotNull(getMessageData(message));

        try (QpidByteBuffer content = message.getContent(0, Integer.MAX_VALUE))
        {
            assertArrayEquals(new byte[]{1, 2, 3}, readRemaining(content));
        }
    }

    @Test
    void flowToDiskClearsMetadataAndContentAndUpdatesStoreCounters() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        final int metadataSize = 7;
        final int contentSize = 4;
        final TestMetaData metaData = new TestMetaData(metadataSize, contentSize);

        final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, true);

        assertEquals(metadataSize, store.getInMemorySize());
        assertEquals(0L, store.getBytesEvacuatedFromMemory());

        try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2});
             QpidByteBuffer f2 = QpidByteBuffer.wrap(new byte[]{3, 4}))
        {
            message.addContent(f1);
            message.addContent(f2);
        }

        message.allContentAdded();

        assertEquals(metadataSize + contentSize, store.getInMemorySize());
        assertTrue(message.isInContentInMemory());

        message.flowToDisk();

        assertEquals(0L, store.getInMemorySize());
        assertEquals(metadataSize + contentSize, store.getBytesEvacuatedFromMemory());
        assertFalse(message.isInContentInMemory());
        assertEquals(0L, message.getInMemorySize());
    }

    @Test
    void afterFlowToDiskGetMetaDataReloadsAndAccountsMemory() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        store.openMessageStore(createParent());
        try
        {
            final int metadataSize = 5;
            final int contentSize = 4;

            final TestMetaData initialMetaData = new TestMetaData(metadataSize, contentSize);
            final TestMetaData reloadedMetaData = new TestMetaData(metadataSize, contentSize);
            store.setMessageMetaData(1L, reloadedMetaData);

            final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                    store.new StoredBDBMessage<>(1L, initialMetaData, true);

            try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2, 3, 4}))
            {
                message.addContent(f1);
            }
            message.allContentAdded();

            message.flowToDisk();
            assertEquals(0L, store.getInMemorySize());

            final TestMetaData loaded = message.getMetaData();
            assertNotNull(loaded);
            assertEquals(1, store.getMetaDataLoadCount());
            assertEquals(metadataSize, store.getInMemorySize());
            assertEquals(metadataSize, message.getInMemorySize());
        }
        finally
        {
            store.closeMessageStore();
        }
    }

    @Test
    void afterFlowToDiskGetContentReloadsAndAccountsMemory() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        store.openMessageStore(createParent());
        try
        {
            final int metadataSize = 5;
            final int contentSize = 4;

            final TestMetaData initialMetaData = new TestMetaData(metadataSize, contentSize);
            store.setMessageContent(1L, new byte[]{1, 2, 3, 4});

            final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                    store.new StoredBDBMessage<>(1L, initialMetaData, true);

            // Ensure data can be cleared from memory.
            try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2, 3, 4}))
            {
                message.addContent(f1);
            }
            message.allContentAdded();

            message.flowToDisk();
            assertEquals(0L, store.getInMemorySize());

            try (QpidByteBuffer content = message.getContent(0, Integer.MAX_VALUE))
            {
                assertArrayEquals(new byte[]{1, 2, 3, 4}, readRemaining(content));
            }

            assertEquals(1, store.getContentLoadCount());
            assertEquals(contentSize, store.getInMemorySize());
            assertTrue(message.isInContentInMemory());
            assertEquals(contentSize, message.getInMemorySize());

            // Clean up direct/heap buffers held by the message.
            message.flowToDisk();
        }
        finally
        {
            store.closeMessageStore();
        }
    }

    @Test
    void isInContentInMemoryTransitionsAcrossLifecycle() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        store.openMessageStore(createParent());
        try
        {
            final int metadataSize = 5;
            final int contentSize = 4;

            final TestMetaData initialMetaData = new TestMetaData(metadataSize, contentSize);
            store.setMessageContent(1L, new byte[]{1, 2, 3, 4});

            final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                    store.new StoredBDBMessage<>(1L, initialMetaData, true);

            assertFalse(message.isInContentInMemory());

            try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2, 3, 4}))
            {
                message.addContent(f1);
            }
            message.allContentAdded();

            assertTrue(message.isInContentInMemory());

            message.flowToDisk();
            assertFalse(message.isInContentInMemory());

            try (QpidByteBuffer content = message.getContent(0, Integer.MAX_VALUE))
            {
                assertArrayEquals(new byte[]{1, 2, 3, 4}, readRemaining(content));
            }

            assertTrue(message.isInContentInMemory());

            // Clean up direct/heap buffers held by the message.
            message.flowToDisk();
        }
        finally
        {
            store.closeMessageStore();
        }
    }

    @Test
    void clearFalseInvokesClearEncodedFormNotDispose() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();

        @SuppressWarnings("unchecked")
        final StorableMessageMetaData metaData = Mockito.mock(StorableMessageMetaData.class);
        Mockito.when(metaData.getStorableSize()).thenReturn(3);
        Mockito.when(metaData.getContentSize()).thenReturn(0);
        Mockito.when(metaData.isPersistent()).thenReturn(true);

        final AbstractBDBMessageStore.StoredBDBMessage<StorableMessageMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, false);

        message.clear(false);

        Mockito.verify(metaData).clearEncodedForm();
        Mockito.verify(metaData, Mockito.never()).dispose();
    }

    @Test
    void clearTrueInvokesDispose() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();

        @SuppressWarnings("unchecked")
        final StorableMessageMetaData metaData = Mockito.mock(StorableMessageMetaData.class);
        Mockito.when(metaData.getStorableSize()).thenReturn(3);
        Mockito.when(metaData.getContentSize()).thenReturn(0);
        Mockito.when(metaData.isPersistent()).thenReturn(true);

        final AbstractBDBMessageStore.StoredBDBMessage<StorableMessageMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, false);

        message.clear(true);

        Mockito.verify(metaData).dispose();
        Mockito.verify(metaData, Mockito.never()).clearEncodedForm();
    }

    @Test
    void removeReleasesResourcesAndDecrementsStoreInMemorySize() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        store.openMessageStore(createParent());
        try
        {
            final int metadataSize = 4;
            final int contentSize = 4;
            final TestMetaData metaData = new TestMetaData(metadataSize, contentSize);

            final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                    store.new StoredBDBMessage<>(1L, metaData, false);

            try (QpidByteBuffer f1 = QpidByteBuffer.wrap(new byte[]{1, 2, 3, 4}))
            {
                message.addContent(f1);
            }
            message.allContentAdded();

            assertEquals(metadataSize + contentSize, store.getInMemorySize());
            assertNotNull(getMessageData(message));

            message.remove();

            assertEquals(0L, store.getInMemorySize());
            assertNull(message.getMetaData());
            assertNull(getMessageData(message));
            assertEquals(0, fragmentCount(message));
            assertFalse(message.isInContentInMemory());
            assertEquals(0L, message.getInMemorySize());

            try (QpidByteBuffer content = message.getContent(0, Integer.MAX_VALUE))
            {
                assertEquals(0, content.remaining());
            }
        }
        finally
        {
            store.closeMessageStore();
        }
    }

    @Test
    void getMetaDataThrowsWhenStoreIsClosedAndReloadIsRequired() throws Exception
    {
        final TestBDBMessageStore store = new TestBDBMessageStore();
        final int metadataSize = 3;
        final int contentSize = 0;
        final TestMetaData metaData = new TestMetaData(metadataSize, contentSize);

        final AbstractBDBMessageStore.StoredBDBMessage<TestMetaData> message =
                store.new StoredBDBMessage<>(1L, metaData, true);

        message.flowToDisk();

        assertThrows(IllegalStateException.class, message::getMetaData);
    }

    private static byte[] readRemaining(final QpidByteBuffer buffer)
    {
        final byte[] actual = new byte[buffer.remaining()];
        buffer.get(actual);
        return actual;
    }

    private static ConfiguredObject<?> createParent()
    {
        final ConfiguredObject<?> parent = Mockito.mock(ConfiguredObject.class,
                Mockito.withSettings().extraInterfaces(SizeMonitoringSettings.class));
        Mockito.when(((SizeMonitoringSettings) parent).getStoreOverfullSize()).thenReturn(1024L);
        Mockito.when(((SizeMonitoringSettings) parent).getStoreUnderfullSize()).thenReturn(512L);
        return parent;
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
        private final EnvironmentFacade _environmentFacade = Mockito.mock(EnvironmentFacade.class);
        private final Logger _logger = Mockito.mock(Logger.class);

        private final Map<Long, StorableMessageMetaData> _metaDataById = new HashMap<>();
        private final Map<Long, byte[]> _contentById = new HashMap<>();
        private final AtomicInteger _metaDataLoadCount = new AtomicInteger();
        private final AtomicInteger _contentLoadCount = new AtomicInteger();

        void setMessageMetaData(final long messageId, final StorableMessageMetaData metaData)
        {
            _metaDataById.put(messageId, metaData);
        }

        void setMessageContent(final long messageId, final byte[] content)
        {
            _contentById.put(messageId, content);
        }

        int getMetaDataLoadCount()
        {
            return _metaDataLoadCount.get();
        }

        int getContentLoadCount()
        {
            return _contentLoadCount.get();
        }

        @Override
        protected void doOpen(final ConfiguredObject<?> parent)
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
            // Most unit tests do not require a real JE environment.
            return _environmentFacade;
        }

        @Override
        protected Logger getLogger()
        {
            return _logger;
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
        public void onDelete(final ConfiguredObject<?> parent)
        {
            // Not used by these unit tests.
        }

        @Override
        StorableMessageMetaData getMessageMetaData(final long messageId) throws StoreException
        {
            _metaDataLoadCount.incrementAndGet();
            final StorableMessageMetaData metaData = _metaDataById.get(messageId);
            if (metaData == null)
            {
                throw new StoreException("Metadata not found for message with id " + messageId);
            }
            return metaData;
        }

        @Override
        QpidByteBuffer getAllContent(final long messageId) throws StoreException
        {
            _contentLoadCount.incrementAndGet();
            final byte[] content = _contentById.get(messageId);
            if (content == null)
            {
                throw new StoreException("Unable to find message with id " + messageId);
            }
            // Use a new buffer instance to mimic the behaviour of the production store implementation.
            return QpidByteBuffer.wrap(content.clone());
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
