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

package org.apache.qpid.server.store.rocksdb;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.rocksdb.Transaction;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;

/**
 * Represents stored message data.
 * <br>
 * Thread-safety: supports concurrent access after persistence.
 */
final class RocksDBStoredMessage<T extends StorableMessageMetaData>
        implements StoredMessage<T>, MessageHandle<T>
{
    interface StoreAccess
    {
        int getMessageChunkSize();

        int getDefaultChunkSize();

        boolean shouldChunk(int contentSize);

        byte[] loadContent(long messageId);

        MetaDataRecord loadMetaDataRecord(long messageId);

        byte[] loadChunkedContentSlice(long messageId,
                                       int contentSize,
                                       int chunkSize,
                                       int offset,
                                       int length);

        void storeChunk(Transaction txn, long messageId, int chunkIndex, byte[] data);

        void storeChunkedMessage(Transaction txn,
                                 long messageId,
                                 StorableMessageMetaData metaData,
                                 int chunkSize,
                                 List<byte[]> chunks,
                                 byte[] trailingChunk);

        void storeMessageMetadata(Transaction txn,
                                  long messageId,
                                  StorableMessageMetaData metaData,
                                  boolean chunked,
                                  int chunkSize);

        void storeMessage(Transaction txn,
                          long messageId,
                          StorableMessageMetaData metaData,
                          byte[] content);

        void deleteMessage(long messageId);

        void storedSizeChangeOccurred(int delta);

        void notifyMessageDeleted(StoredMessage<?> message);

        AtomicLong getInMemorySize();

        AtomicLong getBytesEvacuatedFromMemory();

        byte[] emptyValue();
    }

    private final StoreAccess _access;
    private final long _messageId;
    private final int _contentSize;
    private final int _metadataSize;
    private int _chunkSize;

    private T _metaData;
    private byte[] _content;
    private ByteArrayOutputStream _contentStream;
    private QpidByteBuffer _directContent;
    private byte[] _chunkBuffer;
    private int _chunkBufferOffset;
    private int _nextChunkIndex;
    private List<byte[]> _pendingChunks;
    private boolean _stored;
    private boolean _storePending;
    private boolean _hardRef;
    private boolean _chunked;

    RocksDBStoredMessage(final StoreAccess access, final long messageId, final T metaData)
    {
        _access = access;
        _messageId = messageId;
        _metaData = metaData;
        _metadataSize = metaData.getStorableSize();
        _contentSize = metaData.getContentSize();
        _chunked = access.shouldChunk(_contentSize);
        _chunkSize = _chunked ? access.getMessageChunkSize() : 0;
        if (_chunked)
        {
            _chunkBuffer = new byte[_chunkSize];
            _chunkBufferOffset = 0;
            _nextChunkIndex = 0;
            _pendingChunks = new ArrayList<>();
        }
        else
        {
            _contentStream = new ByteArrayOutputStream(Math.max(_contentSize, 0));
        }
        _stored = false;
        _storePending = false;
        _hardRef = true;
        _access.getInMemorySize().addAndGet(_metadataSize);
    }

    RocksDBStoredMessage(final StoreAccess access, final long messageId, final T metaData, final boolean stored)
    {
        _access = access;
        _messageId = messageId;
        _metaData = metaData;
        _metadataSize = metaData.getStorableSize();
        _contentSize = metaData.getContentSize();
        _chunked = false;
        _chunkSize = 0;
        _stored = stored;
        _storePending = false;
        _hardRef = !stored;
        _access.getInMemorySize().addAndGet(_metadataSize);
    }

    RocksDBStoredMessage(final StoreAccess access,
                         final long messageId,
                         final MetaDataRecord record,
                         final boolean stored)
    {
        _access = access;
        _messageId = messageId;
        T metaData = (T) record.metaData();
        _metaData = metaData;
        _metadataSize = metaData.getStorableSize();
        _contentSize = metaData.getContentSize();
        _chunked = record.chunked();
        _chunkSize = record.chunkSize();
        if (_chunked && _chunkSize <= 0)
        {
            _chunkSize = access.getDefaultChunkSize();
        }
        _stored = stored;
        _hardRef = !stored;
        _access.getInMemorySize().addAndGet(_metadataSize);
    }

    @Override
    public void addContent(final QpidByteBuffer src)
    {
        clearDirectContentCache();
        if (_chunked)
        {
            while (src.hasRemaining())
            {
                int toCopy = Math.min(src.remaining(), _chunkSize - _chunkBufferOffset);
                src.get(_chunkBuffer, _chunkBufferOffset, toCopy);
                _chunkBufferOffset += toCopy;
                if (_chunkBufferOffset == _chunkSize)
                {
                    if (_pendingChunks == null)
                    {
                        _pendingChunks = new ArrayList<>();
                    }
                    _pendingChunks.add(Arrays.copyOf(_chunkBuffer, _chunkBufferOffset));
                    _chunkBufferOffset = 0;
                }
            }
        }
        else
        {
            if (_contentStream == null)
            {
                _contentStream = new ByteArrayOutputStream();
            }
            byte[] chunk = new byte[src.remaining()];
            src.get(chunk);
            _contentStream.write(chunk, 0, chunk.length);
        }
    }

    @Override
    public synchronized StoredMessage<T> allContentAdded()
    {
        _access.getInMemorySize().addAndGet(_contentSize);
        return this;
    }

    @Override
    public synchronized T getMetaData()
    {
        if (_metaData == null)
        {
            MetaDataRecord record = _access.loadMetaDataRecord(_messageId);
            @SuppressWarnings("unchecked")
            T metaData = record == null ? null : (T) record.metaData();
            _metaData = metaData;
            if (record != null)
            {
                _chunked = record.chunked();
                if (_chunked && _chunkSize <= 0)
                {
                    _chunkSize = record.chunkSize() > 0 ? record.chunkSize() : _access.getDefaultChunkSize();
                }
            }
            _access.getInMemorySize().addAndGet(_metadataSize);
        }
        return _metaData;
    }

    @Override
    public long getMessageNumber()
    {
        return _messageId;
    }

    @Override
    public synchronized QpidByteBuffer getContent(final int offset, final int length)
    {
        if (_chunked)
        {
            byte[] slice = _stored
                    ? _access.loadChunkedContentSlice(_messageId, _contentSize, _chunkSize, offset, length)
                    : loadChunkedContentSliceFromMemory(offset, length);
            return slice.length == 0 ? QpidByteBuffer.emptyQpidByteBuffer() : toDirectBuffer(slice, 0, slice.length);
        }
        else
        {
            byte[] content = loadContentIfNecessary();
            if (offset >= content.length)
            {
                return QpidByteBuffer.emptyQpidByteBuffer();
            }
            int available = content.length - offset;
            int size = length == Integer.MAX_VALUE ? available : Math.min(length, available);
            if (size <= 0)
            {
                return QpidByteBuffer.emptyQpidByteBuffer();
            }
            return getDirectContentBuffer().view(offset, size);
        }
    }

    @Override
    public int getContentSize()
    {
        return _contentSize;
    }

    @Override
    public int getMetadataSize()
    {
        return _metadataSize;
    }

    @Override
    public void remove()
    {
        _access.deleteMessage(_messageId);
        _access.storedSizeChangeOccurred(-getContentSize());
        _access.notifyMessageDeleted(this);
        long bytesCleared = clearInMemory(true);
        _access.getInMemorySize().addAndGet(-bytesCleared);
    }

    @Override
    public boolean isInContentInMemory()
    {
        return _hardRef
               || _content != null
               || _contentStream != null
               || _directContent != null
               || _chunkBufferOffset > 0
               || (_pendingChunks != null && !_pendingChunks.isEmpty());
    }

    @Override
    public long getInMemorySize()
    {
        long size = 0L;
        if (_hardRef)
        {
            size += _metadataSize;
            if (_chunked)
            {
                size += _chunkBufferOffset + getPendingChunksSize();
            }
            else
            {
                size += _contentSize;
            }
        }
        else
        {
            if (_metaData != null)
            {
                size += _metadataSize;
            }
            if (_content != null || _contentStream != null)
            {
                size += _contentSize;
            }
            if (_chunked)
            {
                size += _chunkBufferOffset + getPendingChunksSize();
            }
        }
        return size;
    }

    @Override
    public synchronized boolean flowToDisk()
    {
        storeIfNecessary(null, true);
        if (!_hardRef)
        {
            long bytesCleared = clearInMemory(false);
            _access.getInMemorySize().addAndGet(-bytesCleared);
            _access.getBytesEvacuatedFromMemory().addAndGet(bytesCleared);
        }
        return true;
    }

    @Override
    public synchronized void reallocate()
    {
        if (_metaData != null)
        {
            _metaData.reallocate();
        }
    }

    void storeInTransaction(final Transaction txn)
    {
        storeIfNecessary(txn, false);
    }

    void commitStore()
    {
        if (_storePending)
        {
            finalizeStore();
        }
    }

    void abortStore()
    {
        _storePending = false;
    }

    boolean isStored()
    {
        return _stored;
    }

    private void storeIfNecessary(final Transaction txn, final boolean markStored)
    {
        if (_stored || _storePending)
        {
            return;
        }
        if (_chunked)
        {
            byte[] trailingChunk = _chunkBufferOffset > 0
                    ? Arrays.copyOf(_chunkBuffer, _chunkBufferOffset)
                    : null;
            _access.storeChunkedMessage(txn,
                                        _messageId,
                                        _metaData,
                                        _chunkSize,
                                        _pendingChunks,
                                        trailingChunk);
            int chunkIndex = (_pendingChunks == null ? 0 : _pendingChunks.size()) + (trailingChunk == null ? 0 : 1);
            _nextChunkIndex = chunkIndex;
        }
        else
        {
            byte[] content = _contentStream == null
                    ? (_content == null ? _access.emptyValue() : _content)
                    : _contentStream.toByteArray();
            _access.storeMessage(txn, _messageId, _metaData, content);
            _content = content;
            _contentStream = null;
        }
        if (markStored)
        {
            finalizeStore();
        }
        else
        {
            _storePending = true;
        }
    }

    private void finalizeStore()
    {
        _stored = true;
        _storePending = false;
        _hardRef = false;
    }

    private long getPendingChunksSize()
    {
        if (_pendingChunks == null || _pendingChunks.isEmpty())
        {
            return 0L;
        }
        long size = 0L;
        for (byte[] chunk : _pendingChunks)
        {
            size += chunk.length;
        }
        return size;
    }

    private byte[] loadContentIfNecessary()
    {
        if (_content == null)
        {
            if (_contentStream != null)
            {
                _content = _contentStream.toByteArray();
                return _content;
            }
            _content = _access.loadContent(_messageId);
            _access.getInMemorySize().addAndGet(_contentSize);
        }
        return _content;
    }

    private QpidByteBuffer getDirectContentBuffer()
    {
        if (_directContent == null)
        {
            byte[] content = loadContentIfNecessary();
            if (content.length == 0)
            {
                return QpidByteBuffer.emptyQpidByteBuffer();
            }
            _directContent = toDirectBuffer(content, 0, content.length);
        }
        return _directContent;
    }

    private QpidByteBuffer toDirectBuffer(final byte[] data, final int offset, final int length)
    {
        QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(length);
        buffer.put(data, offset, length);
        buffer.flip();
        return buffer;
    }

    private void clearDirectContentCache()
    {
        if (_directContent != null)
        {
            try
            {
                _directContent.dispose();
            }
            finally
            {
                _directContent = null;
            }
        }
    }

    private byte[] loadChunkedContentSliceFromMemory(final int offset, final int length)
    {
        if (_contentSize <= 0)
        {
            return _access.emptyValue();
        }
        byte[] content = buildChunkedContentFromMemory();
        if (offset >= content.length)
        {
            return _access.emptyValue();
        }
        int available = content.length - offset;
        int size = length == Integer.MAX_VALUE ? available : Math.min(length, available);
        return Arrays.copyOfRange(content, offset, offset + size);
    }

    private byte[] buildChunkedContentFromMemory()
    {
        int targetSize = _contentSize <= 0 ? 0 : _contentSize;
        if (targetSize == 0)
        {
            return _access.emptyValue();
        }
        byte[] data = new byte[targetSize];
        int position = 0;
        if (_pendingChunks != null)
        {
            for (byte[] chunk : _pendingChunks)
            {
                System.arraycopy(chunk, 0, data, position, chunk.length);
                position += chunk.length;
            }
        }
        if (_chunkBuffer != null && _chunkBufferOffset > 0)
        {
            System.arraycopy(_chunkBuffer, 0, data, position, _chunkBufferOffset);
            position += _chunkBufferOffset;
        }
        if (position < data.length)
        {
            return Arrays.copyOf(data, position);
        }
        return data;
    }

    private long clearInMemory(final boolean close)
    {
        long bytesCleared = 0L;
        clearDirectContentCache();
        if (_contentStream != null || _content != null)
        {
            bytesCleared += _contentSize;
            _contentStream = null;
            _content = null;
        }
        _chunkBuffer = null;
        _chunkBufferOffset = 0;
        _pendingChunks = null;
        if (_metaData != null)
        {
            bytesCleared += _metadataSize;
            try
            {
                if (close)
                {
                    _metaData.dispose();
                }
                else
                {
                    _metaData.clearEncodedForm();
                }
            }
            finally
            {
                _metaData = null;
            }
        }
        return bytesCleared;
    }
}
