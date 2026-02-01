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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;
import org.rocksdb.Status;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.EventManager;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageMetaDataTypeRegistry;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.SizeMonitoringSettings;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.Transaction.StoredXidRecord;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.util.FileUtils;

/**
 * Provides a RocksDB-backed message store.
 * <br>
 * Thread-safety: safe for concurrent access by the configuration model.
 */
public class RocksDBMessageStore implements MessageStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBMessageStore.class);
    private static final byte[] NEXT_MESSAGE_ID_KEY = "nextMessageId".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MESSAGE_STORE_VERSION_KEY =
            "rocksdb_message_store_version".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] EMPTY_VALUE = new byte[0];
    private static final int LONG_BYTES = 8;
    private static final int UUID_BYTES = 16;
    private static final int INT_BYTES = 4;
    private static final byte METADATA_FORMAT_MARKER = (byte) 0x80;
    private static final byte METADATA_FORMAT_VERSION = 1;
    private static final int METADATA_HEADER_BYTES = 1 + 1 + 4;
    private static final byte METADATA_FLAG_CHUNKED = 0x01;
    private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
    private static final int ORPHAN_CHUNK_DELETE_BATCH_SIZE = 10000;
    private static final int DEFAULT_QUEUE_SEGMENT_SHIFT = 16;
    private static final int QUEUE_SEGMENT_WARN_THRESHOLD = 100_000;
    private static final int ORPHAN_SEGMENT_DELETE_BATCH_SIZE = 10000;
    private static final long MESSAGE_STORE_VERSION = 1L;
    private static final int DEFAULT_TRANSACTION_RETRY_ATTEMPTS = 5;
    private static final long DEFAULT_TRANSACTION_RETRY_BASE_SLEEP_MILLIS = 25L;
    private static final int DEFAULT_COMMITTER_NOTIFY_THRESHOLD = 8;
    private static final long DEFAULT_COMMITTER_WAIT_TIMEOUT_MS = 500L;
    static final String MESSAGE_ID_ALLOCATION_SIZE_PROPERTY = "qpid.rocksdb.messageIdAllocationSize";
    private static final int DEFAULT_MESSAGE_ID_ALLOCATION_SIZE = 1024;

    private final AtomicLong _messageId = new AtomicLong(0);
    private final AtomicLong _inMemorySize = new AtomicLong(0);
    private final AtomicLong _bytesEvacuatedFromMemory = new AtomicLong(0);
    private final Set<MessageDeleteListener> _messageDeleteListeners = ConcurrentHashMap.newKeySet();
    private final EventManager _eventManager = new EventManager();
    private final Object _messageIdLock = new Object();

    private RocksDBEnvironment _environment;
    private boolean _environmentOwned;
    private String _storePath;
    private String _walDir;
    private int _messageChunkSize = DEFAULT_CHUNK_SIZE;
    private int _messageInlineThreshold = DEFAULT_CHUNK_SIZE;
    private int _queueSegmentShift = DEFAULT_QUEUE_SEGMENT_SHIFT;
    private int _transactionRetryAttempts = DEFAULT_TRANSACTION_RETRY_ATTEMPTS;
    private long _transactionRetryBaseSleepMillis = DEFAULT_TRANSACTION_RETRY_BASE_SLEEP_MILLIS;
    private long _transactionLockTimeoutMs;
    private int _committerNotifyThreshold = DEFAULT_COMMITTER_NOTIFY_THRESHOLD;
    private long _committerWaitTimeoutMs = DEFAULT_COMMITTER_WAIT_TIMEOUT_MS;
    private int _messageIdAllocationSize = DEFAULT_MESSAGE_ID_ALLOCATION_SIZE;
    private volatile long _messageIdUpperBound;
    private Boolean _writeSync;
    private Boolean _disableWAL;
    private long _persistentSizeHighThreshold;
    private long _persistentSizeLowThreshold;
    private long _totalStoreSize;
    private boolean _limitBusted;
    private RocksDBCommitter _committer;
    private final RocksDBStoredMessage.StoreAccess _storedMessageAccess = new RocksDBStoredMessage.StoreAccess()
    {
        @Override
        public int getMessageChunkSize()
        {
            return _messageChunkSize;
        }

        @Override
        public int getDefaultChunkSize()
        {
            return DEFAULT_CHUNK_SIZE;
        }

        @Override
        public boolean shouldChunk(final int contentSize)
        {
            return RocksDBMessageStore.this.shouldChunk(contentSize);
        }

        @Override
        public byte[] loadContent(final long messageId)
        {
            return RocksDBMessageStore.this.loadContent(messageId);
        }

        @Override
        public MetaDataRecord loadMetaDataRecord(final long messageId)
        {
            return RocksDBMessageStore.this.loadMetaDataRecord(messageId);
        }

        @Override
        public byte[] loadChunkedContentSlice(final long messageId,
                                              final int contentSize,
                                              final int chunkSize,
                                              final int offset,
                                              final int length)
        {
            return RocksDBMessageStore.this.loadChunkedContentSlice(messageId,
                                                                   contentSize,
                                                                   chunkSize,
                                                                   offset,
                                                                   length);
        }

        @Override
        public void storeChunk(final org.rocksdb.Transaction txn,
                               final long messageId,
                               final int chunkIndex,
                               final byte[] data)
        {
            RocksDBMessageStore.this.storeChunk(txn, messageId, chunkIndex, data);
        }

        @Override
        public void storeChunkedMessage(final org.rocksdb.Transaction txn,
                                        final long messageId,
                                        final StorableMessageMetaData metaData,
                                        final int chunkSize,
                                        final List<byte[]> chunks,
                                        final byte[] trailingChunk)
        {
            RocksDBMessageStore.this.storeChunkedMessage(txn,
                                                        messageId,
                                                        metaData,
                                                        chunkSize,
                                                        chunks,
                                                        trailingChunk);
        }

        @Override
        public void storeMessageMetadata(final org.rocksdb.Transaction txn,
                                         final long messageId,
                                         final StorableMessageMetaData metaData,
                                         final boolean chunked,
                                         final int chunkSize)
        {
            RocksDBMessageStore.this.storeMessageMetadata(txn, messageId, metaData, chunked, chunkSize);
        }

        @Override
        public void storeMessage(final org.rocksdb.Transaction txn,
                                 final long messageId,
                                 final StorableMessageMetaData metaData,
                                 final byte[] content)
        {
            RocksDBMessageStore.this.storeMessage(txn, messageId, metaData, content);
        }

        @Override
        public void deleteMessage(final long messageId)
        {
            RocksDBMessageStore.this.deleteMessage(messageId);
        }

        @Override
        public void storedSizeChangeOccurred(final int delta)
        {
            RocksDBMessageStore.this.storedSizeChangeOccurred(delta);
        }

        @Override
        public void notifyMessageDeleted(final StoredMessage<?> message)
        {
            RocksDBMessageStore.this.notifyMessageDeleted(message);
        }

        @Override
        public AtomicLong getInMemorySize()
        {
            return _inMemorySize;
        }

        @Override
        public AtomicLong getBytesEvacuatedFromMemory()
        {
            return _bytesEvacuatedFromMemory;
        }

        @Override
        public byte[] emptyValue()
        {
            return EMPTY_VALUE;
        }
    };
    private final RocksDBXidRecordMapper.RecordFactory _xidRecordFactory =
            new RocksDBXidRecordMapper.RecordFactory()
    {
        @Override
        public Transaction.EnqueueRecord createEnqueue(final UUID queueId, final long messageId)
        {
            return new RecordImpl(queueId, messageId);
        }

        @Override
        public Transaction.DequeueRecord createDequeue(final UUID queueId, final long messageId)
        {
            return new RecordImpl(queueId, messageId);
        }
    };
    private final RocksDBTransaction.StoreAccess _transactionAccess = new RocksDBTransaction.StoreAccess()
    {
        @Override
        public TransactionDB getTransactionDb()
        {
            return RocksDBMessageStore.this.getTransactionDb();
        }

        @Override
        public WriteOptions createWriteOptions(final boolean deferSync)
        {
            return RocksDBMessageStore.this.createWriteOptions(deferSync);
        }

        @Override
        public void applyQueueEntriesToTransaction(final org.rocksdb.Transaction txn,
                                                   final List<RocksDBEnqueueRecord> enqueues,
                                                   final List<RocksDBEnqueueRecord> dequeues)
        {
            RocksDBMessageStore.this.applyQueueEntriesToTransaction(txn, enqueues, dequeues);
        }

        @Override
        public void applyXidRecordsToTransaction(final org.rocksdb.Transaction txn,
                                                 final List<XidRecord> xidRecords,
                                                 final List<byte[]> xidRemovals)
        {
            RocksDBMessageStore.this.applyXidRecordsToTransaction(txn, xidRecords, xidRemovals);
        }

        @Override
        public void storedSizeChangeOccurred(final int delta)
        {
            RocksDBMessageStore.this.storedSizeChangeOccurred(delta);
        }

        @Override
        public boolean isRetryable(final RocksDBException exception)
        {
            return RocksDBMessageStore.this.isRetryable(exception);
        }

        @Override
        public int getTransactionRetryAttempts()
        {
            return _transactionRetryAttempts;
        }

        @Override
        public long getTransactionRetryBaseSleepMillis()
        {
            return _transactionRetryBaseSleepMillis;
        }

        @Override
        public long getTransactionLockTimeoutMs()
        {
            return _transactionLockTimeoutMs;
        }

        @Override
        public RocksDBCommitter getCommitter()
        {
            return _committer;
        }
    };

    /**
     * Creates a RocksDB message store instance.
     */
    public RocksDBMessageStore()
    {
    }

    /**
     * Creates a RocksDB message store instance.
     *
     * @param environment RocksDB environment.
     */
    public RocksDBMessageStore(final RocksDBEnvironment environment)
    {
        setEnvironment(environment);
    }

    /**
     * Sets the RocksDB environment.
     *
     * @param environment RocksDB environment.
     */
    public void setEnvironment(final RocksDBEnvironment environment)
    {
        _environment = environment;
        _environmentOwned = false;
        _storePath = environment == null ? null : environment.getStorePath();
    }

    /**
     * Returns the RocksDB environment.
     *
     * @return the RocksDB environment.
     */
    public RocksDBEnvironment getEnvironment()
    {
        return _environment;
    }

    /**
     * Returns the next message id.
     *
     * @return the next message id.
     *
     */
    @Override
    public long getNextMessageId()
    {
        checkOpen();
        while (true)
        {
            long current = _messageId.get();
            long upperBound = _messageIdUpperBound;
            if (current < upperBound)
            {
                if (_messageId.compareAndSet(current, current + 1))
                {
                    return current + 1;
                }
                continue;
            }
            synchronized (_messageIdLock)
            {
                current = _messageId.get();
                upperBound = _messageIdUpperBound;
                if (current < upperBound)
                {
                    continue;
                }
                return allocateMessageIdBlock();
            }
        }
    }

    /**
     * Returns the store location string.
     *
     * @return the store location string.
     *
     */
    @Override
    public String getStoreLocation()
    {
        return _storePath;
    }

    /**
     * Returns the store location file.
     *
     * @return the store location file.
     *
     */
    @Override
    public File getStoreLocationAsFile()
    {
        if (_storePath == null || _storePath.isEmpty())
        {
            return null;
        }
        return new File(_storePath);
    }

    /**
     * Adds an event listener for store events.
     *
     * @param eventListener event listener.
     * @param events events to listen to.
     *
     */
    @Override
    public void addEventListener(final EventListener eventListener, final Event... events)
    {
        _eventManager.addEventListener(eventListener, events);
    }

    /**
     * Opens the message store.
     *
     * @param parent parent object.
     *
     */
    @Override
    public void openMessageStore(final ConfiguredObject<?> parent)
    {
        RocksDBSettings settings = parent instanceof RocksDBSettings ? (RocksDBSettings) parent : null;
        if (_environment == null)
        {
            RocksDBEnvironment environment = null;
            if (parent instanceof FileBasedSettings && parent instanceof RocksDBSettings)
            {
                _storePath = ((FileBasedSettings) parent).getStorePath();
                _environment = RocksDBEnvironmentImpl.open(_storePath, (RocksDBSettings) parent);
                _environmentOwned = true;
            }
            else
            {
                if (parent != null)
                {
                    ConfiguredObject<?> ancestor = parent.getParent();
                    if (ancestor instanceof VirtualHostNode)
                    {
                        DurableConfigurationStore store = ((VirtualHostNode<?>) ancestor).getConfigurationStore();
                        if (store instanceof RocksDBConfigurationStore)
                        {
                            environment = ((RocksDBConfigurationStore) store).getEnvironment();
                        }
                    }
                }
                if (environment != null)
                {
                    setEnvironment(environment);
                }
                else
                {
                    throw new StoreException("Parent does not provide RocksDB settings");
                }
            }
        }
        else
        {
            _storePath = _environment.getStorePath();
        }
        applyMessageChunkSettings(settings);
        applyQueueSegmentSettings(settings);
        applyTransactionSettings(settings);
        applyCommitterSettings(settings);
        applyWriteSettings(settings);
        _messageIdAllocationSize = getMessageIdAllocationSize();
        applySizeMonitoringSettings(parent);
        _totalStoreSize = getSizeOnDisk();
        ensureMessageStoreVersion();
        cleanupOrphanedMessageChunks();
        cleanupOrphanedQueueSegments();
        loadNextMessageId();
        startAsyncCommitter();
    }

    /**
     * Upgrades the store structure if required.
     *
     * @throws StoreException on store errors.
     */
    @Override
    public void upgradeStoreStructure() throws StoreException
    {
        ensureMessageStoreVersion();
    }

    /**
     * Adds a message to the store.
     *
     * @param metaData message metadata.
     * @param <T> metadata type.
     *
     * @return the message handle.
     *
     */
    @Override
    public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(final T metaData)
    {
        checkOpen();
        long messageId = getNextMessageId();
        return new RocksDBStoredMessage<>(_storedMessageAccess, messageId, metaData);
    }

    /**
     * Returns the in-memory size.
     *
     * @return the in-memory size.
     *
     */
    @Override
    public long getInMemorySize()
    {
        return _inMemorySize.get();
    }

    /**
     * Returns the bytes evacuated from memory.
     *
     * @return the bytes evacuated from memory.
     *
     */
    @Override
    public long getBytesEvacuatedFromMemory()
    {
        return _bytesEvacuatedFromMemory.get();
    }

    /**
     * Resets store statistics.
     *
     */
    @Override
    public void resetStatistics()
    {
        _bytesEvacuatedFromMemory.set(0L);
    }

    /**
     * Returns whether the store is persistent.
     *
     * @return true when the store is persistent.
     *
     */
    @Override
    public boolean isPersistent()
    {
        return true;
    }

    /**
     * Creates a new transaction.
     *
     * @return the transaction.
     *
     */
    @Override
    public Transaction newTransaction()
    {
        checkOpen();
        return new RocksDBTransaction(_transactionAccess);
    }

    /**
     * Closes the message store.
     *
     */
    @Override
    public void closeMessageStore()
    {
        stopAsyncCommitter();
        if (_environmentOwned && _environment != null)
        {
            _environment.close();
        }
        _environment = null;
        _environmentOwned = false;
        _inMemorySize.set(0L);
        _bytesEvacuatedFromMemory.set(0L);
        _totalStoreSize = 0L;
        _limitBusted = false;
        _messageId.set(0L);
        _messageIdUpperBound = 0L;
    }

    /**
     * Deletes the message store data.
     *
     * @param parent parent object.
     *
     */
    @Override
    public void onDelete(final ConfiguredObject<?> parent)
    {
        if (_environment != null)
        {
            throw new IllegalStateException("Cannot delete the store while it is open");
        }
        if (!(parent instanceof FileBasedSettings))
        {
            return;
        }
        String storePath = ((FileBasedSettings) parent).getStorePath();
        if (storePath == null)
        {
            return;
        }
        FileUtils.delete(new File(storePath), true);
    }

    /**
     * Adds a message delete listener.
     *
     * @param listener listener to add.
     *
     */
    @Override
    public void addMessageDeleteListener(final MessageDeleteListener listener)
    {
        _messageDeleteListeners.add(listener);
    }

    /**
     * Removes a message delete listener.
     *
     * @param listener listener to remove.
     *
     */
    @Override
    public void removeMessageDeleteListener(final MessageDeleteListener listener)
    {
        _messageDeleteListeners.remove(listener);
    }

    /**
     * Creates a message store reader.
     *
     * @return the message store reader.
     *
     */
    @Override
    public MessageStoreReader newMessageStoreReader()
    {
        checkOpen();
        return new RocksDBMessageStoreReader();
    }

    /**
     * Loads the next message id from RocksDB.
     *
     * @throws StoreException on load failures.
     */
    private void loadNextMessageId() throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle versionHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.VERSION);
        try
        {
            byte[] value = database.get(versionHandle, NEXT_MESSAGE_ID_KEY);
            if (value != null)
            {
                long persisted = decodeLong(value);
                long maxId = findMaxMessageId();
                long nextId = Math.max(persisted, maxId);
                _messageId.set(nextId);
                _messageIdUpperBound = nextId;
                if (nextId != persisted)
                {
                    storeNextMessageId(nextId);
                }
            }
            else
            {
                long maxId = findMaxMessageId();
                _messageId.set(maxId);
                _messageIdUpperBound = maxId;
                storeNextMessageId(maxId);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to load next message id", e);
        }
    }

    private void ensureMessageStoreVersion()
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle versionHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.VERSION);
        try
        {
            byte[] stored = database.get(versionHandle, MESSAGE_STORE_VERSION_KEY);
            if (stored == null)
            {
                database.put(versionHandle, MESSAGE_STORE_VERSION_KEY, encodeLong(MESSAGE_STORE_VERSION));
                return;
            }
            long version = decodeLong(stored);
            if (version > MESSAGE_STORE_VERSION)
            {
                throw new StoreException("Unsupported RocksDB message store version " + version);
            }
            if (version < MESSAGE_STORE_VERSION)
            {
                throw new StoreException("RocksDB message store version " + version
                                         + " requires upgrade to " + MESSAGE_STORE_VERSION);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to read RocksDB message store version", e);
        }
    }

    /**
     * Stores the next message id in RocksDB.
     *
     * @param nextId next message id.
     *
     * @throws StoreException on write failures.
     */
    private void storeNextMessageId(final long nextId) throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle versionHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.VERSION);
        try
        {
            database.put(versionHandle, NEXT_MESSAGE_ID_KEY, encodeLong(nextId));
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to persist next message id", e);
        }
    }

    private long allocateMessageIdBlock()
    {
        TransactionDB database = getTransactionDb();
        ColumnFamilyHandle versionHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.VERSION);
        for (int attempt = 0; attempt < _transactionRetryAttempts; attempt++)
        {
            try (WriteOptions writeOptions = createWriteOptions();
                 TransactionOptions txnOptions = new TransactionOptions();
                 ReadOptions readOptions = new ReadOptions())
            {
                if (_transactionLockTimeoutMs > 0L)
                {
                    txnOptions.setLockTimeout(_transactionLockTimeoutMs);
                }
                try (org.rocksdb.Transaction txn = database.beginTransaction(writeOptions, txnOptions))
                {
                    byte[] value = txn.getForUpdate(readOptions, versionHandle, NEXT_MESSAGE_ID_KEY, true);
                    long current = value == null ? findMaxMessageId() : decodeLong(value);
                    long start = current + 1;
                    long upper = current + Math.max(1, _messageIdAllocationSize);
                    txn.put(versionHandle, NEXT_MESSAGE_ID_KEY, encodeLong(upper));
                    txn.commit();
                    _messageId.set(start);
                    _messageIdUpperBound = upper;
                    return start;
                }
            }
            catch (RocksDBException e)
            {
                if (!isRetryable(e) || attempt == _transactionRetryAttempts - 1)
                {
                    throw new StoreException("Failed to allocate message id block", e);
                }
                sleepBeforeRetry(attempt);
            }
        }
        throw new StoreException("Failed to allocate message id block");
    }

    /**
     * Removes message chunks without corresponding metadata.
     */
    private void cleanupOrphanedMessageChunks() throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle metadataHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
        ColumnFamilyHandle chunkHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CHUNKS);
        try (RocksIterator metadataIterator = database.newIterator(metadataHandle);
             RocksIterator chunkIterator = database.newIterator(chunkHandle);
             WriteBatch batch = new WriteBatch();
             WriteOptions options = createWriteOptions())
        {
            metadataIterator.seekToFirst();
            chunkIterator.seekToFirst();

            long currentMetaId = metadataIterator.isValid()
                    ? decodeMessageKey(metadataIterator.key())
                    : Long.MAX_VALUE;
            int deletesInBatch = 0;

            while (chunkIterator.isValid())
            {
                byte[] chunkKey = chunkIterator.key();
                long chunkMessageId = decodeChunkMessageId(chunkKey);

                while (metadataIterator.isValid() && currentMetaId < chunkMessageId)
                {
                    metadataIterator.next();
                    currentMetaId = metadataIterator.isValid()
                            ? decodeMessageKey(metadataIterator.key())
                            : Long.MAX_VALUE;
                }

                if (currentMetaId != chunkMessageId)
                {
                    batch.delete(chunkHandle, Arrays.copyOf(chunkKey, chunkKey.length));
                    deletesInBatch++;
                    if (deletesInBatch >= ORPHAN_CHUNK_DELETE_BATCH_SIZE)
                    {
                        database.write(options, batch);
                        batch.clear();
                        deletesInBatch = 0;
                    }
                }
                chunkIterator.next();
            }

            if (deletesInBatch > 0)
            {
                database.write(options, batch);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to remove orphaned message chunks", e);
        }
    }

    /**
     * Removes queue segments without corresponding queue state.
     */
    private void cleanupOrphanedQueueSegments() throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle segmentHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_SEG);
        ColumnFamilyHandle stateHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_STATE);
        try (RocksIterator segmentIterator = database.newIterator(segmentHandle);
             WriteBatch batch = new WriteBatch();
             WriteOptions options = createWriteOptions())
        {
            segmentIterator.seekToFirst();
            UUID currentQueueId = null;
            boolean stateExists = false;
            int deletesInBatch = 0;

            while (segmentIterator.isValid())
            {
                byte[] key = segmentIterator.key();
                UUID queueId = decodeQueueId(key);
                if (!queueId.equals(currentQueueId))
                {
                    currentQueueId = queueId;
                    byte[] state = database.get(stateHandle, encodeQueuePrefix(queueId));
                    stateExists = state != null;
                }
                if (!stateExists)
                {
                    batch.delete(segmentHandle, Arrays.copyOf(key, key.length));
                    deletesInBatch++;
                    if (deletesInBatch >= ORPHAN_SEGMENT_DELETE_BATCH_SIZE)
                    {
                        database.write(options, batch);
                        batch.clear();
                        deletesInBatch = 0;
                    }
                }
                segmentIterator.next();
            }

            if (deletesInBatch > 0)
            {
                database.write(options, batch);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to remove orphaned queue segments", e);
        }
    }

    /**
     * Finds the maximum message id present in the store.
     *
     * @return the maximum message id or 0 when no messages exist.
     *
     * @throws StoreException on read failures.
     */
    private long findMaxMessageId() throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle metadataHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
        try (RocksIterator iterator = database.newIterator(metadataHandle))
        {
            iterator.seekToLast();
            if (!iterator.isValid())
            {
                return 0L;
            }
            return decodeMessageKey(iterator.key());
        }
    }

    /**
     * Encodes a message id as a key.
     *
     * @param messageId message id.
     *
     * @return the encoded key.
     */
    private byte[] encodeMessageKey(final long messageId)
    {
        byte[] key = new byte[LONG_BYTES];
        writeLong(key, 0, messageId);
        return key;
    }

    /**
     * Encodes a chunk key.
     *
     * @param messageId message id.
     * @param chunkIndex chunk index.
     *
     * @return the encoded key.
     */
    private byte[] encodeChunkKey(final long messageId, final int chunkIndex)
    {
        byte[] key = new byte[LONG_BYTES + INT_BYTES];
        writeLong(key, 0, messageId);
        writeInt(key, LONG_BYTES, chunkIndex);
        return key;
    }

    /**
     * Encodes a chunk prefix for iteration.
     *
     * @param messageId message id.
     *
     * @return the encoded prefix.
     */
    private byte[] encodeChunkPrefix(final long messageId)
    {
        return encodeMessageKey(messageId);
    }

    /**
     * Decodes a message id from a key.
     *
     * @param key the key bytes.
     *
     * @return the message id.
     */
    private long decodeMessageKey(final byte[] key)
    {
        if (key.length != LONG_BYTES)
        {
            throw new StoreException("Invalid message key length: " + key.length);
        }
        return readLong(key, 0);
    }

    /**
     * Decodes a message id from a chunk key.
     *
     * @param key the chunk key bytes.
     *
     * @return the message id.
     */
    private long decodeChunkMessageId(final byte[] key)
    {
        if (key.length < LONG_BYTES)
        {
            throw new StoreException("Invalid chunk key length: " + key.length);
        }
        return readLong(key, 0);
    }

    /**
     * Encodes a queue segment key.
     *
     * @param queueId queue id.
     * @param segmentNo segment number.
     *
     * @return the encoded key.
     */
    private byte[] encodeQueueSegmentKey(final UUID queueId, final long segmentNo)
    {
        byte[] key = new byte[UUID_BYTES + LONG_BYTES];
        writeLong(key, 0, queueId.getMostSignificantBits());
        writeLong(key, LONG_BYTES, queueId.getLeastSignificantBits());
        writeLong(key, UUID_BYTES, segmentNo);
        return key;
    }

    /**
     * Encodes a queue prefix for iteration.
     *
     * @param queueId queue id.
     *
     * @return the encoded prefix.
     */
    private byte[] encodeQueuePrefix(final UUID queueId)
    {
        byte[] key = new byte[UUID_BYTES];
        writeLong(key, 0, queueId.getMostSignificantBits());
        writeLong(key, LONG_BYTES, queueId.getLeastSignificantBits());
        return key;
    }

    /**
     * Decodes a queue id from a queue entry key.
     *
     * @param key queue entry key.
     *
     * @return the queue id.
     */
    private UUID decodeQueueId(final byte[] key)
    {
        if (key.length != UUID_BYTES + LONG_BYTES)
        {
            throw new StoreException("Invalid queue entry key length: " + key.length);
        }
        return new UUID(readLong(key, 0), readLong(key, LONG_BYTES));
    }

    /**
     * Decodes a segment number from a queue segment key.
     *
     * @param key queue segment key.
     *
     * @return the segment number.
     */
    private long decodeQueueSegmentNo(final byte[] key)
    {
        if (key.length != UUID_BYTES + LONG_BYTES)
        {
            throw new StoreException("Invalid queue segment key length: " + key.length);
        }
        return readLong(key, UUID_BYTES);
    }

    /**
     * Encodes a long value.
     *
     * @param value the long value.
     *
     * @return the encoded bytes.
     */
    private byte[] encodeLong(final long value)
    {
        byte[] data = new byte[LONG_BYTES];
        writeLong(data, 0, value);
        return data;
    }

    /**
     * Decodes a long value.
     *
     * @param data encoded bytes.
     *
     * @return the decoded long value.
     */
    private long decodeLong(final byte[] data)
    {
        if (data.length != LONG_BYTES)
        {
            throw new StoreException("Invalid long value length: " + data.length);
        }
        return readLong(data, 0);
    }

    /**
     * Writes an int value into the array at the offset.
     *
     * @param data data array.
     * @param offset offset to write at.
     * @param value value to write.
     */
    private void writeInt(final byte[] data, final int offset, final int value)
    {
        data[offset] = (byte) (value >>> 24);
        data[offset + 1] = (byte) (value >>> 16);
        data[offset + 2] = (byte) (value >>> 8);
        data[offset + 3] = (byte) value;
    }

    private void writeLong(final byte[] data, final int offset, final long value)
    {
        data[offset] = (byte) (value >>> 56);
        data[offset + 1] = (byte) (value >>> 48);
        data[offset + 2] = (byte) (value >>> 40);
        data[offset + 3] = (byte) (value >>> 32);
        data[offset + 4] = (byte) (value >>> 24);
        data[offset + 5] = (byte) (value >>> 16);
        data[offset + 6] = (byte) (value >>> 8);
        data[offset + 7] = (byte) value;
    }

    /**
     * Reads an int value from the array at the offset.
     *
     * @param data data array.
     * @param offset offset to read from.
     *
     * @return the int value.
     */
    private int readInt(final byte[] data, final int offset)
    {
        return ((data[offset] & 0xff) << 24)
               | ((data[offset + 1] & 0xff) << 16)
               | ((data[offset + 2] & 0xff) << 8)
               | (data[offset + 3] & 0xff);
    }

    private long readLong(final byte[] data, final int offset)
    {
        return ((long) (data[offset] & 0xFF) << 56)
               | ((long) (data[offset + 1] & 0xFF) << 48)
               | ((long) (data[offset + 2] & 0xFF) << 40)
               | ((long) (data[offset + 3] & 0xFF) << 32)
               | ((long) (data[offset + 4] & 0xFF) << 24)
               | ((long) (data[offset + 5] & 0xFF) << 16)
               | ((long) (data[offset + 6] & 0xFF) << 8)
               | ((long) (data[offset + 7] & 0xFF));
    }

    /**
     * Returns whether the key starts with the prefix.
     *
     * @param key key bytes.
     * @param prefix prefix bytes.
     *
     * @return true when the key starts with the prefix.
     */
    private boolean hasPrefix(final byte[] key, final byte[] prefix)
    {
        if (key.length < prefix.length)
        {
            return false;
        }
        for (int i = 0; i < prefix.length; i++)
        {
            if (key[i] != prefix[i])
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Ensures the store is open.
     */
    private void checkOpen()
    {
        if (_environment == null)
        {
            throw new IllegalStateException("Message store is not open");
        }
    }

    private TransactionDB getTransactionDb()
    {
        RocksDB database = _environment.getDatabase();
        if (database instanceof TransactionDB)
        {
            return (TransactionDB) database;
        }
        throw new StoreException("RocksDB environment does not support transactions");
    }

    private boolean isRetryable(final RocksDBException exception)
    {
        Status status = exception.getStatus();
        if (status == null)
        {
            return false;
        }
        Status.Code code = status.getCode();
        return code == Status.Code.Busy
               || code == Status.Code.TryAgain
               || code == Status.Code.TimedOut
               || code == Status.Code.Aborted;
    }

    private void sleepBeforeRetry(final int attempt)
    {
        long delay = _transactionRetryBaseSleepMillis * (1L << attempt);
        try
        {
            Thread.sleep(delay);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns whether content should be stored in chunks.
     *
     * @param contentSize content size.
     *
     * @return true if chunking should be used.
     */
    private boolean shouldChunk(final int contentSize)
    {
        return contentSize > _messageInlineThreshold;
    }

    /**
     * Returns the queue segment number for a message id.
     *
     * @param messageId message id.
     *
     * @return the segment number.
     */
    private long getQueueSegmentNo(final long messageId)
    {
        return messageId >>> _queueSegmentShift;
    }

    /**
     * Applies queue segment settings.
     *
     * @param settings RocksDB settings or null.
     */
    private void applyQueueSegmentSettings(final RocksDBSettings settings)
    {
        int shift = DEFAULT_QUEUE_SEGMENT_SHIFT;
        if (settings != null)
        {
            Integer configuredShift = settings.getQueueSegmentShift();
            if (configuredShift != null && configuredShift > 0 && configuredShift < 32)
            {
                shift = configuredShift;
            }
        }
        _queueSegmentShift = shift;
    }

    private void applyTransactionSettings(final RocksDBSettings settings)
    {
        int retryAttempts = DEFAULT_TRANSACTION_RETRY_ATTEMPTS;
        long retryBaseSleep = DEFAULT_TRANSACTION_RETRY_BASE_SLEEP_MILLIS;
        long lockTimeoutMs = -1L;
        if (settings != null)
        {
            Integer configuredAttempts = settings.getTxnRetryAttempts();
            if (configuredAttempts != null && configuredAttempts > 0)
            {
                retryAttempts = configuredAttempts;
            }
            Long configuredSleep = settings.getTxnRetryBaseSleepMs();
            if (configuredSleep != null && configuredSleep > 0L)
            {
                retryBaseSleep = configuredSleep;
            }
            Long configuredLockTimeout = settings.getTransactionLockTimeout();
            if (configuredLockTimeout != null && configuredLockTimeout > 0L)
            {
                lockTimeoutMs = configuredLockTimeout;
            }
        }
        _transactionRetryAttempts = retryAttempts;
        _transactionRetryBaseSleepMillis = retryBaseSleep;
        _transactionLockTimeoutMs = lockTimeoutMs;
    }

    private void applyCommitterSettings(final RocksDBSettings settings)
    {
        int notifyThreshold = DEFAULT_COMMITTER_NOTIFY_THRESHOLD;
        long waitTimeout = DEFAULT_COMMITTER_WAIT_TIMEOUT_MS;
        if (settings != null)
        {
            Integer configuredThreshold = settings.getCommitterNotifyThreshold();
            if (configuredThreshold != null && configuredThreshold > 0)
            {
                notifyThreshold = configuredThreshold;
            }
            Long configuredTimeout = settings.getCommitterWaitTimeoutMs();
            if (configuredTimeout != null && configuredTimeout > 0L)
            {
                waitTimeout = configuredTimeout;
            }
        }
        _committerNotifyThreshold = notifyThreshold;
        _committerWaitTimeoutMs = waitTimeout;
    }

    private int getMessageIdAllocationSize()
    {
        Integer configured = Integer.getInteger(MESSAGE_ID_ALLOCATION_SIZE_PROPERTY);
        if (configured != null && configured > 0)
        {
            return configured;
        }
        return DEFAULT_MESSAGE_ID_ALLOCATION_SIZE;
    }

    private void applyWriteSettings(final RocksDBSettings settings)
    {
        if (settings == null)
        {
            _writeSync = null;
            _disableWAL = null;
            _walDir = null;
            return;
        }
        _writeSync = settings.getWriteSync();
        _disableWAL = settings.getDisableWAL();
        _walDir = settings.getWalDir();
    }

    private void startAsyncCommitter()
    {
        if (_committer != null)
        {
            return;
        }
        _committer = new RocksDBCommitter(_committerNotifyThreshold,
                                          _committerWaitTimeoutMs,
                                          () -> Boolean.TRUE.equals(_writeSync) && !Boolean.TRUE.equals(_disableWAL),
                                          () -> _environment.getDatabase().flushWal(true),
                                          error ->
                                          {
                                              LOGGER.error("Closing RocksDB message store at {} due to WAL flush failure",
                                                           _storePath, error);
                                              closeMessageStore();
                                          });
        _committer.start();
    }

    private void stopAsyncCommitter()
    {
        if (_committer == null)
        {
            return;
        }
        _committer.stop();
        _committer = null;
    }

    private void applySizeMonitoringSettings(final ConfiguredObject<?> parent)
    {
        if (parent instanceof SizeMonitoringSettings settings)
        {
            _persistentSizeHighThreshold = settings.getStoreOverfullSize();
            _persistentSizeLowThreshold = settings.getStoreUnderfullSize();
            if (_persistentSizeLowThreshold > _persistentSizeHighThreshold || _persistentSizeLowThreshold < 0L)
            {
                _persistentSizeLowThreshold = _persistentSizeHighThreshold;
            }
        }
        else
        {
            _persistentSizeHighThreshold = 0L;
            _persistentSizeLowThreshold = 0L;
        }
    }

    private void storedSizeChangeOccurred(final int delta) throws StoreException
    {
        try
        {
            storedSizeChange(delta);
        }
        catch (RuntimeException e)
        {
            throw new StoreException("Stored size change exception", e);
        }
    }

    private void storedSizeChange(final int delta)
    {
        if (_persistentSizeHighThreshold <= 0L)
        {
            return;
        }
        synchronized (this)
        {
            long newSize = _totalStoreSize += 2L * delta;
            if (!_limitBusted && newSize > _persistentSizeHighThreshold)
            {
                _totalStoreSize = getSizeOnDisk();
                long effectiveSize = Math.max(_totalStoreSize, newSize);
                if (effectiveSize > _persistentSizeHighThreshold)
                {
                    _limitBusted = true;
                    _eventManager.notifyEvent(Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);
                }
            }
            else if (_limitBusted && newSize < _persistentSizeLowThreshold)
            {
                long oldSize = _totalStoreSize;
                _totalStoreSize = getSizeOnDisk();
                if (oldSize <= _totalStoreSize)
                {
                    reduceSizeOnDisk();
                    _totalStoreSize = getSizeOnDisk();
                }
                long effectiveSize = Math.min(_totalStoreSize, newSize);
                if (effectiveSize < _persistentSizeLowThreshold)
                {
                    _limitBusted = false;
                    _eventManager.notifyEvent(Event.PERSISTENT_MESSAGE_SIZE_UNDERFULL);
                }
            }
        }
    }

    private void reduceSizeOnDisk()
    {
        if (_environment != null)
        {
            RocksDBManagementSupport.compactRange(_environment, "");
        }
    }

    private long getSizeOnDisk()
    {
        long estimated = getEstimatedStoreSize();
        if (estimated >= 0L)
        {
            return estimated;
        }
        String dataPath = _storePath;
        long total = sizeOnDisk(dataPath);
        if (_walDir != null && !_walDir.isEmpty() && !_walDir.equals(dataPath))
        {
            total += sizeOnDisk(_walDir);
        }
        return total;
    }

    private long getEstimatedLiveDataSize()
    {
        return getDbPropertyLong("rocksdb.estimate-live-data-size");
    }

    private long getEstimatedStoreSize()
    {
        long memtables = getDbPropertyLong("rocksdb.cur-size-all-mem-tables");
        long totalSst = getDbPropertyLong("rocksdb.total-sst-files-size");
        long liveSst = getDbPropertyLong("rocksdb.live-sst-files-size");
        long estimateLive = getEstimatedLiveDataSize();

        long estimate = -1L;
        if (memtables >= 0L && totalSst >= 0L)
        {
            estimate = memtables + totalSst;
        }
        else if (memtables >= 0L && liveSst >= 0L)
        {
            estimate = memtables + liveSst;
        }
        else if (estimateLive >= 0L)
        {
            estimate = estimateLive;
        }
        return estimate;
    }

    private long getDbPropertyLong(final String property)
    {
        if (_environment == null)
        {
            return -1L;
        }
        String value;
        try
        {
            value = RocksDBManagementSupport.getDbProperty(_environment, property);
        }
        catch (StoreException e)
        {
            return -1L;
        }
        if (value == null || value.isEmpty())
        {
            return -1L;
        }
        try
        {
            return Long.parseLong(value.trim());
        }
        catch (NumberFormatException e)
        {
            return -1L;
        }
    }

    private long sizeOnDisk(final String path)
    {
        if (path == null || path.isEmpty())
        {
            return 0L;
        }
        return sizeOnDisk(new File(path));
    }

    private long sizeOnDisk(final File root)
    {
        if (root == null || !root.exists())
        {
            return 0L;
        }
        if (root.isFile())
        {
            return root.length();
        }
        File[] files = root.listFiles();
        if (files == null)
        {
            return 0L;
        }
        long total = 0L;
        for (File file : files)
        {
            total += sizeOnDisk(file);
        }
        return total;
    }

    private WriteOptions createWriteOptions()
    {
        return createWriteOptions(false);
    }

    private WriteOptions createWriteOptions(final boolean deferSync)
    {
        WriteOptions options = new WriteOptions();
        Boolean sync = _writeSync;
        if (deferSync && Boolean.TRUE.equals(_writeSync) && !Boolean.TRUE.equals(_disableWAL))
        {
            sync = Boolean.FALSE;
        }
        if (sync != null)
        {
            options.setSync(sync);
        }
        if (_disableWAL != null)
        {
            options.setDisableWAL(_disableWAL);
        }
        return options;
    }

    private QueueState loadQueueState(final org.rocksdb.Transaction txn, final UUID queueId)
    {
        ColumnFamilyHandle stateHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_STATE);
        try (ReadOptions readOptions = new ReadOptions())
        {
            byte[] data = txn.getForUpdate(readOptions, stateHandle, encodeQueuePrefix(queueId), true);
            if (data == null)
            {
                return null;
            }
            return RocksDBQueueRecordMapper.decodeQueueState(data);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to load queue state", e);
        }
    }

    private QueueState loadQueueState(final ReadOptions readOptions, final UUID queueId)
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle stateHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_STATE);
        try
        {
            byte[] data = database.get(stateHandle, readOptions, encodeQueuePrefix(queueId));
            if (data == null)
            {
                return null;
            }
            return RocksDBQueueRecordMapper.decodeQueueState(data);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to load queue state", e);
        }
    }

    private QueueSegment loadQueueSegment(final org.rocksdb.Transaction txn,
                                          final UUID queueId,
                                          final long segmentNo)
    {
        ColumnFamilyHandle segmentHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_SEG);
        try (ReadOptions readOptions = new ReadOptions())
        {
            byte[] data = txn.getForUpdate(readOptions,
                                           segmentHandle,
                                           encodeQueueSegmentKey(queueId, segmentNo),
                                           true);
            if (data == null)
            {
                return null;
            }
            return RocksDBQueueRecordMapper.decodeQueueSegment(data);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to load queue segment", e);
        }
    }

    /**
     * Recomputes queue state by scanning existing segments.
     *
     * @param queueId queue id.
     * @param updates pending segment updates.
     * @param deletions pending segment deletions.
     *
     * @return queue state or null.
     */
    private QueueState recomputeQueueState(final UUID queueId,
                                           final ReadOptions readOptions,
                                           final Map<QueueSegmentKey, QueueSegment> updates,
                                           final Set<QueueSegmentKey> deletions)
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle segmentHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_SEG);
        byte[] prefix = encodeQueuePrefix(queueId);
        long head = Long.MAX_VALUE;
        long tail = Long.MIN_VALUE;
        try (RocksIterator iterator = database.newIterator(segmentHandle, readOptions))
        {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next())
            {
                byte[] key = iterator.key();
                if (!hasPrefix(key, prefix))
                {
                    break;
                }
                long segmentNo = decodeQueueSegmentNo(key);
                QueueSegmentKey segmentKey = new QueueSegmentKey(queueId, segmentNo);
                if (deletions.contains(segmentKey))
                {
                    continue;
                }
                head = Math.min(head, segmentNo);
                tail = Math.max(tail, segmentNo);
            }
        }
        for (QueueSegmentKey key : updates.keySet())
        {
            if (key._queueId.equals(queueId))
            {
                head = Math.min(head, key._segmentNo);
                tail = Math.max(tail, key._segmentNo);
            }
        }
        if (head == Long.MAX_VALUE)
        {
            return null;
        }
        return new QueueState(head, tail);
    }

    /**
     * Applies message chunking settings.
     *
     * @param settings RocksDB settings or null.
     */
    private void applyMessageChunkSettings(final RocksDBSettings settings)
    {
        int chunkSize = DEFAULT_CHUNK_SIZE;
        int inlineThreshold = DEFAULT_CHUNK_SIZE;
        if (settings != null)
        {
            Integer configuredChunkSize = settings.getMessageChunkSize();
            if (configuredChunkSize != null && configuredChunkSize > 0)
            {
                chunkSize = configuredChunkSize;
            }
            Integer configuredInlineThreshold = settings.getMessageInlineThreshold();
            if (configuredInlineThreshold != null && configuredInlineThreshold > 0)
            {
                inlineThreshold = configuredInlineThreshold;
            }
            else
            {
                inlineThreshold = chunkSize;
            }
        }
        _messageChunkSize = chunkSize;
        _messageInlineThreshold = inlineThreshold;
    }

    /**
     * Persists message metadata and content.
     *
     * @param messageId message id.
     * @param metaData message metadata.
     * @param content message content.
     *
     * @throws StoreException on write errors.
     */
    private void storeMessage(final org.rocksdb.Transaction txn,
                              final long messageId,
                              final StorableMessageMetaData metaData,
                              final byte[] content) throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle metadataHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
        ColumnFamilyHandle contentHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CONTENT);
        byte[] key = encodeMessageKey(messageId);
        try
        {
            if (txn != null)
            {
                txn.put(metadataHandle, key, encodeMetaData(metaData, false, 0));
                txn.put(contentHandle, key, content == null ? EMPTY_VALUE : content);
            }
            else
            {
                try (WriteBatch batch = new WriteBatch(); WriteOptions options = createWriteOptions())
                {
                    batch.put(metadataHandle, key, encodeMetaData(metaData, false, 0));
                    batch.put(contentHandle, key, content == null ? EMPTY_VALUE : content);
                    database.write(options, batch);
                }
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to store message " + messageId, e);
        }
    }

    /**
     * Persists message metadata only.
     *
     * @param messageId message id.
     * @param metaData message metadata.
     * @param chunked true when content is chunked.
     * @param chunkSize chunk size.
     *
     * @throws StoreException on write errors.
     */
    private void storeMessageMetadata(final org.rocksdb.Transaction txn,
                                      final long messageId,
                                      final StorableMessageMetaData metaData,
                                      final boolean chunked,
                                      final int chunkSize) throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle metadataHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
        byte[] key = encodeMessageKey(messageId);
        try
        {
            if (txn != null)
            {
                txn.put(metadataHandle, key, encodeMetaData(metaData, chunked, chunkSize));
            }
            else
            {
                database.put(metadataHandle, key, encodeMetaData(metaData, chunked, chunkSize));
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to store message metadata " + messageId, e);
        }
    }

    private void storeChunkedMessage(final org.rocksdb.Transaction txn,
                                     final long messageId,
                                     final StorableMessageMetaData metaData,
                                     final int chunkSize,
                                     final List<byte[]> chunks,
                                     final byte[] trailingChunk) throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle metadataHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
        ColumnFamilyHandle chunkHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CHUNKS);
        byte[] metadataKey = encodeMessageKey(messageId);
        try
        {
            if (txn != null)
            {
                int chunkIndex = 0;
                if (chunks != null)
                {
                    for (byte[] chunk : chunks)
                    {
                        txn.put(chunkHandle, encodeChunkKey(messageId, chunkIndex), chunk);
                        chunkIndex++;
                    }
                }
                if (trailingChunk != null)
                {
                    txn.put(chunkHandle, encodeChunkKey(messageId, chunkIndex), trailingChunk);
                }
                txn.put(metadataHandle, metadataKey, encodeMetaData(metaData, true, chunkSize));
            }
            else
            {
                try (WriteBatch batch = new WriteBatch();
                     WriteOptions options = createWriteOptions())
                {
                    int chunkIndex = 0;
                    if (chunks != null)
                    {
                        for (byte[] chunk : chunks)
                        {
                            batch.put(chunkHandle, encodeChunkKey(messageId, chunkIndex), chunk);
                            chunkIndex++;
                        }
                    }
                    if (trailingChunk != null)
                    {
                        batch.put(chunkHandle, encodeChunkKey(messageId, chunkIndex), trailingChunk);
                    }
                    batch.put(metadataHandle, metadataKey, encodeMetaData(metaData, true, chunkSize));
                    database.write(options, batch);
                }
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to store chunked message " + messageId, e);
        }
    }

    /**
     * Deletes message metadata and content.
     *
     * @param messageId message id.
     *
     * @throws StoreException on delete errors.
     */
    private void deleteMessage(final long messageId) throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle metadataHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
        ColumnFamilyHandle contentHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CONTENT);
        ColumnFamilyHandle chunkHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CHUNKS);
        byte[] key = encodeMessageKey(messageId);
        byte[] prefix = encodeChunkPrefix(messageId);
        try (WriteBatch batch = new WriteBatch();
             WriteOptions options = createWriteOptions();
             RocksIterator iterator = database.newIterator(chunkHandle))
        {
            batch.delete(metadataHandle, key);
            batch.delete(contentHandle, key);
            for (iterator.seek(prefix); iterator.isValid(); iterator.next())
            {
                byte[] chunkKey = iterator.key();
                if (!hasPrefix(chunkKey, prefix))
                {
                    break;
                }
                batch.delete(chunkHandle, Arrays.copyOf(chunkKey, chunkKey.length));
            }
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to delete message " + messageId, e);
        }
    }

    /**
     * Loads message metadata from RocksDB.
     *
     * @param messageId message id.
     *
     * @return the metadata.
     */
    private MetaDataRecord loadMetaDataRecord(final long messageId)
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle metadataHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
        try
        {
            byte[] data = database.get(metadataHandle, encodeMessageKey(messageId));
            if (data == null)
            {
                return null;
            }
            return decodeMetaDataRecord(data);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to load message metadata " + messageId, e);
        }
    }

    private MetaDataRecord loadMetaDataRecord(final ReadOptions readOptions, final long messageId)
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle metadataHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
        try
        {
            byte[] data = database.get(metadataHandle, readOptions, encodeMessageKey(messageId));
            if (data == null)
            {
                return null;
            }
            return decodeMetaDataRecord(data);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to load message metadata " + messageId, e);
        }
    }

    /**
     * Loads message content from RocksDB.
     *
     * @param messageId message id.
     *
     * @return the content bytes.
     */
    private byte[] loadContent(final long messageId)
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle contentHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CONTENT);
        try
        {
            byte[] data = database.get(contentHandle, encodeMessageKey(messageId));
            return data == null ? EMPTY_VALUE : data;
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to load message content " + messageId, e);
        }
    }

    /**
     * Stores a message content chunk.
     *
     * @param messageId message id.
     * @param chunkIndex chunk index.
     * @param data chunk data.
     */
    private void storeChunk(final org.rocksdb.Transaction txn,
                            final long messageId,
                            final int chunkIndex,
                            final byte[] data)
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle chunkHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CHUNKS);
        try
        {
            if (txn != null)
            {
                txn.put(chunkHandle, encodeChunkKey(messageId, chunkIndex), data);
            }
            else
            {
                database.put(chunkHandle, encodeChunkKey(messageId, chunkIndex), data);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to store message chunk " + messageId + ":" + chunkIndex, e);
        }
    }

    /**
     * Loads a message content chunk.
     *
     * @param messageId message id.
     * @param chunkIndex chunk index.
     *
     * @return the chunk bytes.
     */
    private byte[] loadChunk(final long messageId, final int chunkIndex)
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle chunkHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CHUNKS);
        try
        {
            byte[] data = database.get(chunkHandle, encodeChunkKey(messageId, chunkIndex));
            return data == null ? EMPTY_VALUE : data;
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to load message chunk " + messageId + ":" + chunkIndex, e);
        }
    }

    /**
     * Loads chunked content slice.
     *
     * @param messageId message id.
     * @param contentSize content size.
     * @param chunkSize chunk size.
     * @param offset offset in content.
     * @param length requested length or Integer.MAX_VALUE.
     *
     * @return the content slice bytes.
     */
    private byte[] loadChunkedContentSlice(final long messageId,
                                           final int contentSize,
                                           final int chunkSize,
                                           final int offset,
                                           final int length)
    {
        if (chunkSize <= 0)
        {
            return EMPTY_VALUE;
        }
        if (offset >= contentSize)
        {
            return EMPTY_VALUE;
        }
        int available = contentSize - offset;
        int size = length == Integer.MAX_VALUE ? available : Math.min(length, available);
        if (size <= 0)
        {
            return EMPTY_VALUE;
        }

        int startChunk = offset / chunkSize;
        int endChunk = (offset + size - 1) / chunkSize;
        int maxChunkIndex = (contentSize - 1) / chunkSize;
        int remaining = size;
        byte[] result = new byte[size];
        int destPos = 0;

        for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++)
        {
            byte[] chunk = loadChunk(messageId, chunkIndex);
            int expectedLength = chunkIndex == maxChunkIndex
                    ? (contentSize - (chunkIndex * chunkSize))
                    : chunkSize;
            if (chunk.length < expectedLength)
            {
                throw new StoreException("Missing or truncated message chunk "
                                         + messageId + ":" + chunkIndex
                                         + " (expected at least " + expectedLength
                                         + " bytes, got " + chunk.length + ")");
            }
            int chunkOffset = chunkIndex == startChunk ? (offset % chunkSize) : 0;
            int chunkAvailable = chunk.length - chunkOffset;
            int copySize = Math.min(remaining, chunkAvailable);
            if (copySize > 0)
            {
                System.arraycopy(chunk, chunkOffset, result, destPos, copySize);
                destPos += copySize;
                remaining -= copySize;
            }
        }
        return result;
    }

    /**
     * Encodes message metadata.
     *
     * @param metaData message metadata.
     *
     * @return encoded metadata bytes.
     */
    private byte[] encodeMetaData(final StorableMessageMetaData metaData,
                                  final boolean chunked,
                                  final int chunkSize)
    {
        int payloadSize = 1 + metaData.getStorableSize();
        byte[] data = new byte[METADATA_HEADER_BYTES + payloadSize];
        data[0] = (byte) (METADATA_FORMAT_MARKER | METADATA_FORMAT_VERSION);
        data[1] = chunked ? METADATA_FLAG_CHUNKED : 0;
        writeInt(data, 2, chunkSize);
        data[METADATA_HEADER_BYTES] = (byte) metaData.getType().ordinal();
        try (QpidByteBuffer buffer = QpidByteBuffer.wrap(data))
        {
            buffer.position(METADATA_HEADER_BYTES + 1);
            metaData.writeToBuffer(buffer);
        }
        return data;
    }

    /**
     * Decodes message metadata record.
     *
     * @param data encoded metadata bytes.
     *
     * @return the decoded metadata record.
     */
    private MetaDataRecord decodeMetaDataRecord(final byte[] data)
    {
        if (data.length == 0)
        {
            throw new StoreException("Empty metadata record");
        }
        int offset = 0;
        boolean chunked = false;
        int chunkSize = 0;
        int first = data[0] & 0xff;
        if ((first & 0x80) != 0)
        {
            int version = first & 0x7f;
            if (version != METADATA_FORMAT_VERSION)
            {
                throw new StoreException("Unsupported metadata version " + version);
            }
            if (data.length < METADATA_HEADER_BYTES + 1)
            {
                throw new StoreException("Invalid metadata record length: " + data.length);
            }
            chunked = (data[1] & METADATA_FLAG_CHUNKED) != 0;
            chunkSize = readInt(data, 2);
            offset = METADATA_HEADER_BYTES;
        }
        int ordinal = data[offset] & 0xff;
        MessageMetaDataType<?> type = MessageMetaDataTypeRegistry.fromOrdinal(ordinal);
        int payloadOffset = offset + 1;
        int payloadLength = data.length - payloadOffset;
        try (QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(payloadLength))
        {
            // Align with BDB by decoding metadata from a direct buffer.
            buffer.put(data, payloadOffset, payloadLength);
            buffer.flip();
            StorableMessageMetaData metaData = type.createMetaData(buffer);
            return new MetaDataRecord(metaData, chunked, chunkSize);
        }
    }

    /**
     * Notifies message deletion listeners.
     *
     * @param message deleted message.
     */
    private void notifyMessageDeleted(final StoredMessage<?> message)
    {
        for (MessageDeleteListener listener : _messageDeleteListeners)
        {
            listener.messageDeleted(message);
        }
    }

    /**
     * Stores XID records from a transaction.
     *
     * @param xidRecords xid records.
     * @param deletions xid deletions.
     *
     * @throws StoreException on store errors.
     */
    private void applyXidRecordsToTransaction(final org.rocksdb.Transaction txn,
                                              final List<XidRecord> xidRecords,
                                              final List<byte[]> deletions) throws StoreException
    {
        if (xidRecords.isEmpty() && deletions.isEmpty())
        {
            return;
        }
        ColumnFamilyHandle xidHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.XIDS);
        try
        {
            for (XidRecord record : xidRecords)
            {
                txn.put(xidHandle, record.getKey(), record.getValue());
            }
            for (byte[] key : deletions)
            {
                txn.delete(xidHandle, key);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to persist XID records", e);
        }
    }

    /**
     * Stores queue entry updates.
     *
     * @param enqueues enqueue records.
     * @param dequeues dequeue records.
     *
     * @throws StoreException on store errors.
     */
    private void applyQueueEntriesToTransaction(final org.rocksdb.Transaction txn,
                                                final List<RocksDBEnqueueRecord> enqueues,
                                                final List<RocksDBEnqueueRecord> dequeues) throws StoreException
    {
        if (enqueues.isEmpty() && dequeues.isEmpty())
        {
            return;
        }
        ColumnFamilyHandle segmentHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_SEG);
        ColumnFamilyHandle stateHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_STATE);

        Map<QueueSegmentKey, QueueSegment> segmentUpdates = new HashMap<>();
        Set<QueueSegmentKey> segmentDeletions = new HashSet<>();
        Map<UUID, QueueState> queueStates = new HashMap<>();

        for (RocksDBEnqueueRecord record : enqueues)
        {
            UUID queueId = record.getQueueId();
            long messageId = record.getMessageNumber();
            long segmentNo = getQueueSegmentNo(messageId);
            QueueSegmentKey segmentKey = new QueueSegmentKey(queueId, segmentNo);
            QueueSegment segment = segmentUpdates.get(segmentKey);
            if (segment == null)
            {
                segment = loadQueueSegment(txn, queueId, segmentNo);
            }
            boolean newSegment = false;
            if (segment == null)
            {
                segment = new QueueSegment(new long[]{messageId}, new BitSet(), 1, 0);
                newSegment = true;
            }
            else
            {
                long[] messageIds = segment._messageIds;
                int index = Arrays.binarySearch(messageIds, messageId);
                if (index >= 0)
                {
                    segmentUpdates.put(segmentKey, segment);
                    continue;
                }
                int insertIndex = -index - 1;
                long[] updated = new long[segment._entryCount + 1];
                System.arraycopy(messageIds, 0, updated, 0, insertIndex);
                updated[insertIndex] = messageId;
                System.arraycopy(messageIds, insertIndex, updated, insertIndex + 1, segment._entryCount - insertIndex);
                segment._messageIds = updated;
                if (insertIndex < segment._entryCount)
                {
                    segment._acked = RocksDBQueueRecordMapper.insertAckBit(segment._acked, insertIndex);
                }
                segment._entryCount++;
                logLargeQueueSegment(queueId, segmentNo, segment._entryCount);
            }
            segmentUpdates.put(segmentKey, segment);

            QueueState state = queueStates.get(queueId);
            if (state == null)
            {
                state = loadQueueState(txn, queueId);
                if (state == null)
                {
                    state = new QueueState(segmentNo, segmentNo);
                }
                queueStates.put(queueId, state);
            }
            if (newSegment)
            {
                state._headSegment = Math.min(state._headSegment, segmentNo);
                state._tailSegment = Math.max(state._tailSegment, segmentNo);
            }
        }

        for (RocksDBEnqueueRecord record : dequeues)
        {
            UUID queueId = record.getQueueId();
            long messageId = record.getMessageNumber();
            long segmentNo = getQueueSegmentNo(messageId);
            QueueSegmentKey segmentKey = new QueueSegmentKey(queueId, segmentNo);
            QueueSegment segment = segmentUpdates.get(segmentKey);
            if (segment == null && !segmentDeletions.contains(segmentKey))
            {
                segment = loadQueueSegment(txn, queueId, segmentNo);
            }
            if (segment == null)
            {
                continue;
            }
            int index = Arrays.binarySearch(segment._messageIds, messageId);
            if (index < 0 || segment._acked.get(index))
            {
                continue;
            }
            segment._acked.set(index);
            segment._ackedCount++;
            if (segment._ackedCount >= segment._entryCount)
            {
                segmentUpdates.remove(segmentKey);
                segmentDeletions.add(segmentKey);

                QueueState state = queueStates.get(queueId);
                if (state == null)
                {
                    state = loadQueueState(txn, queueId);
                    if (state == null)
                    {
                        continue;
                    }
                    queueStates.put(queueId, state);
                }
                if (state._headSegment == segmentNo)
                {
                    state._headDirty = true;
                }
                if (state._tailSegment == segmentNo)
                {
                    state._tailDirty = true;
                }
            }
            else
            {
                segmentUpdates.put(segmentKey, segment);
            }
        }

        RocksDB database = _environment.getDatabase();
        Snapshot snapshot = database.getSnapshot();
        try (ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot))
        {
            for (Map.Entry<UUID, QueueState> entry : queueStates.entrySet())
            {
                QueueState state = entry.getValue();
                if (state._headDirty || state._tailDirty)
                {
                    QueueState recalculated =
                            recomputeQueueState(entry.getKey(), readOptions, segmentUpdates, segmentDeletions);
                    if (recalculated == null)
                    {
                        state._exists = false;
                    }
                    else
                    {
                        state._headSegment = recalculated._headSegment;
                        state._tailSegment = recalculated._tailSegment;
                        state._exists = true;
                    }
                }
            }
        }
        finally
        {
            database.releaseSnapshot(snapshot);
        }

        if (segmentUpdates.isEmpty() && segmentDeletions.isEmpty() && queueStates.isEmpty())
        {
            return;
        }
        try
        {
            for (Map.Entry<QueueSegmentKey, QueueSegment> entry : segmentUpdates.entrySet())
            {
                QueueSegmentKey key = entry.getKey();
                if (!segmentDeletions.contains(key))
                {
                    txn.put(segmentHandle,
                            encodeQueueSegmentKey(key._queueId, key._segmentNo),
                            RocksDBQueueRecordMapper.encodeQueueSegment(entry.getValue()));
                }
            }
            for (QueueSegmentKey key : segmentDeletions)
            {
                txn.delete(segmentHandle, encodeQueueSegmentKey(key._queueId, key._segmentNo));
            }
            for (Map.Entry<UUID, QueueState> entry : queueStates.entrySet())
            {
                UUID queueId = entry.getKey();
                QueueState state = entry.getValue();
                if (state._exists)
                {
                    txn.put(stateHandle, encodeQueuePrefix(queueId),
                            RocksDBQueueRecordMapper.encodeQueueState(state._headSegment, state._tailSegment));
                }
                else
                {
                    txn.delete(stateHandle, encodeQueuePrefix(queueId));
                }
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to persist queue segments", e);
        }
    }

    private void logLargeQueueSegment(final UUID queueId, final long segmentNo, final int entryCount)
    {
        if (entryCount < QUEUE_SEGMENT_WARN_THRESHOLD)
        {
            return;
        }
        if (entryCount == QUEUE_SEGMENT_WARN_THRESHOLD || entryCount % QUEUE_SEGMENT_WARN_THRESHOLD == 0)
        {
            LOGGER.warn("Queue segment size {} exceeded threshold {} for queue {} segment {} (queueSegmentShift={})",
                        entryCount,
                        QUEUE_SEGMENT_WARN_THRESHOLD,
                        queueId,
                        segmentNo,
                        _queueSegmentShift);
        }
    }

    /**
     * Reads data from the RocksDB message store.
     * <br>
     * Thread-safety: not thread-safe.
     */
    private final class RocksDBMessageStoreReader implements MessageStoreReader
    {
        private final RocksDB _database;

        private RocksDBMessageStoreReader()
        {
            _database = _environment.getDatabase();
        }

        /**
         * Visits all stored messages.
         *
         * @param handler message handler.
         *
         * @throws StoreException on read errors.
         */
        @Override
        public void visitMessages(final MessageHandler handler) throws StoreException
        {
            ColumnFamilyHandle metadataHandle =
                    _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
            Snapshot snapshot = _database.getSnapshot();
            try (ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
                 RocksIterator iterator = _database.newIterator(metadataHandle, readOptions))
            {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                {
                    long messageId = decodeMessageKey(iterator.key());
                    MetaDataRecord record = decodeMetaDataRecord(iterator.value());
                    StoredMessage<?> message = new RocksDBStoredMessage<>(_storedMessageAccess, messageId, record, true);
                    if (!handler.handle(message))
                    {
                        return;
                    }
                }
            }
            finally
            {
                _database.releaseSnapshot(snapshot);
            }
        }

        /**
         * Visits all message instances.
         *
         * @param handler message instance handler.
         *
         * @throws StoreException on read errors.
         */
        @Override
        public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
        {
            ColumnFamilyHandle segmentHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_SEG);
            Snapshot snapshot = _database.getSnapshot();
            try (ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
                 RocksIterator iterator = _database.newIterator(segmentHandle, readOptions))
            {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                {
                    byte[] key = iterator.key();
                    UUID queueId = decodeQueueId(key);
                    QueueSegment segment = RocksDBQueueRecordMapper.decodeQueueSegment(iterator.value());
                    for (int i = 0; i < segment._entryCount; i++)
                    {
                        if (!segment._acked.get(i))
                        {
                            if (!handler.handle(new RocksDBEnqueueRecord(queueId, segment._messageIds[i])))
                            {
                                return;
                            }
                        }
                    }
                }
            }
            finally
            {
                _database.releaseSnapshot(snapshot);
            }
        }

        /**
         * Visits message instances for a queue.
         *
         * @param queue queue resource.
         * @param handler message instance handler.
         *
         * @throws StoreException on read errors.
         */
        @Override
        public void visitMessageInstances(final TransactionLogResource queue,
                                          final MessageInstanceHandler handler) throws StoreException
        {
            ColumnFamilyHandle segmentHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.Q_SEG);
            byte[] prefix = encodeQueuePrefix(queue.getId());
            Snapshot snapshot = _database.getSnapshot();
            try (ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
                 RocksIterator iterator = _database.newIterator(segmentHandle, readOptions))
            {
                QueueState state = loadQueueState(readOptions, queue.getId());
                byte[] startKey = state == null ? prefix : encodeQueueSegmentKey(queue.getId(), state._headSegment);
                for (iterator.seek(startKey); iterator.isValid(); iterator.next())
                {
                    byte[] key = iterator.key();
                    if (!hasPrefix(key, prefix))
                    {
                        break;
                    }
                    QueueSegment segment = RocksDBQueueRecordMapper.decodeQueueSegment(iterator.value());
                    for (int i = 0; i < segment._entryCount; i++)
                    {
                        if (!segment._acked.get(i))
                        {
                            if (!handler.handle(new RocksDBEnqueueRecord(queue.getId(), segment._messageIds[i])))
                            {
                                return;
                            }
                        }
                    }
                }
            }
            finally
            {
                _database.releaseSnapshot(snapshot);
            }
        }

        /**
         * Visits distributed transactions.
         *
         * @param handler transaction handler.
         *
         * @throws StoreException on read errors.
         */
        @Override
        public void visitDistributedTransactions(final DistributedTransactionHandler handler) throws StoreException
        {
            ColumnFamilyHandle xidHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.XIDS);
            Snapshot snapshot = _database.getSnapshot();
            try (ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
                 RocksIterator iterator = _database.newIterator(xidHandle, readOptions))
            {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                {
                    XidRecordData data = RocksDBXidRecordMapper.decodeXidValue(iterator.value(),
                                                                              _xidRecordFactory);
                    StoredXidRecord xid = data.getXid();
                    XidActions actions = data.getActions();
                    Transaction.EnqueueRecord[] enqueues =
                            actions.getEnqueues().toArray(new Transaction.EnqueueRecord[0]);
                    Transaction.DequeueRecord[] dequeues =
                            actions.getDequeues().toArray(new Transaction.DequeueRecord[0]);
                    if (!handler.handle(xid, enqueues, dequeues))
                    {
                        return;
                    }
                }
            }
            finally
            {
                _database.releaseSnapshot(snapshot);
            }
        }

        /**
         * Returns a stored message by id.
         *
         * @param messageId message id.
         *
         * @return the stored message or null.
         */
        @Override
        public StoredMessage<?> getMessage(final long messageId)
        {
            Snapshot snapshot = _database.getSnapshot();
            try (ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot))
            {
                MetaDataRecord record = loadMetaDataRecord(readOptions, messageId);
                if (record == null)
                {
                    return null;
                }
                return new RocksDBStoredMessage<>(_storedMessageAccess, messageId, record, true);
            }
            finally
            {
                _database.releaseSnapshot(snapshot);
            }
        }

        /**
         * Closes the reader.
         */
        @Override
        public void close()
        {
        }
    }
}
