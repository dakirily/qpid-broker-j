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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.FileBasedSettings;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageMetaDataTypeRegistry;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.DurableConfigurationStore;
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
 *
 * Thread-safety: safe for concurrent access by the configuration model.
 */
public class RocksDBMessageStore implements MessageStore
{
    private static final byte[] NEXT_MESSAGE_ID_KEY = "nextMessageId".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] EMPTY_VALUE = new byte[0];
    private static final int LONG_BYTES = 8;
    private static final int UUID_BYTES = 16;
    private static final byte ACTION_ENQUEUE = 'E';
    private static final byte ACTION_DEQUEUE = 'D';

    private final AtomicLong _messageId = new AtomicLong(0);
    private final AtomicLong _inMemorySize = new AtomicLong(0);
    private final AtomicLong _bytesEvacuatedFromMemory = new AtomicLong(0);
    private final Set<MessageDeleteListener> _messageDeleteListeners = ConcurrentHashMap.newKeySet();
    private final Object _transactionLock = new Object();

    private RocksDBEnvironment _environment;
    private boolean _environmentOwned;
    private String _storePath;

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
        long id = _messageId.incrementAndGet();
        storeNextMessageId(id);
        return id;
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
        loadNextMessageId();
    }

    /**
     * Upgrades the store structure if required.
     *
     * @throws StoreException on store errors.
     */
    @Override
    public void upgradeStoreStructure() throws StoreException
    {
        // No-op for initial RocksDB implementation.
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
        return new RocksDBStoredMessage<>(messageId, metaData);
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
        return new RocksDBTransaction();
    }

    /**
     * Closes the message store.
     *
     */
    @Override
    public void closeMessageStore()
    {
        if (_environmentOwned && _environment != null)
        {
            _environment.close();
        }
        _environment = null;
        _environmentOwned = false;
        _inMemorySize.set(0L);
        _bytesEvacuatedFromMemory.set(0L);
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
                _messageId.set(decodeLong(value));
            }
            else
            {
                long maxId = findMaxMessageId();
                _messageId.set(maxId);
                storeNextMessageId(maxId);
            }
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to load next message id", e);
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
        long maxId = 0L;
        try (RocksIterator iterator = database.newIterator(metadataHandle))
        {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
            {
                long id = decodeMessageKey(iterator.key());
                if (id > maxId)
                {
                    maxId = id;
                }
            }
        }
        return maxId;
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
        ByteBuffer buffer = ByteBuffer.allocate(LONG_BYTES);
        buffer.putLong(messageId);
        return buffer.array();
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
        return ByteBuffer.wrap(key).getLong();
    }

    /**
     * Encodes a queue entry key.
     *
     * @param queueId queue id.
     * @param messageId message id.
     *
     * @return the encoded key.
     */
    private byte[] encodeQueueEntryKey(final UUID queueId, final long messageId)
    {
        ByteBuffer buffer = ByteBuffer.allocate(UUID_BYTES + LONG_BYTES);
        buffer.putLong(queueId.getMostSignificantBits());
        buffer.putLong(queueId.getLeastSignificantBits());
        buffer.putLong(messageId);
        return buffer.array();
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
        ByteBuffer buffer = ByteBuffer.allocate(UUID_BYTES);
        buffer.putLong(queueId.getMostSignificantBits());
        buffer.putLong(queueId.getLeastSignificantBits());
        return buffer.array();
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
        ByteBuffer buffer = ByteBuffer.wrap(key);
        long most = buffer.getLong();
        long least = buffer.getLong();
        return new UUID(most, least);
    }

    /**
     * Decodes a message id from a queue entry key.
     *
     * @param key queue entry key.
     *
     * @return the message id.
     */
    private long decodeQueueMessageId(final byte[] key)
    {
        if (key.length != UUID_BYTES + LONG_BYTES)
        {
            throw new StoreException("Invalid queue entry key length: " + key.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(key);
        buffer.position(UUID_BYTES);
        return buffer.getLong();
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
        ByteBuffer buffer = ByteBuffer.allocate(LONG_BYTES);
        buffer.putLong(value);
        return buffer.array();
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
        return ByteBuffer.wrap(data).getLong();
    }

    /**
     * Encodes an XID key.
     *
     * @param format XID format.
     * @param globalId global id bytes.
     * @param branchId branch id bytes.
     *
     * @return the encoded key.
     */
    private byte[] encodeXidKey(final long format, final byte[] globalId, final byte[] branchId)
    {
        try
        {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(stream))
            {
                output.writeLong(format);
                writeBytes(output, globalId);
                writeBytes(output, branchId);
            }
            return stream.toByteArray();
        }
        catch (IOException e)
        {
            throw new StoreException("Failed to encode XID key", e);
        }
    }

    /**
     * Decodes an XID key.
     *
     * @param key encoded key.
     *
     * @return the stored XID record.
     */
    private StoredXidRecord decodeXidKey(final byte[] key)
    {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(key)))
        {
            long format = input.readLong();
            byte[] globalId = readBytes(input);
            byte[] branchId = readBytes(input);
            return new RocksDBStoredXidRecord(format, globalId, branchId);
        }
        catch (IOException e)
        {
            throw new StoreException("Failed to decode XID key", e);
        }
    }

    /**
     * Encodes XID actions.
     *
     * @param enqueues enqueue records.
     * @param dequeues dequeue records.
     *
     * @return the encoded actions.
     */
    private byte[] encodeXidValue(final Transaction.EnqueueRecord[] enqueues,
                                  final Transaction.DequeueRecord[] dequeues)
    {
        int enqueueCount = enqueues == null ? 0 : enqueues.length;
        int dequeueCount = dequeues == null ? 0 : dequeues.length;
        try
        {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(stream))
            {
                output.writeInt(enqueueCount);
                if (enqueueCount > 0)
                {
                    for (Transaction.EnqueueRecord record : enqueues)
                    {
                        writeAction(output, ACTION_ENQUEUE, record.getResource().getId(),
                                    record.getMessage().getMessageNumber());
                    }
                }
                output.writeInt(dequeueCount);
                if (dequeueCount > 0)
                {
                    for (Transaction.DequeueRecord record : dequeues)
                    {
                        MessageEnqueueRecord enqueueRecord = record.getEnqueueRecord();
                        writeAction(output, ACTION_DEQUEUE, enqueueRecord.getQueueId(),
                                    enqueueRecord.getMessageNumber());
                    }
                }
            }
            return stream.toByteArray();
        }
        catch (IOException e)
        {
            throw new StoreException("Failed to encode XID actions", e);
        }
    }

    /**
     * Decodes XID actions.
     *
     * @param data encoded actions.
     *
     * @return decoded actions.
     */
    private XidActions decodeXidValue(final byte[] data)
    {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(data)))
        {
            int enqueueCount = input.readInt();
            List<RecordImpl> enqueues = new ArrayList<>(enqueueCount);
            for (int i = 0; i < enqueueCount; i++)
            {
                enqueues.add(readAction(input));
            }
            int dequeueCount = input.readInt();
            List<RecordImpl> dequeues = new ArrayList<>(dequeueCount);
            for (int i = 0; i < dequeueCount; i++)
            {
                dequeues.add(readAction(input));
            }
            return new XidActions(enqueues, dequeues);
        }
        catch (IOException e)
        {
            throw new StoreException("Failed to decode XID actions", e);
        }
    }

    /**
     * Writes an XID action record.
     *
     * @param output output stream.
     * @param action action type.
     * @param queueId queue id.
     * @param messageId message id.
     *
     * @throws IOException on write errors.
     */
    private void writeAction(final DataOutputStream output,
                             final byte action,
                             final UUID queueId,
                             final long messageId) throws IOException
    {
        output.writeByte(action);
        output.writeLong(queueId.getMostSignificantBits());
        output.writeLong(queueId.getLeastSignificantBits());
        output.writeLong(messageId);
    }

    /**
     * Reads an XID action record.
     *
     * @param input input stream.
     *
     * @return the action record.
     *
     * @throws IOException on read errors.
     */
    private RecordImpl readAction(final DataInputStream input) throws IOException
    {
        byte action = input.readByte();
        long most = input.readLong();
        long least = input.readLong();
        long messageId = input.readLong();
        if (action != ACTION_ENQUEUE && action != ACTION_DEQUEUE)
        {
            throw new StoreException("Unknown XID action: " + action);
        }
        return new RecordImpl(new UUID(most, least), messageId);
    }

    /**
     * Writes a byte array with length.
     *
     * @param output output stream.
     * @param data data bytes.
     *
     * @throws IOException on write errors.
     */
    private void writeBytes(final DataOutputStream output, final byte[] data) throws IOException
    {
        if (data == null)
        {
            output.writeInt(0);
            return;
        }
        output.writeInt(data.length);
        output.write(data);
    }

    /**
     * Reads a byte array with length.
     *
     * @param input input stream.
     *
     * @return the read bytes.
     *
     * @throws IOException on read errors.
     */
    private byte[] readBytes(final DataInputStream input) throws IOException
    {
        int length = input.readInt();
        if (length == 0)
        {
            return new byte[0];
        }
        byte[] data = new byte[length];
        input.readFully(data);
        return data;
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

    /**
     * Persists message metadata and content.
     *
     * @param messageId message id.
     * @param metaData message metadata.
     * @param content message content.
     *
     * @throws StoreException on write errors.
     */
    private void storeMessage(final long messageId,
                              final StorableMessageMetaData metaData,
                              final byte[] content) throws StoreException
    {
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle metadataHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
        ColumnFamilyHandle contentHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_CONTENT);
        byte[] key = encodeMessageKey(messageId);
        try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions())
        {
            batch.put(metadataHandle, key, encodeMetaData(metaData));
            batch.put(contentHandle, key, content == null ? EMPTY_VALUE : content);
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to store message " + messageId, e);
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
        byte[] key = encodeMessageKey(messageId);
        try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions())
        {
            batch.delete(metadataHandle, key);
            batch.delete(contentHandle, key);
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
    private StorableMessageMetaData loadMetaData(final long messageId)
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
            return decodeMetaData(data);
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
     * Encodes message metadata.
     *
     * @param metaData message metadata.
     *
     * @return encoded metadata bytes.
     */
    private byte[] encodeMetaData(final StorableMessageMetaData metaData)
    {
        int bodySize = 1 + metaData.getStorableSize();
        byte[] data = new byte[bodySize];
        data[0] = (byte) metaData.getType().ordinal();
        try (QpidByteBuffer buffer = QpidByteBuffer.wrap(data))
        {
            buffer.position(1);
            metaData.writeToBuffer(buffer);
        }
        return data;
    }

    /**
     * Decodes message metadata.
     *
     * @param data encoded metadata bytes.
     *
     * @return the decoded metadata.
     */
    private StorableMessageMetaData decodeMetaData(final byte[] data)
    {
        if (data.length == 0)
        {
            throw new StoreException("Empty metadata record");
        }
        int ordinal = data[0] & 0xff;
        MessageMetaDataType type = MessageMetaDataTypeRegistry.fromOrdinal(ordinal);
        try (QpidByteBuffer buffer = QpidByteBuffer.wrap(data, 1, data.length - 1))
        {
            return type.createMetaData(buffer);
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
    private void storeXidRecords(final List<XidRecord> xidRecords,
                                 final List<byte[]> deletions) throws StoreException
    {
        if (xidRecords.isEmpty() && deletions.isEmpty())
        {
            return;
        }
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle xidHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.XIDS);
        try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions())
        {
            for (XidRecord record : xidRecords)
            {
                batch.put(xidHandle, record.getKey(), record.getValue());
            }
            for (byte[] key : deletions)
            {
                batch.delete(xidHandle, key);
            }
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to store XID records", e);
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
    private void storeQueueEntries(final List<RocksDBEnqueueRecord> enqueues,
                                   final List<RocksDBEnqueueRecord> dequeues) throws StoreException
    {
        if (enqueues.isEmpty() && dequeues.isEmpty())
        {
            return;
        }
        RocksDB database = _environment.getDatabase();
        ColumnFamilyHandle queueHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.QUEUE_ENTRIES);
        try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions())
        {
            for (RocksDBEnqueueRecord record : enqueues)
            {
                batch.put(queueHandle, encodeQueueEntryKey(record.getQueueId(), record.getMessageNumber()),
                          EMPTY_VALUE);
            }
            for (RocksDBEnqueueRecord record : dequeues)
            {
                batch.delete(queueHandle, encodeQueueEntryKey(record.getQueueId(), record.getMessageNumber()));
            }
            database.write(options, batch);
        }
        catch (RocksDBException e)
        {
            throw new StoreException("Failed to store queue entries", e);
        }
    }

    /**
     * Ensures messages referenced by enqueues are stored.
     *
     * @param enqueues enqueue records.
     */
    private void ensureMessagesStored(final Transaction.EnqueueRecord[] enqueues)
    {
        if (enqueues == null)
        {
            return;
        }
        for (Transaction.EnqueueRecord record : enqueues)
        {
            EnqueueableMessage message = record.getMessage();
            if (message != null && message.isPersistent())
            {
                StoredMessage storedMessage = message.getStoredMessage();
                if (storedMessage != null)
                {
                    storedMessage.flowToDisk();
                }
            }
        }
    }

    /**
     * Represents stored message data.
     *
     * Thread-safety: supports concurrent access after persistence.
     */
    private final class RocksDBStoredMessage<T extends StorableMessageMetaData>
            implements StoredMessage<T>, MessageHandle<T>
    {
        private final long _messageId;
        private final int _contentSize;
        private final int _metadataSize;

        private T _metaData;
        private byte[] _content;
        private ByteArrayOutputStream _contentStream;
        private boolean _stored;
        private boolean _hardRef;

        /**
         * Creates a new stored message.
         *
         * @param messageId message id.
         * @param metaData message metadata.
         */
        private RocksDBStoredMessage(final long messageId, final T metaData)
        {
            _messageId = messageId;
            _metaData = metaData;
            _metadataSize = metaData.getStorableSize();
            _contentSize = metaData.getContentSize();
            _contentStream = new ByteArrayOutputStream(Math.max(_contentSize, 0));
            _stored = false;
            _hardRef = true;
            _inMemorySize.addAndGet(_metadataSize);
        }

        /**
         * Creates a recovered stored message.
         *
         * @param messageId message id.
         * @param metaData message metadata.
         * @param stored true when already stored.
         */
        private RocksDBStoredMessage(final long messageId, final T metaData, final boolean stored)
        {
            _messageId = messageId;
            _metaData = metaData;
            _metadataSize = metaData.getStorableSize();
            _contentSize = metaData.getContentSize();
            _stored = stored;
            _hardRef = !stored;
            _inMemorySize.addAndGet(_metadataSize);
        }

        /**
         * Adds content data.
         *
         * @param src content buffer.
         */
        @Override
        public void addContent(final QpidByteBuffer src)
        {
            if (_contentStream == null)
            {
                _contentStream = new ByteArrayOutputStream();
            }
            byte[] chunk = new byte[src.remaining()];
            src.get(chunk);
            _contentStream.write(chunk, 0, chunk.length);
        }

        /**
         * Marks all content as added and persists the message.
         *
         * @return the stored message.
         */
        @Override
        public synchronized StoredMessage<T> allContentAdded()
        {
            _inMemorySize.addAndGet(_contentSize);
            storeIfNecessary();
            return this;
        }

        /**
         * Returns the message metadata.
         *
         * @return the message metadata.
         */
        @Override
        public synchronized T getMetaData()
        {
            if (_metaData == null)
            {
                @SuppressWarnings("unchecked")
                T metaData = (T) loadMetaData(_messageId);
                _metaData = metaData;
                _inMemorySize.addAndGet(_metadataSize);
            }
            return _metaData;
        }

        /**
         * Returns the message id.
         *
         * @return the message id.
         */
        @Override
        public long getMessageNumber()
        {
            return _messageId;
        }

        /**
         * Returns message content.
         *
         * @param offset offset in content.
         * @param length content length or Integer.MAX_VALUE.
         *
         * @return the content buffer.
         */
        @Override
        public synchronized QpidByteBuffer getContent(final int offset, final int length)
        {
            byte[] content = loadContentIfNecessary();
            if (offset >= content.length)
            {
                return QpidByteBuffer.emptyQpidByteBuffer();
            }
            int available = content.length - offset;
            int size = length == Integer.MAX_VALUE ? available : Math.min(length, available);
            return QpidByteBuffer.wrap(content, offset, size);
        }

        /**
         * Returns the content size.
         *
         * @return the content size.
         */
        @Override
        public int getContentSize()
        {
            return _contentSize;
        }

        /**
         * Returns the metadata size.
         *
         * @return the metadata size.
         */
        @Override
        public int getMetadataSize()
        {
            return _metadataSize;
        }

        /**
         * Removes the message from the store.
         */
        @Override
        public void remove()
        {
            deleteMessage(_messageId);
            notifyMessageDeleted(this);
            long bytesCleared = clearInMemory(true);
            _inMemorySize.addAndGet(-bytesCleared);
        }

        /**
         * Returns whether content is in memory.
         *
         * @return true when content is in memory.
         */
        @Override
        public boolean isInContentInMemory()
        {
            return _hardRef || _content != null || _contentStream != null;
        }

        /**
         * Returns in-memory size estimate.
         *
         * @return the in-memory size.
         */
        @Override
        public long getInMemorySize()
        {
            long size = 0L;
            if (_hardRef)
            {
                size += _metadataSize + _contentSize;
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
            }
            return size;
        }

        /**
         * Forces message persistence.
         *
         * @return true if message was persisted.
         */
        @Override
        public synchronized boolean flowToDisk()
        {
            storeIfNecessary();
            if (!_hardRef)
            {
                long bytesCleared = clearInMemory(false);
                _inMemorySize.addAndGet(-bytesCleared);
                _bytesEvacuatedFromMemory.addAndGet(bytesCleared);
            }
            return true;
        }

        /**
         * Reallocates message metadata buffers.
         */
        @Override
        public synchronized void reallocate()
        {
            if (_metaData != null)
            {
                _metaData.reallocate();
            }
        }

        /**
         * Stores the message if not yet stored.
         */
        private void storeIfNecessary()
        {
            if (_stored)
            {
                return;
            }
            byte[] content = _contentStream == null
                    ? (_content == null ? EMPTY_VALUE : _content)
                    : _contentStream.toByteArray();
            storeMessage(_messageId, _metaData, content);
            if (_contentStream != null)
            {
                _content = content;
                _contentStream = null;
            }
            _stored = true;
            _hardRef = false;
        }

        /**
         * Loads content if it is not cached.
         *
         * @return the content bytes.
         */
        private byte[] loadContentIfNecessary()
        {
            if (_content == null)
            {
                if (_contentStream != null)
                {
                    _content = _contentStream.toByteArray();
                    return _content;
                }
                _content = loadContent(_messageId);
                _inMemorySize.addAndGet(_contentSize);
            }
            return _content;
        }

        /**
         * Clears message data from memory.
         *
         * @param close true to dispose metadata, false to clear encoded form.
         *
         * @return the number of bytes cleared.
         */
        private long clearInMemory(final boolean close)
        {
            long bytesCleared = 0L;
            if (_contentStream != null || _content != null)
            {
                bytesCleared += _contentSize;
                _contentStream = null;
                _content = null;
            }
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

    /**
     * Represents a queue entry record.
     *
     * Thread-safety: immutable.
     */
    private static final class RocksDBEnqueueRecord implements MessageEnqueueRecord
    {
        private final UUID _queueId;
        private final long _messageNumber;

        /**
         * Creates an enqueue record.
         *
         * @param queueId queue id.
         * @param messageNumber message number.
         */
        private RocksDBEnqueueRecord(final UUID queueId, final long messageNumber)
        {
            _queueId = queueId;
            _messageNumber = messageNumber;
        }

        /**
         * Returns the queue id.
         *
         * @return the queue id.
         */
        @Override
        public UUID getQueueId()
        {
            return _queueId;
        }

        /**
         * Returns the message number.
         *
         * @return the message number.
         */
        @Override
        public long getMessageNumber()
        {
            return _messageNumber;
        }
    }

    /**
     * Represents a stored XID record.
     *
     * Thread-safety: immutable.
     */
    private static final class RocksDBStoredXidRecord implements Transaction.StoredXidRecord
    {
        private final long _format;
        private final byte[] _globalId;
        private final byte[] _branchId;

        /**
         * Creates a stored XID record.
         *
         * @param format format.
         * @param globalId global id.
         * @param branchId branch id.
         */
        private RocksDBStoredXidRecord(final long format, final byte[] globalId, final byte[] branchId)
        {
            _format = format;
            _globalId = globalId;
            _branchId = branchId;
        }

        /**
         * Returns the format.
         *
         * @return the format.
         */
        @Override
        public long getFormat()
        {
            return _format;
        }

        /**
         * Returns the global id.
         *
         * @return the global id.
         */
        @Override
        public byte[] getGlobalId()
        {
            return _globalId;
        }

        /**
         * Returns the branch id.
         *
         * @return the branch id.
         */
        @Override
        public byte[] getBranchId()
        {
            return _branchId;
        }

        /**
         * Compares XID records by value.
         *
         * @param object object to compare.
         *
         * @return true when the records match.
         */
        @Override
        public boolean equals(final Object object)
        {
            if (this == object)
            {
                return true;
            }
            if (object == null || getClass() != object.getClass())
            {
                return false;
            }

            RocksDBStoredXidRecord that = (RocksDBStoredXidRecord) object;
            return _format == that._format
                   && Arrays.equals(_globalId, that._globalId)
                   && Arrays.equals(_branchId, that._branchId);
        }

        /**
         * Returns the hash code for the XID record.
         *
         * @return hash code.
         */
        @Override
        public int hashCode()
        {
            int result = (int) (_format ^ (_format >>> 32));
            result = 31 * result + Arrays.hashCode(_globalId);
            result = 31 * result + Arrays.hashCode(_branchId);
            return result;
        }
    }

    /**
     * Represents an XID record entry.
     *
     * Thread-safety: immutable.
     */
    private static final class XidRecord
    {
        private final byte[] _key;
        private final byte[] _value;

        /**
         * Creates an XID record.
         *
         * @param key XID key.
         * @param value XID value.
         */
        private XidRecord(final byte[] key, final byte[] value)
        {
            _key = key;
            _value = value;
        }

        /**
         * Returns the XID key.
         *
         * @return the key.
         */
        private byte[] getKey()
        {
            return _key;
        }

        /**
         * Returns the XID value.
         *
         * @return the value.
         */
        private byte[] getValue()
        {
            return _value;
        }
    }

    /**
     * Holds decoded XID actions.
     *
     * Thread-safety: immutable.
     */
    private static final class XidActions
    {
        private final List<RecordImpl> _enqueues;
        private final List<RecordImpl> _dequeues;

        /**
         * Creates XID actions.
         *
         * @param enqueues enqueue records.
         * @param dequeues dequeue records.
         */
        private XidActions(final List<RecordImpl> enqueues, final List<RecordImpl> dequeues)
        {
            _enqueues = enqueues;
            _dequeues = dequeues;
        }

        /**
         * Returns enqueue records.
         *
         * @return enqueue records.
         */
        private List<RecordImpl> getEnqueues()
        {
            return _enqueues;
        }

        /**
         * Returns dequeue records.
         *
         * @return dequeue records.
         */
        private List<RecordImpl> getDequeues()
        {
            return _dequeues;
        }
    }

    /**
     * Represents a transaction record used for distributed transactions.
     *
     * Thread-safety: immutable.
     */
    private static final class RecordImpl implements Transaction.EnqueueRecord,
                                                     Transaction.DequeueRecord,
                                                     TransactionLogResource,
                                                     EnqueueableMessage
    {
        private final RocksDBEnqueueRecord _record;
        private final long _messageNumber;
        private final UUID _queueId;

        /**
         * Creates a record instance.
         *
         * @param queueId queue id.
         * @param messageNumber message number.
         */
        private RecordImpl(final UUID queueId, final long messageNumber)
        {
            _queueId = queueId;
            _messageNumber = messageNumber;
            _record = new RocksDBEnqueueRecord(queueId, messageNumber);
        }

        /**
         * Returns the enqueue record.
         *
         * @return the enqueue record.
         */
        @Override
        public MessageEnqueueRecord getEnqueueRecord()
        {
            return _record;
        }

        /**
         * Returns the queue resource.
         *
         * @return the queue resource.
         */
        @Override
        public TransactionLogResource getResource()
        {
            return this;
        }

        /**
         * Returns the message for the enqueue record.
         *
         * @return the message.
         */
        @Override
        public EnqueueableMessage getMessage()
        {
            return this;
        }

        /**
         * Returns the message number.
         *
         * @return the message number.
         */
        @Override
        public long getMessageNumber()
        {
            return _messageNumber;
        }

        /**
         * Returns whether the message is persistent.
         *
         * @return true for persistence.
         */
        @Override
        public boolean isPersistent()
        {
            return true;
        }

        /**
         * Returns the stored message.
         *
         * @return the stored message.
         */
        @Override
        public StoredMessage getStoredMessage()
        {
            throw new UnsupportedOperationException();
        }

        /**
         * Returns the queue name.
         *
         * @return the queue name.
         */
        @Override
        public String getName()
        {
            return _queueId.toString();
        }

        /**
         * Returns the queue id.
         *
         * @return the queue id.
         */
        @Override
        public UUID getId()
        {
            return _queueId;
        }

        /**
         * Returns the message durability.
         *
         * @return the message durability.
         */
        @Override
        public MessageDurability getMessageDurability()
        {
            return MessageDurability.DEFAULT;
        }
    }

    /**
     * Provides a RocksDB-backed transaction.
     *
     * Thread-safety: not thread-safe.
     */
    private final class RocksDBTransaction implements Transaction
    {
        private final List<RocksDBEnqueueRecord> _enqueues = new ArrayList<>();
        private final List<RocksDBEnqueueRecord> _dequeues = new ArrayList<>();
        private final List<XidRecord> _xidRecords = new ArrayList<>();
        private final List<byte[]> _xidRemovals = new ArrayList<>();

        /**
         * Enqueues a message within the transaction.
         *
         * @param queue queue resource.
         * @param message message to enqueue.
         *
         * @return the enqueue record.
         */
        @Override
        public MessageEnqueueRecord enqueueMessage(final TransactionLogResource queue,
                                                   final EnqueueableMessage message)
        {
            if (message != null && message.isPersistent())
            {
                StoredMessage storedMessage = message.getStoredMessage();
                if (storedMessage != null)
                {
                    storedMessage.flowToDisk();
                }
            }
            RocksDBEnqueueRecord record = new RocksDBEnqueueRecord(queue.getId(), message.getMessageNumber());
            _enqueues.add(record);
            return record;
        }

        /**
         * Dequeues a message within the transaction.
         *
         * @param enqueueRecord enqueue record to dequeue.
         */
        @Override
        public void dequeueMessage(final MessageEnqueueRecord enqueueRecord)
        {
            _dequeues.add(new RocksDBEnqueueRecord(enqueueRecord.getQueueId(), enqueueRecord.getMessageNumber()));
        }

        /**
         * Commits the transaction.
         */
        @Override
        public void commitTran()
        {
            synchronized (_transactionLock)
            {
                storeQueueEntries(_enqueues, _dequeues);
                storeXidRecords(_xidRecords, _xidRemovals);
            }
            clear();
        }

        /**
         * Commits the transaction asynchronously.
         *
         * @param val value to return.
         * @param <X> return type.
         *
         * @return completed future.
         */
        @Override
        public <X> CompletableFuture<X> commitTranAsync(final X val)
        {
            commitTran();
            return CompletableFuture.completedFuture(val);
        }

        /**
         * Aborts the transaction.
         */
        @Override
        public void abortTran()
        {
            clear();
        }

        /**
         * Removes an XID record.
         *
         * @param record stored XID record.
         */
        @Override
        public void removeXid(final StoredXidRecord record)
        {
            _xidRemovals.add(encodeXidKey(record.getFormat(), record.getGlobalId(), record.getBranchId()));
        }

        /**
         * Records an XID with actions.
         *
         * @param format format.
         * @param globalId global id.
         * @param branchId branch id.
         * @param enqueues enqueue records.
         * @param dequeues dequeue records.
         *
         * @return the stored XID record.
         */
        @Override
        public StoredXidRecord recordXid(final long format,
                                         final byte[] globalId,
                                         final byte[] branchId,
                                         final EnqueueRecord[] enqueues,
                                         final DequeueRecord[] dequeues)
        {
            ensureMessagesStored(enqueues);
            byte[] key = encodeXidKey(format, globalId, branchId);
            byte[] value = encodeXidValue(enqueues, dequeues);
            _xidRecords.add(new XidRecord(key, value));
            return new RocksDBStoredXidRecord(format, globalId, branchId);
        }

        /**
         * Clears pending transaction data.
         */
        private void clear()
        {
            _enqueues.clear();
            _dequeues.clear();
            _xidRecords.clear();
            _xidRemovals.clear();
        }
    }

    /**
     * Reads data from the RocksDB message store.
     *
     * Thread-safety: not thread-safe.
     */
    private final class RocksDBMessageStoreReader implements MessageStoreReader
    {
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
            RocksDB database = _environment.getDatabase();
            ColumnFamilyHandle metadataHandle =
                    _environment.getColumnFamilyHandle(RocksDBColumnFamily.MESSAGE_METADATA);
            try (RocksIterator iterator = database.newIterator(metadataHandle))
            {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                {
                    long messageId = decodeMessageKey(iterator.key());
                    StorableMessageMetaData metaData = decodeMetaData(iterator.value());
                    StoredMessage<?> message = new RocksDBStoredMessage<>(messageId, metaData, true);
                    if (!handler.handle(message))
                    {
                        return;
                    }
                }
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
            RocksDB database = _environment.getDatabase();
            ColumnFamilyHandle queueHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.QUEUE_ENTRIES);
            try (RocksIterator iterator = database.newIterator(queueHandle))
            {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                {
                    byte[] key = iterator.key();
                    UUID queueId = decodeQueueId(key);
                    long messageId = decodeQueueMessageId(key);
                    if (!handler.handle(new RocksDBEnqueueRecord(queueId, messageId)))
                    {
                        return;
                    }
                }
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
            RocksDB database = _environment.getDatabase();
            ColumnFamilyHandle queueHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.QUEUE_ENTRIES);
            byte[] prefix = encodeQueuePrefix(queue.getId());
            try (RocksIterator iterator = database.newIterator(queueHandle))
            {
                for (iterator.seek(prefix); iterator.isValid(); iterator.next())
                {
                    byte[] key = iterator.key();
                    if (!hasPrefix(key, prefix))
                    {
                        break;
                    }
                    long messageId = decodeQueueMessageId(key);
                    if (!handler.handle(new RocksDBEnqueueRecord(queue.getId(), messageId)))
                    {
                        return;
                    }
                }
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
            RocksDB database = _environment.getDatabase();
            ColumnFamilyHandle xidHandle = _environment.getColumnFamilyHandle(RocksDBColumnFamily.XIDS);
            try (RocksIterator iterator = database.newIterator(xidHandle))
            {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                {
                    StoredXidRecord xid = decodeXidKey(iterator.key());
                    XidActions actions = decodeXidValue(iterator.value());
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
            StorableMessageMetaData metaData = loadMetaData(messageId);
            if (metaData == null)
            {
                return null;
            }
            return new RocksDBStoredMessage<>(messageId, metaData, true);
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
