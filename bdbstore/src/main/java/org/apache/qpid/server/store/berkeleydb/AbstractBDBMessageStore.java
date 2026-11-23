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

import static org.apache.qpid.server.store.berkeleydb.BDBUtils.DEFAULT_DATABASE_CONFIG;
import static org.apache.qpid.server.store.berkeleydb.BDBUtils.abortTransactionSafely;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.DEFAULT_BDB_RECOVERY_SNAPSHOT_MAX_BYTES;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import org.slf4j.Logger;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.EventManager;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.SizeMonitoringSettings;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.berkeleydb.entry.PreparedTransaction;
import org.apache.qpid.server.store.berkeleydb.tuple.MessageMetaDataBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.PreparedTransactionBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.QueueEntryBinding;
import org.apache.qpid.server.store.berkeleydb.tuple.XidBinding;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.txn.Xid;
import org.apache.qpid.server.util.CachingUUIDFactory;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;


public abstract class AbstractBDBMessageStore implements MessageStore
{

    private static final int LOCK_RETRY_ATTEMPTS = 5;

    private static final String MESSAGE_META_DATA_DB_NAME = "MESSAGE_METADATA";
    private static final String MESSAGE_META_DATA_SEQ_DB_NAME = "MESSAGE_METADATA.SEQ";
    private static final String MESSAGE_CONTENT_DB_NAME = "MESSAGE_CONTENT";
    private static final String DELIVERY_DB_NAME = "QUEUE_ENTRIES";

    //TODO: Add upgrader to remove BRIDGES and LINKS
    private static final String BRIDGEDB_NAME = "BRIDGES";
    private static final String LINKDB_NAME = "LINKS";
    private static final String XID_DB_NAME = "XIDS";

    // Use these entries only as inputs to synchronous JE operations. Keep cursor outputs local to each scan.
    private static final ThreadLocal<DatabaseEntry> MESSAGE_ID_ENTRY =
            ThreadLocal.withInitial(() -> new DatabaseEntry(new byte[Long.BYTES]));
    private static final ThreadLocal<DatabaseEntry> QUEUE_ENTRY =
            ThreadLocal.withInitial(() -> new DatabaseEntry(new byte[QueueEntryBinding.KEY_SIZE]));
    private static final ThreadLocal<DatabaseEntry> ENQUEUE_RECORD_ENTRY =
            ThreadLocal.withInitial(() -> new DatabaseEntry(new byte[0]));

    private final AtomicBoolean _messageStoreOpen = new AtomicBoolean();
    private final AtomicLong _messageStoreCloseCount = new AtomicLong();

    private final EventManager _eventManager = new EventManager();

    private final DatabaseEntry MESSAGE_METADATA_SEQ_KEY = new DatabaseEntry("MESSAGE_METADATA_SEQ_KEY".getBytes(
            StandardCharsets.UTF_8));

    private final SequenceConfig MESSAGE_METADATA_SEQ_CONFIG = SequenceConfig.DEFAULT.
            setAllowCreate(true).
            setInitialValue(1).
            setWrap(true).
            setCacheSize(100000);
    private ConfiguredObject<?> _parent;
    private long _persistentSizeLowThreshold;
    private long _persistentSizeHighThreshold;

    private boolean _limitBusted;
    private long _totalStoreSize;
    private final Random _lockConflictRandom = new Random();
    private final AtomicLong _inMemorySize = new AtomicLong();
    private final AtomicLong _bytesEvacuatedFromMemory = new AtomicLong();
    private final Set<StoredBDBMessage<?>> _messages = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<MessageDeleteListener> _messageDeleteListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public void openMessageStore(final ConfiguredObject<?> parent)
    {
        if (_messageStoreOpen.compareAndSet(false, true))
        {
            _parent = parent;

            final SizeMonitoringSettings sizeMonitorSettings = (SizeMonitoringSettings) parent;
            _persistentSizeHighThreshold = sizeMonitorSettings.getStoreOverfullSize();
            _persistentSizeLowThreshold = sizeMonitorSettings.getStoreUnderfullSize();

            if (_persistentSizeLowThreshold > _persistentSizeHighThreshold || _persistentSizeLowThreshold < 0L)
            {
                _persistentSizeLowThreshold = _persistentSizeHighThreshold;
            }

            doOpen(parent);
        }
    }

    protected abstract void doOpen(final ConfiguredObject<?> parent);

    @Override
    public void closeMessageStore()
    {
        if (_messageStoreOpen.compareAndSet(true, false))
        {
            _messageStoreCloseCount.incrementAndGet();
            for (StoredBDBMessage<?> message : _messages)
            {
                message.clear(true);
            }
            _messages.clear();
            _inMemorySize.set(0);
            _bytesEvacuatedFromMemory.set(0);
            doClose();
        }
    }

    protected abstract void doClose();

    @Override
    public void upgradeStoreStructure() throws StoreException
    {
        try
        {
            getEnvironmentFacade().upgradeIfNecessary(getParent());

            // TODO this relies on the fact that the VH will call upgrade just before putting the VH into service.
            _totalStoreSize = getSizeOnDisk();
        }
        catch(RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Cannot upgrade store", e);
        }
    }

    void deleteMessageStoreDatabases()
    {
        try
        {
            for (String db : Arrays.asList(MESSAGE_META_DATA_DB_NAME,
                                          MESSAGE_META_DATA_SEQ_DB_NAME,
                                          MESSAGE_CONTENT_DB_NAME,
                                          DELIVERY_DB_NAME,
                                          XID_DB_NAME))
            {
                try
                {

                    getEnvironmentFacade().deleteDatabase(db);
                }
                catch (DatabaseNotFoundException ignore)
                {
                }

            }
        }
        catch (IllegalStateException e)
        {
            getLogger().warn("Could not delete message store databases: {}", e.getMessage());
        }
        catch (RuntimeException e)
        {
            getEnvironmentFacade().handleDatabaseException("Deletion of message store databases failed", e);
        }
    }

    @Override
    public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(T metaData)
    {

        long newMessageId = getNextMessageId();

        return createStoredBDBMessage(newMessageId, metaData, false);
    }

    private <T extends StorableMessageMetaData> StoredBDBMessage<T> createStoredBDBMessage(final long newMessageId,
                                                                                           final T metaData,
                                                                                           final boolean recovered)
    {
        final StoredBDBMessage<T> message = new StoredBDBMessage<>(newMessageId, metaData, recovered);
        _messages.add(message);
        return message;
    }

    @Override
    public long getNextMessageId()
    {
        long newMessageId;
        try
        {
            // The implementations of sequences mean that there is only a transaction
            // after every n sequence values, where n is the MESSAGE_METADATA_SEQ_CONFIG.getCacheSize()

            Sequence mmdSeq = getEnvironmentFacade().openSequence(getMessageMetaDataSeqDb(),
                                                              MESSAGE_METADATA_SEQ_KEY,
                                                              MESSAGE_METADATA_SEQ_CONFIG);
            newMessageId = mmdSeq.get(null, 1);
        }
        catch(LockTimeoutException le)
        {
           throw new ConnectionScopedRuntimeException("Unexpected exception on BDB sequence", le);
        }
        catch (RuntimeException de)
        {
            throw getEnvironmentFacade().handleDatabaseException("Cannot get sequence value for new message", de);
        }
        return newMessageId;
    }

    @Override
    public long getInMemorySize()
    {
        return _inMemorySize.get();
    }

    @Override
    public long getBytesEvacuatedFromMemory()
    {
        return _bytesEvacuatedFromMemory.get();
    }

    @Override
    public boolean isPersistent()
    {
        return true;
    }

    @Override
    public void resetStatistics()
    {
        _bytesEvacuatedFromMemory.set(0L);
    }

    @Override
    public org.apache.qpid.server.store.Transaction newTransaction()
    {
        checkMessageStoreOpen();

        return new BDBTransaction();
    }

    @Override
    public void addEventListener(final EventListener eventListener, final Event... events)
    {
        _eventManager.addEventListener(eventListener, events);
    }

    @Override
    public MessageStoreReader newMessageStoreReader()
    {
        return new BDBMessageStoreReader();
    }

    private static DatabaseEntry messageIdKey(final long messageId)
    {
        final DatabaseEntry key = MESSAGE_ID_ENTRY.get();
        QueueEntryBinding.writeMessageId(messageId, key);
        return key;
    }

    private static DatabaseEntry queueEntryKey(final UUID queueId, final long messageId)
    {
        final DatabaseEntry key = QUEUE_ENTRY.get();
        QueueEntryBinding.objectToEntry(queueId, messageId, key);
        return key;
    }

    /**
     * Retrieves message meta-data.
     *
     * @param messageId The message to get the meta-data for.
     *
     * @return The message meta data.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    StorableMessageMetaData getMessageMetaData(final long messageId) throws StoreException
    {
        getLogger().debug("public MessageMetaData getMessageMetaData(Long messageId = {}): called", messageId);

        final DatabaseEntry value = new DatabaseEntry();

        try
        {
            final OperationStatus status = getMessageMetaDataDb().get(null, messageIdKey(messageId), value,
                    LockMode.READ_UNCOMMITTED);
            if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Metadata not found for message with id " + messageId);
            }

            return MessageMetaDataBinding.getInstance().entryToObject(value);
        }
        catch (RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error reading message metadata for message with id "
                                                                 + messageId
                                                                 + ": "
                                                                 + e.getMessage(), e);
        }
    }

    void removeMessage(final long messageId) throws StoreException
    {
        boolean complete = false;
        Transaction tx = null;
        int attempts = 0;
        try
        {
            do
            {
                tx = null;
                try
                {
                    tx = getEnvironmentFacade().beginTransaction(null);

                    getLogger().debug("Removing message id {}", messageId);

                    final OperationStatus status = getMessageMetaDataDb().delete(tx, messageIdKey(messageId));
                    if (status == OperationStatus.NOTFOUND)
                    {
                        getLogger().debug("Message id {} not found (attempt to remove failed - probably application initiated rollback)",messageId);
                    }

                    getLogger().debug("Deleted metadata for message {}", messageId);

                    getMessageContentDb().delete(tx, messageIdKey(messageId));

                    getLogger().debug("Deleted content for message {}", messageId);

                    getEnvironmentFacade().commitNoSync(tx);

                    complete = true;
                    tx = null;
                }
                catch (LockConflictException e)
                {
                    try
                    {
                        if(tx != null)
                        {
                            tx.abort();
                        }
                    }
                    catch(RuntimeException e2)
                    {
                        getLogger().warn("Unable to abort transaction after LockConflictException on removal of message with id {}", messageId,
                                e2);
                        // rethrow the original log conflict exception, the secondary exception should already have
                        // been logged.
                        throw getEnvironmentFacade().handleDatabaseException("Cannot remove message with id "
                                                                             + messageId, e);
                    }

                    sleepOrThrowOnLockConflict(attempts++, "Cannot remove messages", e);
                }
            }
            while(!complete);
        }
        catch (RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Unexpected BDB exception", e);
            }

            try
            {
                abortTransactionSafely(tx,
                                       getEnvironmentFacade());
            }
            finally
            {
                tx = null;
            }

            throw getEnvironmentFacade().handleDatabaseException("Error removing message with id "
                                                                 + messageId
                                                                 + " from database: "
                                                                 + e.getMessage(), e);
        }
        finally
        {
            try
            {
                abortTransactionSafely(tx,
                                       getEnvironmentFacade());
            }
            finally
            {
                tx = null;
            }
        }
    }

    QpidByteBuffer getAllContent(final long messageId) throws StoreException
    {
        final DatabaseEntry value = new DatabaseEntry();

        getLogger().debug("Message Id: {} Getting content body", messageId);

        try
        {
            final OperationStatus status = getMessageContentDb().get(null, messageIdKey(messageId), value,
                    LockMode.READ_UNCOMMITTED);

            if (status == OperationStatus.SUCCESS)
            {
                byte[] data = value.getData();
                int offset = value.getOffset();
                int length = value.getSize();
                QpidByteBuffer buf = QpidByteBuffer.allocateDirect(length);
                buf.put(data, offset, length);
                buf.flip();
                return buf;
            }
            else
            {
                throw new StoreException("Unable to find message with id " + messageId);
            }

        }
        catch (RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error getting AMQMessage with id "
                                                                 + messageId
                                                                 + " to database: "
                                                                 + e.getMessage(), e);
        }
    }

    private void visitMessagesInternal(MessageHandler handler, EnvironmentFacade environmentFacade)
    {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();
        MessageMetaDataBinding valueBinding = MessageMetaDataBinding.getInstance();

        try(Cursor cursor = getMessageMetaDataDb().openCursor(null, null))
        {
            while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
            {
                long messageId = LongBinding.entryToLong(key);
                StorableMessageMetaData metaData = valueBinding.entryToObject(value);
                StoredBDBMessage message = createStoredBDBMessage(messageId, metaData, true);
                if (!handler.handle(message))
                {
                    break;
                }
            }
        }
        catch (RuntimeException e)
        {
            throw environmentFacade.handleDatabaseException("Cannot visit messages", e);
        }
    }

    private void sleepOrThrowOnLockConflict(int attempts, String throwMessage, LockConflictException cause)
    {
        if (attempts < LOCK_RETRY_ATTEMPTS)
        {
            getLogger().info("Lock conflict exception. Retrying (attempt {} of {})", attempts, LOCK_RETRY_ATTEMPTS);
            try
            {
                Thread.sleep(500L + (long)(500L * _lockConflictRandom.nextDouble()));
            }
            catch (InterruptedException ie)
            {
                Thread.currentThread().interrupt();
                throw getEnvironmentFacade().handleDatabaseException(throwMessage, cause);
            }
        }
        else
        {
            // rethrow the lock conflict exception since we could not solve by retrying
            throw getEnvironmentFacade().handleDatabaseException(throwMessage, cause);
        }
    }

    private StoredBDBMessage<?> getMessageInternal(final long messageId, final EnvironmentFacade environmentFacade)
    {
        try
        {
            final DatabaseEntry value = new DatabaseEntry();
            if (getMessageMetaDataDb().get(null, messageIdKey(messageId), value, LockMode.READ_COMMITTED)
                    == OperationStatus.SUCCESS)
            {
                final StorableMessageMetaData metaData = MessageMetaDataBinding.getInstance().entryToObject(value);
                return createStoredBDBMessage(messageId, metaData, true);
            }
            else
            {
                return null;
            }

        }
        catch (RuntimeException e)
        {
            throw environmentFacade.handleDatabaseException("Cannot visit messages", e);
        }
    }

    /**
     * Stores a chunk of message data.
     *
     * @param tx         The transaction for the operation.
     * @param messageId       The message to store the data for.
     * @param contentBody     The content of the data chunk.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    private void addContent(final Transaction tx, final long messageId, final QpidByteBuffer contentBody)
            throws StoreException
    {
        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            buffer.prepare(messageId, contentBody.remaining());
            buffer.copyContent(contentBody);
            try
            {
                final OperationStatus status = getMessageContentDb().put(tx, buffer.getKey(), buffer.getValue());
                if (status != OperationStatus.SUCCESS)
                {
                    throw new StoreException("Error adding content for message id " + messageId + ": " + status);
                }

                getLogger().debug("Storing content for message {} in transaction {}", messageId, tx);
            }
            catch (final RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException(
                        "Error writing AMQMessage with id " + messageId + " to database: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Stores message meta-data.
     *
     * @param tx         The transaction for the operation.
     * @param messageId       The message to store the data for.
     * @param messageMetaData The message meta data to store.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    private void storeMetaData(final Transaction tx, final long messageId,
                               final StorableMessageMetaData messageMetaData) throws StoreException
    {
        getLogger().debug("storeMetaData called for transaction {}, messageId {}, messageMetaData {} ",
                tx, messageId, messageMetaData);

        try (final BDBWriteBuffer buffer = BDBWriteBuffer.acquire())
        {
            final MessageMetaDataBinding binding = MessageMetaDataBinding.getInstance();
            buffer.prepare(messageId, binding.getEntrySize(messageMetaData));
            binding.writeToEntry(messageMetaData, buffer.getValue());

            boolean complete = false;
            int attempts = 0;
            do
            {
                try
                {
                    getMessageMetaDataDb().put(tx, buffer.getKey(), buffer.getValue());
                    getLogger().debug("Storing message metadata for message id {} in transaction {}", messageId, tx);
                    complete = true;
                }
                catch (final LockConflictException e)
                {
                    sleepOrThrowOnLockConflict(attempts++, "Cannot store metadata", e);
                }
                catch (final RuntimeException e)
                {
                    throw getEnvironmentFacade().handleDatabaseException(
                            "Error writing message metadata with id " + messageId + " to database: " + e.getMessage(),
                            e);
                }
            }
            while (!complete);
        }
    }


    /**
     * Places a message onto a specified queue, in a given transaction.
     *
     * @param tx   The transaction for the operation.
     * @param queue     The the queue to place the message on.
     * @param messageId The message to enqueue.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason.
     */
    private void enqueueMessage(final Transaction tx, final TransactionLogResource queue, final long messageId)
            throws StoreException
    {
        try
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Enqueuing message {} on queue {} with id {} in transaction {}",
                                  messageId, queue.getName(), queue.getId(), tx);
            }
            final DatabaseEntry value = ENQUEUE_RECORD_ENTRY.get();
            getDeliveryDb().put(tx, queueEntryKey(queue.getId(), messageId), value);
        }
        catch (RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Failed to enqueue: {}", e.getMessage(), e);
            }
            throw getEnvironmentFacade().handleDatabaseException("Error writing enqueued message with id "
                                                                 + messageId
                                                                 + " for queue "
                                                                 + queue.getName()
                                                                 + " with id "
                                                                 + queue.getId()
                                                                 + " to database", e);
        }
    }

    /**
     * Extracts a message from a specified queue, in a given transaction.
     *
     * @param tx   The transaction for the operation.
     * @param queueId     The id of the queue to take the message from.
     * @param messageId The message to dequeue.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    private void dequeueMessage(final Transaction tx, final UUID queueId, final long messageId) throws StoreException
    {
        getLogger().debug("Dequeue message id {} from queue with id {}", messageId, queueId);

        try
        {
            final OperationStatus status = getDeliveryDb().delete(tx, queueEntryKey(queueId, messageId));
            if (status == OperationStatus.NOTFOUND)
            {
                throw new StoreException("Unable to find message with id " + messageId +
                        " on queue with id " + queueId);
            }
            else if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Unable to remove message with id " + messageId +
                        " on queue with id " + queueId);
            }

            getLogger().debug("Removed message {} on queue with id {}", messageId, queueId);

        }
        catch (RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Failed to dequeue message {} in transaction {}", messageId, tx, e);
            }

            throw getEnvironmentFacade().handleDatabaseException("Error accessing database while dequeuing message: "
                                                                 + e.getMessage(), e);
        }
    }

    private List<Runnable> recordXid(Transaction txn,
                                     long format,
                                     byte[] globalId,
                                     byte[] branchId,
                                     org.apache.qpid.server.store.Transaction.EnqueueRecord[] enqueues,
                                     org.apache.qpid.server.store.Transaction.DequeueRecord[] dequeues) throws StoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        Xid xid = new Xid(format, globalId, branchId);
        XidBinding keyBinding = XidBinding.getInstance();
        keyBinding.objectToEntry(xid,key);

        DatabaseEntry value = new DatabaseEntry();
        PreparedTransaction preparedTransaction = new PreparedTransaction(enqueues, dequeues);
        PreparedTransactionBinding.objectToEntry(preparedTransaction, value);
        for(org.apache.qpid.server.store.Transaction.EnqueueRecord enqueue : enqueues)
        {
            StoredMessage storedMessage = enqueue.getMessage().getStoredMessage();
            if(storedMessage instanceof StoredBDBMessage)
            {
                ((StoredBDBMessage) storedMessage).store(txn);
            }
        }

        try
        {
            getXidDb().put(txn, key, value);
            return Collections.emptyList();
        }
        catch (RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Failed to write xid: {}", e.getMessage(), e);
            }
            throw getEnvironmentFacade().handleDatabaseException("Error writing xid to database", e);
        }
    }

    private void removeXid(Transaction txn, long format, byte[] globalId, byte[] branchId)
            throws StoreException
    {
        DatabaseEntry key = new DatabaseEntry();
        Xid xid = new Xid(format, globalId, branchId);
        XidBinding keyBinding = XidBinding.getInstance();

        keyBinding.objectToEntry(xid, key);


        try
        {

            OperationStatus status = getXidDb().delete(txn, key);
            if (status == OperationStatus.NOTFOUND)
            {
                throw new StoreException("Unable to find xid");
            }
            else if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Unable to remove xid");
            }

        }
        catch (RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().error("Failed to remove xid in transaction {}", e);
            }

            throw getEnvironmentFacade().handleDatabaseException("Error accessing database while removing xid: "
                                                                 + e.getMessage(), e);
        }
    }

    /**
     * Commits all operations performed within a given transaction.
     *
     * @param tx The transaction to commit all operations for.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason.
     */
    private void commitTranImpl(final Transaction tx) throws StoreException
    {
        if (tx == null)
        {
            throw new StoreException("Fatal internal error: transactional is null at commitTran");
        }

        getEnvironmentFacade().commit(tx);

        getLogger().debug("commitTranImpl completed {} transaction synchronous", tx);


    }

    private <X> CompletableFuture<X> commitTranAsyncImpl(final Transaction tx, X val) throws StoreException
    {
        if (tx == null)
        {
            throw new StoreException("Fatal internal error: transactional is null at commitTran");
        }

        CompletableFuture<X> result = getEnvironmentFacade().commitAsync(tx, val);

        getLogger().debug("commitTranAsynImpl completed transaction {}", tx);

        return result;
    }


    /**
     * Abandons all operations performed within a given transaction.
     *
     * @param tx The transaction to abandon.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason.
     */
    private void abortTran(final Transaction tx) throws StoreException
    {
        getLogger().debug("abortTran called for transaction {}", tx);

        try
        {
            tx.abort();
        }
        catch (RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error aborting transaction: " + e.getMessage(), e);
        }
    }

    private void storedSizeChangeOccurred(final int delta) throws StoreException
    {
        try
        {
            storedSizeChange(delta);
        }
        catch(RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Stored size change exception", e);
        }
    }

    private void storedSizeChange(final int delta)
    {
        if(getPersistentSizeHighThreshold() > 0)
        {
            synchronized (this)
            {
                // the delta supplied is an approximation of a store size change. we don;t want to check the statistic every
                // time, so we do so only when there's been enough change that it is worth looking again. We do this by
                // assuming the total size will change by less than twice the amount of the message data change.
                long newSize = _totalStoreSize += 2*delta;

                if(!_limitBusted &&  newSize > getPersistentSizeHighThreshold())
                {
                    _totalStoreSize = getSizeOnDisk();

                    if(_totalStoreSize > getPersistentSizeHighThreshold())
                    {
                        _limitBusted = true;
                        _eventManager.notifyEvent(Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);
                    }
                }
                else if(_limitBusted && newSize < getPersistentSizeLowThreshold())
                {
                    long oldSize = _totalStoreSize;
                    _totalStoreSize = getSizeOnDisk();

                    if(oldSize <= _totalStoreSize)
                    {

                        reduceSizeOnDisk();

                        _totalStoreSize = getSizeOnDisk();

                    }

                    if(_totalStoreSize < getPersistentSizeLowThreshold())
                    {
                        _limitBusted = false;
                        _eventManager.notifyEvent(Event.PERSISTENT_MESSAGE_SIZE_UNDERFULL);
                    }


                }
            }
        }
    }

    private void reduceSizeOnDisk()
    {
        getEnvironmentFacade().reduceSizeOnDisk();
    }

    private long getSizeOnDisk()
    {
        return getEnvironmentFacade().getTotalLogSize();
    }

    private Database getMessageContentDb()
    {
        return getEnvironmentFacade().openDatabase(MESSAGE_CONTENT_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private Database getMessageMetaDataDb()
    {
        return getEnvironmentFacade().openDatabase(MESSAGE_META_DATA_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private Database getMessageMetaDataSeqDb()
    {
        return getEnvironmentFacade().openDatabase(MESSAGE_META_DATA_SEQ_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private Database getDeliveryDb()
    {
        return getEnvironmentFacade().openDatabase(DELIVERY_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private Database getXidDb()
    {
        return getEnvironmentFacade().openDatabase(XID_DB_NAME, DEFAULT_DATABASE_CONFIG);
    }

    private void checkMessageStoreOpen()
    {
        if (!_messageStoreOpen.get())
        {
            throw new IllegalStateException("Message store is not open");
        }
    }

    protected boolean isMessageStoreOpen()
    {
        return _messageStoreOpen.get();
    }

    protected final ConfiguredObject<?> getParent()
    {
        return _parent;
    }

    protected abstract EnvironmentFacade getEnvironmentFacade();

    private long getPersistentSizeLowThreshold()
    {
        return _persistentSizeLowThreshold;
    }

    private long getPersistentSizeHighThreshold()
    {
        return _persistentSizeHighThreshold;
    }

    protected abstract Logger getLogger();

    final class StoredBDBMessage<T extends StorableMessageMetaData> implements StoredMessage<T>, MessageHandle<T>
    {

        private final long _messageId;
        private final int _contentSize;
        private final int _metadataSize;
        // This message's monitor guards the mutable payload and lifecycle state.
        private T _metaData;
        private QpidByteBuffer _data;
        private List<QpidByteBuffer> _contentFragments;
        // Body bytes already included in the store's in-memory count.
        private int _accountedContentSize;
        // Both records have been written. The transaction may still be uncommitted.
        private boolean _stored;
        private boolean _removed;

        StoredBDBMessage(final long messageId, final T metaData, final boolean isRecovered)
        {
            _messageId = messageId;

            _metaData = metaData;
            _stored = isRecovered;

            _contentSize = metaData.getContentSize();
            _metadataSize = metaData.getStorableSize();
            _inMemorySize.addAndGet(_metadataSize);
        }

        @Override
        public synchronized T getMetaData()
        {
            if (_removed)
            {
                return null;
            }

            if (_metaData == null)
            {
                checkMessageStoreOpen();
                _metaData = (T) getMessageMetaData(_messageId);
                _stored = true;
                _inMemorySize.addAndGet(getMetadataSize());
            }
            return _metaData;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageId;
        }

        @Override
        public synchronized void addContent(final QpidByteBuffer src)
        {
            if (_removed || !src.hasRemaining())
            {
                return;
            }

            final QpidByteBuffer fragment = src.slice();
            if (_contentFragments == null)
            {
                if (_data == null)
                {
                    _data = fragment;
                    return;
                }

                _contentFragments = new ArrayList<>();
                _contentFragments.add(_data);
                _data = null;
            }
            _contentFragments.add(fragment);
        }

        @Override
        public synchronized StoredMessage<T> allContentAdded()
        {
            if (!_removed)
            {
                accountContent(consolidateContentFragments());
            }
            return this;
        }

        /**
         * Returns the content buffer owned by this message.
         */
        private QpidByteBuffer getContentAsByteBuffer()
        {
            if (_removed)
            {
                return QpidByteBuffer.emptyQpidByteBuffer();
            }

            consolidateContentFragments();
            if (_data == null && _stored)
            {
                checkMessageStoreOpen();
                _data = AbstractBDBMessageStore.this.getAllContent(_messageId);
                accountContent(_data);
            }
            return _data == null ? QpidByteBuffer.emptyQpidByteBuffer() : _data;
        }

        private QpidByteBuffer consolidateContentFragments()
        {
            if (_contentFragments != null)
            {
                _data = QpidByteBuffer.concatenate(_contentFragments);
                clearContentFragments();
            }
            return _data;
        }

        private void accountContent(final QpidByteBuffer data)
        {
            final int size = data == null ? 0 : data.remaining();
            if (size != _accountedContentSize)
            {
                _inMemorySize.addAndGet(size - _accountedContentSize);
                _accountedContentSize = size;
            }
        }

        @Override
        public synchronized QpidByteBuffer getContent(final int offset, final int length)
        {
            final QpidByteBuffer content = getContentAsByteBuffer();
            if (length == Integer.MAX_VALUE)
            {
                final int remaining = content.remaining();
                if (offset < 0 || offset > remaining)
                {
                    throw new IllegalArgumentException("Content offset out of range: " + offset);
                }
                return content.view(offset, remaining - offset);
            }
            return content.view(offset, length);
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

        synchronized void store(final Transaction txn)
        {
            if (_removed)
            {
                throw new StoreException("Cannot store removed message " + _messageId);
            }

            if (!_stored)
            {
                AbstractBDBMessageStore.this.storeMetaData(txn, _messageId, _metaData);
                final QpidByteBuffer data = consolidateContentFragments();
                AbstractBDBMessageStore.this.addContent(txn, _messageId,
                                                        data == null ? QpidByteBuffer.emptyQpidByteBuffer() : data);
                accountContent(data);
                _stored = true;
            }
        }

        synchronized void flushToStore()
        {
            if (!_removed && !_stored)
            {
                checkMessageStoreOpen();

                final Transaction txn;
                try
                {
                    txn = getEnvironmentFacade().beginTransaction(null);
                }
                catch (final RuntimeException e)
                {
                    throw getEnvironmentFacade().handleDatabaseException("failed to begin transaction", e);
                }
                store(txn);
                getEnvironmentFacade().commitAsync(txn, false);
            }
        }

        @Override
        public synchronized void remove()
        {
            checkMessageStoreOpen();
            if (_removed)
            {
                return;
            }
            if (_stored)
            {
                removeMessage(_messageId);
                storedSizeChangeOccurred(-getContentSize());
            }
            _messages.remove(this);
            try
            {
                if (!_messageDeleteListeners.isEmpty())
                {
                    for (final MessageDeleteListener messageDeleteListener : _messageDeleteListeners)
                    {
                        messageDeleteListener.messageDeleted(this);
                    }
                }
            }
            finally
            {
                _removed = true;
                clearMessageData(true);
            }
        }

        private long clearContentFragments()
        {
            final List<QpidByteBuffer> fragments = _contentFragments;
            _contentFragments = null;
            long bytesCleared = 0;
            if (fragments != null)
            {
                for (final QpidByteBuffer fragment : fragments)
                {
                    bytesCleared += fragment.remaining();
                    fragment.dispose();
                }
            }
            return bytesCleared;
        }

        private long clearMessageData(final boolean close)
        {
            final T metaData = _metaData;
            final long metadataSize = metaData == null ? 0L : getMetadataSize();
            final int accountedContentSize = _accountedContentSize;
            _metaData = null;
            _accountedContentSize = 0;

            try (final QpidByteBuffer data = _data)
            {
                _data = null;
                return metadataSize + clearContentFragments() + (data == null ? 0L : data.remaining());
            }
            finally
            {
                try
                {
                    if (metaData != null)
                    {
                        if (close)
                        {
                            metaData.dispose();
                        }
                        else
                        {
                            metaData.clearEncodedForm();
                        }
                    }
                }
                finally
                {
                    _inMemorySize.addAndGet(-metadataSize - accountedContentSize);
                }
            }
        }

        @Override
        public synchronized boolean isInContentInMemory()
        {
            return !_removed && (!_stored || _data != null || _contentFragments != null);
        }

        @Override
        public synchronized long getInMemorySize()
        {
            if (_removed)
            {
                return 0L;
            }
            if (!_stored)
            {
                return (long) getMetadataSize() + getContentSize();
            }
            final long metadataSize = _metaData == null ? 0L : getMetadataSize();
            return metadataSize + _accountedContentSize;
        }

        @Override
        public synchronized boolean flowToDisk()
        {
            flushToStore();
            if (!_removed && _stored)
            {
                _bytesEvacuatedFromMemory.addAndGet(clearMessageData(false));
            }
            return true;
        }

        @Override
        public String toString()
        {
            return this.getClass() + "[messageId=" + _messageId + "]";
        }

        @Override
        public synchronized void reallocate()
        {
            if (!_removed)
            {
                consolidateContentFragments();
                if (_metaData != null)
                {
                    _metaData.reallocate();
                }
                _data = QpidByteBuffer.reallocateIfNecessary(_data);
            }
        }

        public synchronized void clear(final boolean close)
        {
            if (!_removed)
            {
                clearMessageData(close);
            }
        }
    }


    private class BDBTransaction implements org.apache.qpid.server.store.Transaction
    {
        private final Transaction _txn;
        private final List<Runnable> _preCommitActions = new ArrayList<>();
        private final List<Runnable> _postCommitActions = new ArrayList<>();

        private int _storeSizeIncrease;

        private BDBTransaction() throws StoreException
        {
            try
            {
                _txn = getEnvironmentFacade().beginTransaction(null);
            }
            catch(RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot create store transaction", e);
            }
        }

        @Override
        public MessageEnqueueRecord enqueueMessage(TransactionLogResource queue, EnqueueableMessage message) throws StoreException
        {
            checkMessageStoreOpen();

            if(message.getStoredMessage() instanceof StoredBDBMessage)
            {
                final StoredBDBMessage storedMessage = (StoredBDBMessage) message.getStoredMessage();
                final long contentSize = storedMessage.getContentSize();
                _preCommitActions.add(() ->
                {
                    storedMessage.store(_txn);
                    _storeSizeIncrease += contentSize;
                });

            }

            AbstractBDBMessageStore.this.enqueueMessage(_txn, queue, message.getMessageNumber());
            return new BDBEnqueueRecord(queue.getId(), message.getMessageNumber());
        }

        @Override
        public void dequeueMessage(final MessageEnqueueRecord enqueueRecord)
        {
            checkMessageStoreOpen();

            AbstractBDBMessageStore.this.dequeueMessage(_txn, enqueueRecord.getQueueId(),
                                                        enqueueRecord.getMessageNumber());
        }

        @Override
        public void commitTran() throws StoreException
        {
            checkMessageStoreOpen();
            doPreCommitActions();
            AbstractBDBMessageStore.this.commitTranImpl(_txn);
            doPostCommitActions();
            AbstractBDBMessageStore.this.storedSizeChangeOccurred(_storeSizeIncrease);
        }

        private void doPreCommitActions()
        {
            for(Runnable action : _preCommitActions)
            {
                action.run();
            }
            _preCommitActions.clear();
        }

        private void doPostCommitActions()
        {
            // QPID-7447: prevent unnecessary allocation of empty iterator
            if (!_postCommitActions.isEmpty())
            {
                for (Runnable action : _postCommitActions)
                {
                    action.run();
                }
                _postCommitActions.clear();
            }
        }

        @Override
        public <X> CompletableFuture<X> commitTranAsync(final X val) throws StoreException
        {
            checkMessageStoreOpen();
            doPreCommitActions();
            AbstractBDBMessageStore.this.storedSizeChangeOccurred(_storeSizeIncrease);
            CompletableFuture<X> futureResult = AbstractBDBMessageStore.this.commitTranAsyncImpl(_txn, val);
            doPostCommitActions();
            return futureResult;
        }

        @Override
        public void abortTran() throws StoreException
        {
            checkMessageStoreOpen();
            _preCommitActions.clear();
            _postCommitActions.clear();
            AbstractBDBMessageStore.this.abortTran(_txn);
        }

        @Override
        public void removeXid(final StoredXidRecord record)
        {
            checkMessageStoreOpen();

            AbstractBDBMessageStore.this.removeXid(_txn, record.getFormat(), record.getGlobalId(), record.getBranchId());
        }

        @Override
        public StoredXidRecord recordXid(final long format, final byte[] globalId, final byte[] branchId, final EnqueueRecord[] enqueues,
                                         final DequeueRecord[] dequeues) throws StoreException
        {
            checkMessageStoreOpen();

            _postCommitActions.addAll(AbstractBDBMessageStore.this.recordXid(_txn, format, globalId, branchId, enqueues, dequeues));
            return new BDBStoredXidRecord(format, globalId, branchId);
        }

    }

    @Override
    public void addMessageDeleteListener(final MessageDeleteListener listener)
    {
        _messageDeleteListeners.add(listener);
    }

    @Override
    public void removeMessageDeleteListener(final MessageDeleteListener listener)
    {
        _messageDeleteListeners.remove(listener);
    }

    private static class BDBStoredXidRecord implements org.apache.qpid.server.store.Transaction.StoredXidRecord
    {
        private final long _format;
        private final byte[] _globalId;
        private final byte[] _branchId;

        public BDBStoredXidRecord(final long format, final byte[] globalId, final byte[] branchId)
        {
            _format = format;
            _globalId = globalId;
            _branchId = branchId;
        }

        @Override
        public long getFormat()
        {
            return _format;
        }

        @Override
        public byte[] getGlobalId()
        {
            return _globalId;
        }

        @Override
        public byte[] getBranchId()
        {
            return _branchId;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final BDBStoredXidRecord that = (BDBStoredXidRecord) o;

            return _format == that._format
                   && Arrays.equals(_globalId, that._globalId)
                   && Arrays.equals(_branchId, that._branchId);

        }

        @Override
        public int hashCode()
        {
            int result = (int) (_format ^ (_format >>> 32));
            result = 31 * result + Arrays.hashCode(_globalId);
            result = 31 * result + Arrays.hashCode(_branchId);
            return result;
        }
    }
    public static class BDBEnqueueRecord implements MessageEnqueueRecord
    {
        private final UUID _queueId;

        private final long _messageNumber;

        public BDBEnqueueRecord(final UUID queueid, final long messageNumber)
        {
            _queueId = queueid;
            _messageNumber = messageNumber;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageNumber;
        }

        @Override
        public UUID getQueueId()
        {
            return _queueId;
        }

    }

    final class BDBMessageStoreReader implements MessageStoreReader
    {
        private static final int INITIAL_MESSAGE_ID_CAPACITY = 8;
        private static final int RECOVERY_PAGE_SIZE = 1000;
        private static final long MAX_RECOVERY_SNAPSHOT_BYTES = (long) Integer.MAX_VALUE * RecoverySnapshot.BLOCK_BYTES;

        @Override
        public void visitMessages(final MessageHandler handler) throws StoreException
        {
            checkMessageStoreOpen();
            visitMessagesInternal(handler, getEnvironmentFacade());
        }

        @Override
        public StoredMessage<?> getMessage(final long messageId)
        {
            checkMessageStoreOpen();
            return getMessageInternal(messageId, getEnvironmentFacade());
        }

        @Override
        public void close()
        {

        }

        @Override
        public void visitMessageInstances(final TransactionLogResource queue, final MessageInstanceHandler handler)
                throws StoreException
        {
            checkMessageStoreOpen();

            long[] messageIds = new long[0];
            int messageCount = 0;
            final UUID queueId;
            try (final Cursor cursor = getDeliveryDb().openCursor(null, null))
            {
                queueId = queue.getId();
                final byte[] queuePrefix = QueueEntryBinding.queuePrefix(queueId);
                // JE writes the found key into the search entry.
                final DatabaseEntry key = new DatabaseEntry(queuePrefix.clone());
                final DatabaseEntry value = new DatabaseEntry();
                value.setPartial(0, 0, true);

                OperationStatus status = cursor.getSearchKeyRange(key, value, LockMode.READ_UNCOMMITTED);
                while (status == OperationStatus.SUCCESS && hasKeyPrefix(key, queuePrefix))
                {
                    if (messageCount == messageIds.length)
                    {
                        final int capacity = Math.max(INITIAL_MESSAGE_ID_CAPACITY, Math.multiplyExact(messageCount, 2));
                        messageIds = Arrays.copyOf(messageIds, capacity);
                    }
                    messageIds[messageCount++] = QueueEntryBinding.readMessageId(key);
                    status = cursor.getNext(key, value, LockMode.READ_UNCOMMITTED);
                }
            }
            catch (final RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot visit message instances", e);
            }

            for (int index = 0; index < messageCount; index++)
            {
                if (!handler.handle(new BDBEnqueueRecord(queueId, messageIds[index])))
                {
                    break;
                }
            }
        }

        private static boolean hasKeyPrefix(final DatabaseEntry key, final byte[] prefix)
        {
            final int offset = key.getOffset();
            final int length = prefix.length;
            return key.getSize() >= length &&
                    Arrays.equals(prefix, 0, length, key.getData(), offset, offset + length);
        }

        @Override
        public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
        {
            checkMessageStoreOpen();

            final List<BDBEnqueueRecord> entries = new ArrayList<>();
            try (final Cursor cursor = getDeliveryDb().openCursor(null, null))
            {
                final DatabaseEntry key = new DatabaseEntry();
                final QueueEntryBinding.OrderedDecoder decoder = new QueueEntryBinding.OrderedDecoder();

                final DatabaseEntry value = new DatabaseEntry();
                value.setPartial(0, 0, true);
                while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    entries.add(decoder.readIds(key));
                }
            }
            catch (RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot visit message instances", e);
            }

            for (final BDBEnqueueRecord entry : entries)
            {
                if (!handler.handle(entry))
                {
                    break;
                }
            }

        }

        @Override
        public void visitMessageInstancesForRecovery(final MessageInstanceHandler handler) throws StoreException
        {
            final long closeCount = _messageStoreCloseCount.get();
            checkMessageStoreOpen();
            final EnvironmentFacade environment = getEnvironmentFacade();
            final int snapshotBlockLimit = getRecoverySnapshotBlockLimit();
            checkRecoveryStoreOpen(environment, closeCount);

            // Check delivery keys before callbacks can make recovery writes.
            final RecoverySnapshot snapshot = new RecoverySnapshot(snapshotBlockLimit);
            final Database deliveryDb;
            byte[] resumeKey;
            try
            {
                deliveryDb = environment.openDatabase(DELIVERY_DB_NAME, DEFAULT_DATABASE_CONFIG);
                resumeKey = bufferValidatedRecoveryPrefix(deliveryDb, snapshot);
            }
            catch (final RuntimeException e)
            {
                throw environment.handleDatabaseException("Cannot visit message instances", e);
            }

            final QueueEntryBinding.OrderedDecoder decoder = new QueueEntryBinding.OrderedDecoder();
            final MessageInstanceHandler recoveryHandler = entry ->
            {
                if (!handler.handle(entry))
                {
                    return false;
                }
                checkRecoveryStoreOpen(environment, closeCount);
                return true;
            };
            checkRecoveryStoreOpen(environment, closeCount);
            if (!snapshot.visit(decoder, recoveryHandler) || resumeKey == null)
            {
                return;
            }

            final List<BDBEnqueueRecord> page = new ArrayList<>(RECOVERY_PAGE_SIZE);
            final DatabaseEntry key = new DatabaseEntry();

            while (resumeKey != null)
            {
                checkRecoveryStoreOpen(environment, closeCount);
                page.clear();
                try (final Cursor cursor = deliveryDb.openCursor(null, null))
                {
                    key.setData(resumeKey.clone());
                    OperationStatus status = cursor.getSearchKeyRange(key, null, LockMode.READ_UNCOMMITTED);
                    if (status == OperationStatus.SUCCESS &&
                            key.getSize() == resumeKey.length && hasKeyPrefix(key, resumeKey))
                    {
                        status = cursor.getNext(key, null, LockMode.READ_UNCOMMITTED);
                    }

                    resumeKey = null;
                    while (status == OperationStatus.SUCCESS)
                    {
                        page.add(decoder.readIds(key));
                        if (page.size() == RECOVERY_PAGE_SIZE)
                        {
                            resumeKey = copyKey(key);
                            break;
                        }
                        status = cursor.getNext(key, null, LockMode.READ_UNCOMMITTED);
                    }
                }
                catch (final RuntimeException e)
                {
                    throw environment.handleDatabaseException("Cannot visit message instances", e);
                }

                checkRecoveryStoreOpen(environment, closeCount);
                for (final BDBEnqueueRecord entry : page)
                {
                    if (!recoveryHandler.handle(entry))
                    {
                        return;
                    }
                }
            }
        }

        private int getRecoverySnapshotBlockLimit()
        {
            final Long maxBytes;
            try
            {
                maxBytes = BDBUtils.getContextValue(getParent(), Long.class,
                                                   QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES,
                                                   DEFAULT_BDB_RECOVERY_SNAPSHOT_MAX_BYTES);
            }
            catch (final IllegalArgumentException e)
            {
                throw new IllegalConfigurationException(QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES +
                                                        " must be a whole number of bytes", e);
            }
            if (maxBytes == null || maxBytes < RecoverySnapshot.BLOCK_BYTES || maxBytes > MAX_RECOVERY_SNAPSHOT_BYTES)
            {
                throw new IllegalConfigurationException(QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES +
                                                        " must be between " + RecoverySnapshot.BLOCK_BYTES + " and " +
                                                        MAX_RECOVERY_SNAPSHOT_BYTES + " bytes");
            }
            return (int) (maxBytes / RecoverySnapshot.BLOCK_BYTES);
        }

        private byte[] bufferValidatedRecoveryPrefix(final Database deliveryDb, final RecoverySnapshot snapshot)
        {
            final QueueEntryBinding.OrderedDecoder decoder = new QueueEntryBinding.OrderedDecoder();
            try (final Cursor cursor = deliveryDb.openCursor(null, null))
            {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry lastBufferedKey = new DatabaseEntry();
                while (cursor.getNext(key, null, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    final UUID queueId = decoder.readQueueId(key);
                    final long messageId = QueueEntryBinding.readMessageId(key);
                    if (!snapshot.add(queueId, messageId))
                    {
                        final byte[] resumeKey = copyKey(lastBufferedKey);
                        while (cursor.getNext(key, null, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                        {
                            QueueEntryBinding.validateKey(key);
                        }
                        return resumeKey;
                    }

                    // Keep the last buffered raw key while the cursor fills a separate entry.
                    final DatabaseEntry nextKey = lastBufferedKey;
                    lastBufferedKey = key;
                    key = nextKey;
                }
            }
            return null;
        }

        private static byte[] copyKey(final DatabaseEntry key)
        {
            final int offset = key.getOffset();
            return Arrays.copyOfRange(key.getData(), offset, offset + key.getSize());
        }

        private static final class RecoverySnapshot
        {
            private static final int BLOCK_BYTES = 8 * 1024;
            private static final int RUN_HEADER_BYTES = 3 * Long.BYTES + Integer.BYTES;
            private static final int DELTA_BITS_PER_BYTE = Byte.SIZE - 1;
            private static final int CONTINUATION_BIT = 1 << DELTA_BITS_PER_BYTE;
            private static final int DELTA_BYTE_MASK = CONTINUATION_BIT - 1;
            // Reserve zero for a full-width ID, and encode deltas in at most eight bytes.
            private static final long MAX_DELTA = (1L << (Long.BYTES * DELTA_BITS_PER_BYTE)) - 2;

            private final int _blockLimit;
            private final List<ByteBuffer> _blocks = new ArrayList<>();
            private ByteBuffer _block;
            private UUID _queueId;
            private long _lastMessageId;
            private int _runCountOffset;
            private int _runCount;

            private RecoverySnapshot(final int blockLimit)
            {
                _blockLimit = blockLimit;
            }

            // A queue run starts with UUID bits, its first message ID and a count.
            private boolean add(final UUID queueId, final long messageId)
            {
                final long delta = messageId - _lastMessageId;
                if (_queueId == queueId && _block.remaining() >= deltaBytes(delta))
                {
                    writeDelta(_block, delta, messageId);
                    _lastMessageId = messageId;
                    _runCount++;
                    return true;
                }

                if (_block == null || _block.remaining() < RUN_HEADER_BYTES)
                {
                    if (_blocks.size() == _blockLimit)
                    {
                        return false;
                    }
                    finishBlock();
                    _block = ByteBuffer.allocate(BLOCK_BYTES);
                    _blocks.add(_block);
                }
                else
                {
                    finishRun();
                }
                _queueId = queueId;
                _lastMessageId = messageId;
                _block.putLong(queueId.getMostSignificantBits()).putLong(queueId.getLeastSignificantBits())
                        .putLong(messageId);
                _runCountOffset = _block.position();
                _block.putInt(0);
                _runCount = 1;
                return true;
            }

            private static boolean fitsPackedDelta(final long delta)
            {
                // Subtraction can wrap when the sorted IDs span more than Long.MAX_VALUE.
                return delta >= 0 && delta <= MAX_DELTA;
            }

            private static int deltaBytes(final long delta)
            {
                if (!fitsPackedDelta(delta))
                {
                    return Byte.BYTES + Long.BYTES;
                }
                final int bits = Long.SIZE - Long.numberOfLeadingZeros(delta + 1);
                return (bits + DELTA_BITS_PER_BYTE - 1) / DELTA_BITS_PER_BYTE;
            }

            private static void writeDelta(final ByteBuffer block, final long delta, final long messageId)
            {
                if (!fitsPackedDelta(delta))
                {
                    block.put((byte) 0).putLong(messageId);
                    return;
                }
                // Adding one also gives two equal decoded message IDs a nonzero code.
                long code = delta + 1;
                while (code > DELTA_BYTE_MASK)
                {
                    block.put((byte) ((code & DELTA_BYTE_MASK) | CONTINUATION_BIT));
                    code >>>= DELTA_BITS_PER_BYTE;
                }
                block.put((byte) code);
            }

            private static long readMessageId(final ByteBuffer block, final long previousId)
            {
                int next = Byte.toUnsignedInt(block.get());
                if (next == 0)
                {
                    return block.getLong();
                }
                long code = next & DELTA_BYTE_MASK;
                for (int shift = DELTA_BITS_PER_BYTE; next >= CONTINUATION_BIT; shift += DELTA_BITS_PER_BYTE)
                {
                    next = Byte.toUnsignedInt(block.get());
                    code |= (long) (next & DELTA_BYTE_MASK) << shift;
                }
                return previousId + (code - 1);
            }

            private void finishRun()
            {
                _block.putInt(_runCountOffset, _runCount);
            }

            private void finishBlock()
            {
                if (_block != null)
                {
                    finishRun();
                    _block.flip();
                }
            }

            private boolean visit(final QueueEntryBinding.OrderedDecoder decoder, final MessageInstanceHandler handler)
            {
                finishBlock();
                for (final ByteBuffer block : _blocks)
                {
                    while (block.hasRemaining())
                    {
                        final UUID queueId = decoder.readQueueId(block.getLong(), block.getLong());
                        long messageId = block.getLong();
                        final int count = block.getInt();
                        if (!handler.handle(new BDBEnqueueRecord(queueId, messageId)))
                        {
                            return false;
                        }
                        for (int index = 1; index < count; index++)
                        {
                            messageId = readMessageId(block, messageId);
                            if (!handler.handle(new BDBEnqueueRecord(queueId, messageId)))
                            {
                                return false;
                            }
                        }
                    }
                }
                _blocks.clear();
                _block = null;
                return true;
            }
        }

        private void checkRecoveryStoreOpen(final EnvironmentFacade environment, final long closeCount)
        {
            checkMessageStoreOpen();
            if (environment == null || closeCount != _messageStoreCloseCount.get() ||
                    environment != getEnvironmentFacade())
            {
                throw new IllegalStateException("Message store changed during delivery recovery");
            }
        }

        @Override
        public void visitDistributedTransactions(final DistributedTransactionHandler handler) throws StoreException
        {
            checkMessageStoreOpen();

            try(Cursor cursor = getXidDb().openCursor(null, null))
            {
                CachingUUIDFactory uuidFactory = new CachingUUIDFactory();
                DatabaseEntry key = new DatabaseEntry();
                XidBinding keyBinding = XidBinding.getInstance();
                DatabaseEntry value = new DatabaseEntry();

                while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    Xid xid = keyBinding.entryToObject(key);
                    PreparedTransaction preparedTransaction = PreparedTransactionBinding.entryToObject(uuidFactory, value);
                    if (!handler.handle(new BDBStoredXidRecord(xid.getFormat(), xid.getGlobalId(), xid.getBranchId()),
                                        preparedTransaction.getEnqueues(), preparedTransaction.getDequeues()))
                    {
                        break;
                    }
                }
            }
            catch (RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot recover distributed transactions", e);
            }
        }


    }
}
