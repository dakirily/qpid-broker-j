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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

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

    private static final ThreadLocal<DatabaseEntry> MESSAGE_ID_ENTRY = ThreadLocal.withInitial(() ->
            new DatabaseEntry(new byte[8]));

    private static final ThreadLocal<DatabaseEntry> ENQUEUE_RECORD_ENTRY = ThreadLocal.withInitial(() ->
            new DatabaseEntry(new byte[0]));

    private static final ThreadLocal<DatabaseEntry> QUEUE_ENTRY = ThreadLocal.withInitial(() ->
            new DatabaseEntry(new byte[QueueEntryBinding.KEY_SIZE]));

    private final AtomicBoolean _messageStoreOpen = new AtomicBoolean();

    private final EventManager _eventManager = new EventManager();

    private final DatabaseEntry MESSAGE_METADATA_SEQ_KEY = new DatabaseEntry("MESSAGE_METADATA_SEQ_KEY"
            .getBytes(StandardCharsets.UTF_8));

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
    private final AtomicLong _inMemorySize = new AtomicLong();
    private final AtomicLong _bytesEvacuatedFromMemory = new AtomicLong();
    private final Set<StoredBDBMessage<?>> _messages = ConcurrentHashMap.newKeySet();
    private final Set<MessageDeleteListener> _messageDeleteListeners = ConcurrentHashMap.newKeySet();

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
            for (final StoredBDBMessage<?> message : _messages)
            {
                message.clear(true);
            }
            _messages.clear();
            _inMemorySize.set(0L);
            _bytesEvacuatedFromMemory.set(0L);
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
        catch (final RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Cannot upgrade store", e);
        }
    }

    void deleteMessageStoreDatabases()
    {
        try
        {
            for (final String db : List.of(MESSAGE_META_DATA_DB_NAME, MESSAGE_META_DATA_SEQ_DB_NAME,
                    MESSAGE_CONTENT_DB_NAME, DELIVERY_DB_NAME, XID_DB_NAME))
            {
                try
                {
                    getEnvironmentFacade().deleteDatabase(db);
                }
                catch (final DatabaseNotFoundException ignore)
                {
                    // no-op
                }
            }
        }
        catch (final IllegalStateException e)
        {
            getLogger().warn("Could not delete message store databases: {}", e.getMessage());
        }
        catch (final RuntimeException e)
        {
            getEnvironmentFacade().handleDatabaseException("Deletion of message store databases failed", e);
        }
    }

    @Override
    public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(final T metaData)
    {
        final long newMessageId = getNextMessageId();
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
            final Sequence mmdSeq = getEnvironmentFacade().openSequence(getMessageMetaDataSeqDb(),
                    MESSAGE_METADATA_SEQ_KEY, MESSAGE_METADATA_SEQ_CONFIG);
            newMessageId = mmdSeq.get(null, 1);
        }
        catch (final LockTimeoutException le)
        {
           throw new ConnectionScopedRuntimeException("Unexpected exception on BDB sequence", le);
        }
        catch (final RuntimeException de)
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

    /**
     * Retrieves message meta-data.
     *
     * @param messageId The message to get the meta-data for.
     *
     * @return The message metadata.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    StorableMessageMetaData getMessageMetaData(final long messageId) throws StoreException
    {
        getLogger().debug("public MessageMetaData getMessageMetaData(Long messageId = {}): called", messageId);

        final DatabaseEntry key = MESSAGE_ID_ENTRY.get();
        QueueEntryBinding.writeMessageId(messageId, key);
        final DatabaseEntry value = new DatabaseEntry();

        try
        {
            final OperationStatus status = getMessageMetaDataDb().get(null, key, value, LockMode.READ_UNCOMMITTED);
            if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Metadata not found for message with id " + messageId);
            }
            return MessageMetaDataBinding.getInstance().entryToObject(value);
        }
        catch (final RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error reading message metadata for message with id " +
                    messageId + ": " + e.getMessage(), e);
        }
    }

    void removeMessage(final long messageId) throws StoreException
    {
        boolean complete = false;
        Transaction tx = null;
        int attempts = 0;
        try
        {
            final DatabaseEntry key = MESSAGE_ID_ENTRY.get();
            QueueEntryBinding.writeMessageId(messageId, key);
            do
            {
                tx = null;
                try
                {
                    tx = getEnvironmentFacade().beginTransaction(null);

                    // remove the message metadata from the store
                    getLogger().debug("Removing message id {}", messageId);
                    final OperationStatus status = getMessageMetaDataDb().delete(tx, key);
                    if (status == OperationStatus.NOTFOUND)
                    {
                        getLogger().debug("Message id {} not found (attempt to remove failed - probably application initiated rollback)",messageId);
                    }
                    getLogger().debug("Deleted metadata for message {}", messageId);

                    // now remove the content data from the store if there is any
                    getMessageContentDb().delete(tx, key);

                    getLogger().debug("Deleted content for message {}", messageId);

                    getEnvironmentFacade().commitNoSync(tx);

                    complete = true;
                    tx = null;
                }
                catch (final LockConflictException e)
                {
                    try
                    {
                        if (tx != null)
                        {
                            tx.abort();
                        }
                    }
                    catch (final RuntimeException e2)
                    {
                        getLogger().warn("Unable to abort transaction after LockConflictException on removal of message with id {}", messageId, e2);
                        // rethrow the original log conflict exception, the secondary exception should already have
                        // been logged.
                        throw getEnvironmentFacade().handleDatabaseException("Cannot remove message with id " + messageId, e);
                    }

                    sleepOrThrowOnLockConflict(attempts++, "Cannot remove messages", e);
                }
            }
            while (!complete);
        }
        catch (final RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Unexpected BDB exception", e);
            }

            try
            {
                abortTransactionSafely(tx, getEnvironmentFacade());
            }
            finally
            {
                tx = null;
            }

            throw getEnvironmentFacade().handleDatabaseException("Error removing message with id " + messageId +
                    " from database: " + e.getMessage(), e);
        }
        finally
        {
            try
            {
                abortTransactionSafely(tx, getEnvironmentFacade());
            }
            finally
            {
                tx = null;
            }
        }
    }

    QpidByteBuffer getAllContent(final long messageId) throws StoreException
    {
        final DatabaseEntry contentKeyEntry = MESSAGE_ID_ENTRY.get();
        QueueEntryBinding.writeMessageId(messageId, contentKeyEntry);
        final DatabaseEntry value = new DatabaseEntry();

        getLogger().debug("Message Id: {} Getting content body", messageId);

        try
        {
            final OperationStatus status = getMessageContentDb().get(null, contentKeyEntry, value, LockMode.READ_UNCOMMITTED);

            if (status == OperationStatus.SUCCESS)
            {
                final byte[] data = value.getData();
                final int offset = value.getOffset();
                final int length = value.getSize();
                final QpidByteBuffer buf = QpidByteBuffer.allocateDirect(length);
                buf.put(data, offset, length);
                buf.flip();
                return buf;
            }
            else
            {
                throw new StoreException("Unable to find message with id " + messageId);
            }
        }
        catch (final RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error getting AMQMessage with id " + messageId +
                    " to database: " + e.getMessage(), e);
        }
    }

    private void visitMessagesInternal(final MessageHandler handler, final EnvironmentFacade environmentFacade)
    {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry value = new DatabaseEntry();
        final MessageMetaDataBinding valueBinding = MessageMetaDataBinding.getInstance();

        try (final Cursor cursor = getMessageMetaDataDb().openCursor(null, null))
        {
            while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
            {
                final long messageId = LongBinding.entryToLong(key);
                final StorableMessageMetaData metaData = valueBinding.entryToObject(value);
                final StoredBDBMessage<?> message = createStoredBDBMessage(messageId, metaData, true);
                if (!handler.handle(message))
                {
                    break;
                }
            }
        }
        catch (final RuntimeException e)
        {
            throw environmentFacade.handleDatabaseException("Cannot visit messages", e);
        }
    }

    private void sleepOrThrowOnLockConflict(final int attempts, final String throwMessage, final LockConflictException cause)
    {
        if (attempts < LOCK_RETRY_ATTEMPTS)
        {
            getLogger().info("Lock conflict exception. Retrying (attempt {} of {})", attempts, LOCK_RETRY_ATTEMPTS);
            try
            {
                Thread.sleep(500L + ThreadLocalRandom.current().nextLong(500));
            }
            catch (final InterruptedException ie)
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
            final DatabaseEntry key = MESSAGE_ID_ENTRY.get();
            QueueEntryBinding.writeMessageId(messageId, key);
            final DatabaseEntry value = new DatabaseEntry();
            final MessageMetaDataBinding valueBinding = MessageMetaDataBinding.getInstance();
            if (getMessageMetaDataDb().get(null, key, value, LockMode.READ_COMMITTED) == OperationStatus.SUCCESS)
            {
                final StorableMessageMetaData metaData = valueBinding.entryToObject(value);
                return createStoredBDBMessage(messageId, metaData, true);
            }
            return null;
        }
        catch (final RuntimeException e)
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
    private void addContent(final Transaction tx, long messageId, final QpidByteBuffer contentBody) throws StoreException
    {
        final DatabaseEntry key = MESSAGE_ID_ENTRY.get();
        QueueEntryBinding.writeMessageId(messageId, key);
        final DatabaseEntry value = new DatabaseEntry();

        final byte[] data = new byte[contentBody.remaining()];
        contentBody.copyTo(data);
        value.setData(data);
        try
        {
            final OperationStatus status = getMessageContentDb().put(tx, key, value);
            if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Error adding content for message id " + messageId + ": " + status);
            }

            getLogger().debug("Storing content for message {} in transaction {}", messageId, tx);

        }
        catch (final RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Error writing AMQMessage with id " + messageId +
                    " to database: " + e.getMessage(), e);
        }
    }

    /**
     * Stores message meta-data.
     *
     * @param tx         The transaction for the operation.
     * @param messageId       The message to store the data for.
     * @param messageMetaData The message metadata to store.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason, or if the specified message does not exist.
     */
    private void storeMetaData(final Transaction tx, final long messageId, final StorableMessageMetaData messageMetaData)
            throws StoreException
    {
        getLogger().debug("storeMetaData called for transaction {}, messageId {}, messageMetaData {} ", tx, messageId, messageMetaData);

        final DatabaseEntry key = MESSAGE_ID_ENTRY.get();
        QueueEntryBinding.writeMessageId(messageId, key);

        final DatabaseEntry value = new DatabaseEntry();
        MessageMetaDataBinding.getInstance().objectToEntry(messageMetaData, value);

        boolean complete = false;
        int attempts = 0;

        do
        {
            try
            {
                getMessageMetaDataDb().put(tx, key, value);
                getLogger().debug("Storing message metadata for message id {} in transaction {}", messageId, tx);
                complete = true;

            }
            catch (final LockConflictException e)
            {
                sleepOrThrowOnLockConflict(attempts++, "Cannot store metadata", e);
            }
            catch (final RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Error writing message metadata with id " + messageId +
                        " to database: " + e.getMessage(), e);
            }
        }
        while(!complete);
    }

    /**
     * Places a message onto a specified queue, in a given transaction.
     *
     * @param tx   The transaction for the operation.
     * @param queue     The queue to place the message on.
     * @param messageId The message to enqueue.
     *
     * @throws org.apache.qpid.server.store.StoreException If the operation fails for any reason.
     */
    private void enqueueMessage(final Transaction tx, final TransactionLogResource queue, final long messageId)
            throws StoreException
    {
        final DatabaseEntry key = QUEUE_ENTRY.get();
        QueueEntryBinding.objectToEntry(queue.getId(), messageId, key);

        try
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Enqueuing message {} on queue {} with id {} in transaction {}", messageId,
                        queue.getName(), queue.getId(), tx);
            }
            final DatabaseEntry databaseEntry = ENQUEUE_RECORD_ENTRY.get();
            getDeliveryDb().put(tx, key, databaseEntry);
        }
        catch (final RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Failed to enqueue: {}", e.getMessage(), e);
            }
            throw getEnvironmentFacade().handleDatabaseException("Error writing enqueued message with id " + messageId +
                    " for queue " + queue.getName() + " with id " + queue.getId() + " to database", e);
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
        final DatabaseEntry key = QUEUE_ENTRY.get();
        QueueEntryBinding.objectToEntry(queueId, messageId, key);

        getLogger().debug("Dequeue message id {} from queue with id {}", messageId, queueId);

        try
        {
            final OperationStatus status = getDeliveryDb().delete(tx, key);
            if (status == OperationStatus.NOTFOUND)
            {
                throw new StoreException("Unable to find message with id " + messageId + " on queue with id "  + queueId);
            }
            else if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Unable to remove message with id " + messageId + " on queue with id " + queueId);
            }

            getLogger().debug("Removed message {} on queue with id {}", messageId, queueId);

        }
        catch (final RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Failed to dequeue message {} in transaction {}", messageId, tx, e);
            }

            throw getEnvironmentFacade().handleDatabaseException("Error accessing database while dequeuing message: "
                    + e.getMessage(), e);
        }
    }

    private List<Runnable> recordXid(final Transaction txn,
                                     final long format,
                                     final byte[] globalId,
                                     final byte[] branchId,
                                     final org.apache.qpid.server.store.Transaction.EnqueueRecord[] enqueues,
                                     final org.apache.qpid.server.store.Transaction.DequeueRecord[] dequeues) throws StoreException
    {
        final DatabaseEntry key = new DatabaseEntry();
        final Xid xid = new Xid(format, globalId, branchId);
        final XidBinding keyBinding = XidBinding.getInstance();
        keyBinding.objectToEntry(xid,key);

        final DatabaseEntry value = new DatabaseEntry();
        final PreparedTransaction preparedTransaction = new PreparedTransaction(enqueues, dequeues);
        PreparedTransactionBinding.objectToEntry(preparedTransaction, value);
        for (final org.apache.qpid.server.store.Transaction.EnqueueRecord enqueue : enqueues)
        {
            final StoredMessage<?> storedMessage = enqueue.getMessage().getStoredMessage();
            if (storedMessage instanceof StoredBDBMessage<?> storedBDBMessage)
            {
                storedBDBMessage.store(txn);
            }
        }

        try
        {
            getXidDb().put(txn, key, value);
            return Collections.emptyList();
        }
        catch (final RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Failed to write xid: {}", e.getMessage(), e);
            }
            throw getEnvironmentFacade().handleDatabaseException("Error writing xid to database", e);
        }
    }

    private void removeXid(final Transaction txn, final long format, final byte[] globalId, final byte[] branchId)
            throws StoreException
    {
        final DatabaseEntry key = new DatabaseEntry();
        final Xid xid = new Xid(format, globalId, branchId);
        final XidBinding keyBinding = XidBinding.getInstance();

        keyBinding.objectToEntry(xid, key);

        try
        {
            final OperationStatus status = getXidDb().delete(txn, key);
            if (status == OperationStatus.NOTFOUND)
            {
                throw new StoreException("Unable to find xid");
            }
            else if (status != OperationStatus.SUCCESS)
            {
                throw new StoreException("Unable to remove xid");
            }

        }
        catch (final RuntimeException e)
        {
            if (getLogger().isDebugEnabled())
            {
                getLogger().error("Failed to remove xid in transaction", e);
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

    private <X> CompletableFuture<X> commitTranAsyncImpl(final Transaction tx, final X val) throws StoreException
    {
        if (tx == null)
        {
            throw new StoreException("Fatal internal error: transactional is null at commitTran");
        }
        final CompletableFuture<X> result = getEnvironmentFacade().commitAsync(tx, val);
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
        catch (final RuntimeException e)
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
        catch (final RuntimeException e)
        {
            throw getEnvironmentFacade().handleDatabaseException("Stored size change exception", e);
        }
    }

    private void storedSizeChange(final int delta)
    {
        if (getPersistentSizeHighThreshold() > 0)
        {
            synchronized (this)
            {
                // the delta supplied is an approximation of a store size change. we don;t want to check the statistic every
                // time, so we do so only when there's been enough change that it is worth looking again. We do this by
                // assuming the total size will change by less than twice the amount of the message data change.
                final long newSize = _totalStoreSize += 2L * delta;

                if (!_limitBusted &&  newSize > getPersistentSizeHighThreshold())
                {
                    _totalStoreSize = getSizeOnDisk();

                    if (_totalStoreSize > getPersistentSizeHighThreshold())
                    {
                        _limitBusted = true;
                        _eventManager.notifyEvent(Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);
                    }
                }
                else if (_limitBusted && newSize < getPersistentSizeLowThreshold())
                {
                    final long oldSize = _totalStoreSize;
                    _totalStoreSize = getSizeOnDisk();

                    if (oldSize <= _totalStoreSize)
                    {
                        reduceSizeOnDisk();
                        _totalStoreSize = getSizeOnDisk();
                    }

                    if (_totalStoreSize < getPersistentSizeLowThreshold())
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

    private static class MessageDataRef<T extends StorableMessageMetaData>
    {
        private volatile T _metaData;
        private volatile QpidByteBuffer _data;
        private volatile boolean _isHardRef;

        private MessageDataRef(final T metaData, final boolean isHardRef)
        {
            this(metaData, null, isHardRef);
        }

        private MessageDataRef(final T metaData, final QpidByteBuffer data, final boolean isHardRef)
        {
            _metaData = metaData;
            _data = data;
            _isHardRef = isHardRef;
        }

        public T getMetaData()
        {
            return _metaData;
        }

        public QpidByteBuffer getData()
        {
            return _data;
        }

        public void setData(final QpidByteBuffer data)
        {
            _data = data;
        }

        public boolean isHardRef()
        {
            return _isHardRef;
        }

        public void setSoft()
        {
            _isHardRef = false;
        }

        public void reallocate()
        {
            if (_metaData != null)
            {
                _metaData.reallocate();
            }
            _data = QpidByteBuffer.reallocateIfNecessary(_data);
        }

        public long clear (final boolean close)
        {
            long bytesCleared = 0;
            if (_data != null)
            {
                if (_data != null)
                {
                    bytesCleared += _data.remaining();
                    _data.dispose();
                    _data = null;
                }
            }
            if (_metaData != null)
            {
                bytesCleared += _metaData.getStorableSize();
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

    final class StoredBDBMessage<T extends StorableMessageMetaData> implements StoredMessage<T>, MessageHandle<T>
    {
        private final long _messageId;
        private final int _contentSize;
        private final int _metadataSize;
        private MessageDataRef<T> _messageDataRef;
        private List<QpidByteBuffer> _contentFragments;

        StoredBDBMessage(final long messageId, final T metaData, final boolean isRecovered)
        {
            _messageId = messageId;
            _messageDataRef = new MessageDataRef<>(metaData, !isRecovered);
            _contentSize = metaData.getContentSize();
            _metadataSize = metaData.getStorableSize();
            _inMemorySize.addAndGet(_metadataSize);
        }

        @Override
        public synchronized T getMetaData()
        {
            if (_messageDataRef == null)
            {
                return null;
            }

            T metaData = _messageDataRef.getMetaData();

            if (metaData == null)
            {
                checkMessageStoreOpen();
                metaData = (T) getMessageMetaData(_messageId);
                _messageDataRef = new MessageDataRef<>(metaData, _messageDataRef.getData(), false);
                _inMemorySize.addAndGet(getMetadataSize());
            }
            return metaData;
        }

        @Override
        public long getMessageNumber()
        {
            return _messageId;
        }

        @Override
        public synchronized void addContent(final QpidByteBuffer src)
        {
            // accumulate content fragments and consolidate lazily
            if (_messageDataRef == null)
            {
                return;
            }

            if (_contentFragments == null)
            {
                _contentFragments = new ArrayList<>();

                final QpidByteBuffer existingData = _messageDataRef.getData();
                if (existingData != null)
                {
                    _contentFragments.add(existingData);
                    _messageDataRef.setData(null);
                }
            }
            _contentFragments.add(src.slice());
        }

        @Override
        public synchronized StoredMessage<T> allContentAdded()
        {
            consolidateContentFragmentsIfNecessary();
            _inMemorySize.addAndGet(getContentSize());
            return this;
        }

        /**
         * returns QBB containing the content. The caller must not dispose of them because we keep a reference in _messageDataRef.
         */
        private QpidByteBuffer getContentAsByteBuffer()
        {
            QpidByteBuffer data = _messageDataRef == null ? QpidByteBuffer.emptyQpidByteBuffer() : _messageDataRef.getData();
            if (data == null)
            {
                if (_contentFragments != null && !_contentFragments.isEmpty())
                {
                    data = consolidateContentFragmentsIfNecessary();
                }
                else if (stored())
                {
                    checkMessageStoreOpen();
                    data = AbstractBDBMessageStore.this.getAllContent(_messageId);
                    _messageDataRef.setData(data);
                    _inMemorySize.addAndGet(getContentSize());
                }
                else
                {
                    data = QpidByteBuffer.emptyQpidByteBuffer();
                }
            }
            return data;
        }

        private QpidByteBuffer consolidateContentFragmentsIfNecessary()
        {
            if (_messageDataRef == null)
            {
                return QpidByteBuffer.emptyQpidByteBuffer();
            }

            final QpidByteBuffer existingData = _messageDataRef.getData();
            if (_contentFragments == null || _contentFragments.isEmpty())
            {
                return existingData;
            }

            final List<QpidByteBuffer> fragments = _contentFragments;
            _contentFragments = null;

            if (fragments.size() == 1)
            {
                final QpidByteBuffer single = fragments.get(0);
                _messageDataRef.setData(single);
                return single;
            }

            final QpidByteBuffer consolidated = QpidByteBuffer.concatenate(fragments);
            for (final QpidByteBuffer fragment : fragments)
            {
                fragment.dispose();
            }

            _messageDataRef.setData(consolidated);
            return consolidated;
        }

        @Override
        public synchronized QpidByteBuffer getContent(final int offset, int length)
        {
            final QpidByteBuffer contentAsByteBuffer = getContentAsByteBuffer();
            if (length == Integer.MAX_VALUE)
            {
                length = contentAsByteBuffer.remaining();
            }
            return contentAsByteBuffer.view(offset, length);
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
            if (!stored())
            {
                consolidateContentFragmentsIfNecessary();
                AbstractBDBMessageStore.this.storeMetaData(txn, _messageId, _messageDataRef.getMetaData());
                AbstractBDBMessageStore.this.addContent(txn, _messageId, _messageDataRef.getData() == null
                        ? QpidByteBuffer.emptyQpidByteBuffer() : _messageDataRef.getData());
                _messageDataRef.setSoft();
            }
        }

        synchronized void flushToStore()
        {
            if (_messageDataRef != null)
            {
                if (!stored())
                {
                    checkMessageStoreOpen();

                    Transaction txn;
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
        }

        @Override
        public synchronized void remove()
        {
            checkMessageStoreOpen();
            _messages.remove(this);
            if (stored())
            {
                removeMessage(_messageId);
                storedSizeChangeOccurred(-getContentSize());
            }
            if (!_messageDeleteListeners.isEmpty())
            {
                for (final MessageDeleteListener messageDeleteListener : _messageDeleteListeners)
                {
                    messageDeleteListener.messageDeleted(this);
                }
            }

            final T metaData;
            long bytesCleared = 0;
            if ((metaData =_messageDataRef.getMetaData()) != null)
            {
                bytesCleared += getMetadataSize();
                metaData.dispose();
            }

            bytesCleared += clearContentFragments();

            try (final QpidByteBuffer data = _messageDataRef.getData())
            {
                if (data != null)
                {
                    bytesCleared += getContentSize();
                    _messageDataRef.setData(null);
                }
            }
            _messageDataRef = null;
            _inMemorySize.addAndGet(-bytesCleared);
        }

        private long clearContentFragments()
        {
            long bytesCleared = 0;
            if (_contentFragments != null)
            {
                for (final QpidByteBuffer fragment : _contentFragments)
                {
                    bytesCleared += fragment.remaining();
                    fragment.dispose();
                }
                _contentFragments = null;
            }
            return bytesCleared;
        }

        @Override
        public synchronized boolean isInContentInMemory()
        {
            return _messageDataRef != null
                    && (_messageDataRef.isHardRef()
                    || _messageDataRef.getData() != null
                    || (_contentFragments != null && !_contentFragments.isEmpty()));
        }

        @Override
        public synchronized long getInMemorySize()
        {
            long size = 0;
            if (_messageDataRef != null)
            {
                if (_messageDataRef.isHardRef())
                {
                    size += getMetadataSize() + getContentSize();
                }
                else
                {
                    if (_messageDataRef.getMetaData() != null)
                    {
                        size += getMetadataSize();
                    }
                    if (_messageDataRef.getData() != null || (_contentFragments != null && !_contentFragments.isEmpty()))
                    {
                        size += getContentSize();
                    }
                }
            }
            return size;
        }

        private boolean stored()
        {
            return _messageDataRef != null && !_messageDataRef.isHardRef();
        }

        @Override
        public synchronized boolean flowToDisk()
        {
            flushToStore();
            if (_messageDataRef != null && !_messageDataRef.isHardRef())
            {
                final long bytesCleared = _messageDataRef.clear(false) + clearContentFragments();
                _inMemorySize.addAndGet(-bytesCleared);
                _bytesEvacuatedFromMemory.addAndGet(bytesCleared);
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
            if (_messageDataRef != null)
            {
                consolidateContentFragmentsIfNecessary();
                _messageDataRef.reallocate();
            }
        }

        public synchronized void clear(final boolean close)
        {
            if (_messageDataRef != null)
            {
                _messageDataRef.clear(close);
                clearContentFragments();
            }
        }
    }

    private class BDBTransaction implements org.apache.qpid.server.store.Transaction
    {
        private final Transaction _txn;
        private final List<Runnable> _postCommitActions = new ArrayList<>();
        private final ArrayList<StoredBDBMessage<?>> _messagesToStore = new ArrayList<>();
        private final IdentityHashMap<StoredBDBMessage<?>, Boolean> _messagesToStoreSet = new IdentityHashMap<>();

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

            if (message.getStoredMessage() instanceof StoredBDBMessage<?> storedMessage)
            {
                if (_messagesToStoreSet.put(storedMessage, Boolean.TRUE) == null)
                {
                    _messagesToStore.add(storedMessage);
                    _storeSizeIncrease += storedMessage.getContentSize();
                }
            }

            AbstractBDBMessageStore.this.enqueueMessage(_txn, queue, message.getMessageNumber());
            return new BDBEnqueueRecord(queue.getId(), message.getMessageNumber());
        }

        @Override
        public void dequeueMessage(final MessageEnqueueRecord enqueueRecord)
        {
            checkMessageStoreOpen();
            AbstractBDBMessageStore.this.dequeueMessage(_txn, enqueueRecord.getQueueId(), enqueueRecord.getMessageNumber());
        }

        @Override
        public void commitTran() throws StoreException
        {
            checkMessageStoreOpen();
            doPreCommitStores();
            AbstractBDBMessageStore.this.commitTranImpl(_txn);
            doPostCommitActions();
            AbstractBDBMessageStore.this.storedSizeChangeOccurred(_storeSizeIncrease);
        }

        private void doPreCommitStores()
        {
            for (final StoredBDBMessage<?> message : _messagesToStore)
            {
                message.store(_txn);
            }
            _messagesToStore.clear();
            _messagesToStoreSet.clear();
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
            doPreCommitStores();
            AbstractBDBMessageStore.this.storedSizeChangeOccurred(_storeSizeIncrease);
            CompletableFuture<X> futureResult = AbstractBDBMessageStore.this.commitTranAsyncImpl(_txn, val);
            doPostCommitActions();
            return futureResult;
        }

        @Override
        public void abortTran() throws StoreException
        {
            checkMessageStoreOpen();
            _messagesToStore.clear();
            _messagesToStoreSet.clear();
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
        public StoredXidRecord recordXid(final long format,
                                         final byte[] globalId,
                                         final byte[] branchId,
                                         final EnqueueRecord[] enqueues,
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
            int result = Long.hashCode(_format);
            result = 31 * result + Arrays.hashCode(_globalId);
            result = 31 * result + Arrays.hashCode(_branchId);
            return result;
        }
    }

    public record BDBEnqueueRecord(UUID queueId, long messageNumber) implements MessageEnqueueRecord
    {
        @Override
        public UUID getQueueId()
        {
            return queueId;
        }

        @Override
        public long getMessageNumber()
        {
            return messageNumber;
        }
    }

    private class BDBMessageStoreReader implements MessageStoreReader
    {
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
        public void visitMessageInstances(final TransactionLogResource queue, final MessageInstanceHandler handler) throws StoreException
        {
            checkMessageStoreOpen();

            long[] entries = new long[1024];
            int size = 0;

            try (final Cursor cursor = getDeliveryDb().openCursor(null, null))
            {
                final DatabaseEntry key = new DatabaseEntry(new byte[QueueEntryBinding.KEY_SIZE]);

                QueueEntryBinding.objectToEntry(queue.getId(), 0L, key);

                if (cursor.getSearchKeyRange(key, null, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    do
                    {
                        if (QueueEntryBinding.matchesQueueId(key, queue.getId()))
                        {
                            if (size == entries.length)
                            {
                                entries = Arrays.copyOf(entries, entries.length * 2);
                            }
                            entries[size++] = QueueEntryBinding.readMessageId(key);
                        }
                        else
                        {
                            break;
                        }
                    }
                    while (cursor.getNext(key, null, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
                }
            }
            catch (final RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot visit message instances", e);
            }

            for (int i = 0; i < size; i ++)
            {
                if (!handler.handle(new BDBEnqueueRecord(queue.getId(), entries[i])))
                {
                    break;
                }
            }
        }

        @Override
        public void visitMessageInstances(final MessageInstanceHandler validHandler,
                                          final Predicate<MessageEnqueueRecord> predicate,
                                          final Consumer<MessageEnqueueRecord> invalidCollector) throws StoreException
        {
            checkMessageStoreOpen();

            final CachingUUIDFactory uuidFactory = new CachingUUIDFactory();

            // Callbacks are invoked while iterating a BDB cursor; BDB resources/locks may be held.
            // Callbacks must be fast and must not re-enter the store (including creating transactions, reading
            // messages, or performing other database operations). Only validHandler controls early termination.
            try (final Cursor cursor = getDeliveryDb().openCursor(null, null))
            {
                final DatabaseEntry key = new DatabaseEntry(new byte[QueueEntryBinding.KEY_SIZE]);

                while (cursor.getNext(key, null, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    final MessageEnqueueRecord record = QueueEntryBinding.readIds(uuidFactory, key);

                    if (predicate.test(record))
                    {
                        if (!validHandler.handle(record))
                        {
                            break;
                        }
                    }
                    else
                    {
                        invalidCollector.accept(record);
                    }
                }
            }
            catch (final RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot visit message instances", e);
            }
        }

        @Override
        public void visitMessageInstances(final TransactionLogResource queue,
                                          final MessageInstanceHandler validHandler,
                                          final Predicate<MessageEnqueueRecord> predicate,
                                          final Consumer<MessageEnqueueRecord> invalidCollector) throws StoreException
        {
            checkMessageStoreOpen();

            final UUID queueId = queue.getId();

            // Callbacks are invoked while iterating a BDB cursor; BDB resources/locks may be held.
            // Callbacks must be fast and must not re-enter the store (including creating transactions, reading
            // messages, or performing other database operations). Only validHandler controls early termination.
            try (final Cursor cursor = getDeliveryDb().openCursor(null, null))
            {
                final DatabaseEntry key = new DatabaseEntry(new byte[QueueEntryBinding.KEY_SIZE]);

                QueueEntryBinding.objectToEntry(queueId, 0L, key);

                if (cursor.getSearchKeyRange(key, null, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    do
                    {
                        if (!QueueEntryBinding.matchesQueueId(key, queueId))
                        {
                            break;
                        }

                        final long messageId = QueueEntryBinding.readMessageId(key);
                        final MessageEnqueueRecord record = new BDBEnqueueRecord(queueId, messageId);

                        if (predicate.test(record))
                        {
                            if (!validHandler.handle(record))
                            {
                                break;
                            }
                        }
                        else
                        {
                            invalidCollector.accept(record);
                        }
                    }
                    while (cursor.getNext(key, null, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
                }
            }
            catch (final RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot visit message instances", e);
            }
        }

        @Override
        public void visitMessageInstances(final MessageInstanceHandler handler) throws StoreException
        {
            checkMessageStoreOpen();

            final CachingUUIDFactory uuidFactory = new CachingUUIDFactory();
            final List<BDBEnqueueRecord> entries = new ArrayList<>();
            try (final Cursor cursor = getDeliveryDb().openCursor(null, null))
            {
                final DatabaseEntry key = new DatabaseEntry(new byte[QueueEntryBinding.KEY_SIZE]);

                while (cursor.getNext(key, null, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    entries.add(QueueEntryBinding.readIds(uuidFactory, key));
                }
            }
            catch (final RuntimeException e)
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
        public void visitDistributedTransactions(final DistributedTransactionHandler handler) throws StoreException
        {
            checkMessageStoreOpen();

            try (final Cursor cursor = getXidDb().openCursor(null, null))
            {
                final CachingUUIDFactory uuidFactory = new CachingUUIDFactory();
                final DatabaseEntry key = new DatabaseEntry();
                final XidBinding keyBinding = XidBinding.getInstance();
                final DatabaseEntry value = new DatabaseEntry();

                while (cursor.getNext(key, value, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)
                {
                    final Xid xid = keyBinding.entryToObject(key);
                    final PreparedTransaction preparedTransaction = PreparedTransactionBinding.entryToObject(uuidFactory, value);
                    final BDBStoredXidRecord record = new BDBStoredXidRecord(xid.getFormat(), xid.getGlobalId(), xid.getBranchId());
                    if (!handler.handle(record, preparedTransaction.enqueues(), preparedTransaction.dequeues()))
                    {
                        break;
                    }
                }
            }
            catch (final RuntimeException e)
            {
                throw getEnvironmentFacade().handleDatabaseException("Cannot recover distributed transactions", e);
            }
        }
    }
}
