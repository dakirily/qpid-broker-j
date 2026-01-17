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
package org.apache.qpid.server.virtualhost;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.messages.MessageStoreMessages;
import org.apache.qpid.server.logging.messages.TransactionLogMessages;
import org.apache.qpid.server.logging.subjects.MessageStoreLogSubject;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.Transaction.EnqueueRecord;
import org.apache.qpid.server.store.handler.DistributedTransactionHandler;
import org.apache.qpid.server.store.handler.MessageHandler;
import org.apache.qpid.server.store.handler.MessageInstanceHandler;
import org.apache.qpid.server.transport.util.Functions;
import org.apache.qpid.server.txn.DtxBranch;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.txn.Xid;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class SynchronousMessageStoreRecoverer implements MessageStoreRecoverer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronousMessageStoreRecoverer.class);

    /** Fixed-size batch used to remove invalid instances during recovery without creating a transaction per record */
    private static final int INVALID_INSTANCE_DEQUEUE_BATCH_SIZE = 1000;

    @Override
    public CompletableFuture<Void> recover(QueueManagingVirtualHost<?> virtualHost)
    {
        EventLogger eventLogger = virtualHost.getEventLogger();
        MessageStore store = virtualHost.getMessageStore();
        MessageStore.MessageStoreReader storeReader = store.newMessageStoreReader();
        MessageStoreLogSubject logSubject = new MessageStoreLogSubject(virtualHost.getName(), store.getClass().getSimpleName());

        Map<Queue<?>, Integer> queueRecoveries = new TreeMap<>();
        Map<Long, ServerMessage<?>> recoveredMessages = new HashMap<>();
        Map<Long, StoredMessage<?>> unusedMessages = new TreeMap<>();
        Map<UUID, Integer> unknownQueuesWithMessages = new HashMap<>();
        Map<Queue<?>, Integer> queuesWithUnknownMessages = new HashMap<>();

        eventLogger.message(logSubject, MessageStoreMessages.RECOVERY_START());

        storeReader.visitMessages(new MessageVisitor(recoveredMessages, unusedMessages));

        eventLogger.message(logSubject, TransactionLogMessages.RECOVERY_START(null, false));
        // Some store implementations iterate using cursors/locks and cannot tolerate mutations during the visit.
        // Collect invalid message instances first and dequeue them after the visit completes.
        final List<MessageEnqueueRecord> invalidMessageInstances = new ArrayList<>();
        try
        {

            final MessageInstanceVisitor validHandler = new MessageInstanceVisitor(virtualHost,
                                                                                 queueRecoveries,
                                                                                 recoveredMessages,
                                                                                 unusedMessages);
            final Predicate<MessageEnqueueRecord> filter = new MessageInstanceFilter(virtualHost, recoveredMessages);
            final Consumer<MessageEnqueueRecord> invalidCollector = new InvalidMessageInstanceCollector(virtualHost,
                                                                                                      invalidMessageInstances,
                                                                                                      unknownQueuesWithMessages,
                                                                                                      queuesWithUnknownMessages);

            storeReader.visitMessageInstances(validHandler, filter, invalidCollector);
        }
        finally
        {
            // Dequeue after visitation to avoid cursor/lock reentrancy issues in the underlying store.
            dequeueInvalidMessageInstances(store, invalidMessageInstances);


            if (!unknownQueuesWithMessages.isEmpty())
            {
                unknownQueuesWithMessages.forEach((queueId, count) ->
                        LOGGER.info("Discarded {} entry(s) associated with queue id '{}' as a queue with this "
                             + "id does not appear in the configuration.", count, queueId));
            }
            if (!queuesWithUnknownMessages.isEmpty())
            {
                queuesWithUnknownMessages.forEach((queue, count) ->
                        LOGGER.info("Discarded {} entry(s) associated with queue '{}' as the referenced message "
                             + "does not exist.", count, queue.getName()));
            }
        }

        for(Map.Entry<Queue<?>, Integer> entry : queueRecoveries.entrySet())
        {
            Queue<?> queue = entry.getKey();
            Integer deliveredCount = entry.getValue();
            eventLogger.message(logSubject, TransactionLogMessages.RECOVERED(deliveredCount, queue.getName()));
            eventLogger.message(logSubject, TransactionLogMessages.RECOVERY_COMPLETE(queue.getName(), true));
            queue.completeRecovery();
        }

        for (Queue<?> q : virtualHost.getChildren(Queue.class))
        {
            if (!queueRecoveries.containsKey(q))
            {
                q.completeRecovery();
            }
        }

        storeReader.visitDistributedTransactions(new DistributedTransactionVisitor(virtualHost,
                                                                                   eventLogger,
                                                                                   logSubject, recoveredMessages, unusedMessages));

        for(StoredMessage<?> m : unusedMessages.values())
        {
            LOGGER.debug("Message id '{}' is orphaned, removing", m.getMessageNumber());
            m.remove();
        }

        if (unusedMessages.size() > 0)
        {
            LOGGER.info("Discarded {} orphaned message(s).", unusedMessages.size());
        }

        eventLogger.message(logSubject, TransactionLogMessages.RECOVERY_COMPLETE(null, false));

        eventLogger.message(logSubject,
                             MessageStoreMessages.RECOVERED(recoveredMessages.size() - unusedMessages.size()));
        eventLogger.message(logSubject, MessageStoreMessages.RECOVERY_COMPLETE());

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void cancel()
    {
        // No-op
    }

    private static class MessageVisitor implements MessageHandler
    {

        private final Map<Long, ServerMessage<?>> _recoveredMessages;
        private final Map<Long, StoredMessage<?>> _unusedMessages;

        MessageVisitor(final Map<Long, ServerMessage<?>> recoveredMessages,
                       final Map<Long, StoredMessage<?>> unusedMessages)
        {
            _recoveredMessages = recoveredMessages;
            _unusedMessages = unusedMessages;
        }

        @Override
        public boolean handle(StoredMessage<?> message)
        {
            StorableMessageMetaData metaData = message.getMetaData();

            @SuppressWarnings("rawtypes")
            MessageMetaDataType type = metaData.getType();

            @SuppressWarnings("unchecked")
            ServerMessage<?> serverMessage = type.createMessage(message);

            _recoveredMessages.put(message.getMessageNumber(), serverMessage);
            _unusedMessages.put(message.getMessageNumber(), message);
            return true;
        }

    }

    private static void dequeueInvalidMessageInstances(final MessageStore store,
                                                       final List<MessageEnqueueRecord> invalidRecords)
    {
        // Batch removals to avoid a transaction per record while keeping transactions bounded in size.
        if (invalidRecords.isEmpty())
        {
            return;
        }

        Transaction txn = store.newTransaction();
        int inTxn = 0;

        for (int i = 0; i < invalidRecords.size(); i++)
        {
            final MessageEnqueueRecord record = invalidRecords.get(i);
            txn.dequeueMessage(record);
            inTxn++;

            if (inTxn == INVALID_INSTANCE_DEQUEUE_BATCH_SIZE && i + 1 < invalidRecords.size())
            {
                txn.commitTranAsync((Void) null);
                txn = store.newTransaction();
                inTxn = 0;
            }
        }

        if (inTxn > 0)
        {
            txn.commitTranAsync((Void) null);
        }
    }

    private static class MessageInstanceFilter implements Predicate<MessageEnqueueRecord>
    {
        private final QueueManagingVirtualHost<?> _virtualHost;
        private final Map<Long, ServerMessage<?>> _recoveredMessages;

        private MessageInstanceFilter(final QueueManagingVirtualHost<?> virtualHost,
                                      final Map<Long, ServerMessage<?>> recoveredMessages)
        {
            _virtualHost = virtualHost;
            _recoveredMessages = recoveredMessages;
        }

        @Override
        public boolean test(final MessageEnqueueRecord record)
        {
            return _virtualHost.getAttainedQueue(record.getQueueId()) != null
                   && _recoveredMessages.containsKey(record.getMessageNumber());
        }
    }

    private static class InvalidMessageInstanceCollector implements Consumer<MessageEnqueueRecord>
    {
        private final QueueManagingVirtualHost<?> _virtualHost;
        private final List<MessageEnqueueRecord> _invalidRecords;
        private final Map<UUID, Integer> _unknownQueuesWithMessages;
        private final Map<Queue<?>, Integer> _queuesWithUnknownMessages;

        private InvalidMessageInstanceCollector(final QueueManagingVirtualHost<?> virtualHost,
                                                final List<MessageEnqueueRecord> invalidRecords,
                                                final Map<UUID, Integer> unknownQueuesWithMessages,
                                                final Map<Queue<?>, Integer> queuesWithUnknownMessages)
        {
            _virtualHost = virtualHost;
            _invalidRecords = invalidRecords;
            _unknownQueuesWithMessages = unknownQueuesWithMessages;
            _queuesWithUnknownMessages = queuesWithUnknownMessages;
        }

        @Override
        public void accept(final MessageEnqueueRecord record)
        {
            final UUID queueId = record.getQueueId();
            final long messageId = record.getMessageNumber();
            final Queue<?> queue = _virtualHost.getAttainedQueue(queueId);
            if (queue != null)
            {
                LOGGER.debug(
                        "Message id '{}' referenced in log as enqueued in queue '{}' is unknown, entry will be discarded",
                        messageId, queue.getName());

                _queuesWithUnknownMessages.merge(queue, 1, (old, unused) -> old + 1);
            }
            else
            {
                LOGGER.debug(
                        "Message id '{}' in log references queue with id '{}' which is not in the configuration, entry will be discarded",
                        messageId, queueId);
                _unknownQueuesWithMessages.merge(queueId, 1, (old, unused) -> old + 1);
            }

            _invalidRecords.add(record);
        }
    }

    private static class MessageInstanceVisitor implements MessageInstanceHandler
    {
        private final QueueManagingVirtualHost<?> _virtualHost;

        private final Map<Queue<?>, Integer> _queueRecoveries;
        private final Map<Long, ServerMessage<?>> _recoveredMessages;
        private final Map<Long, StoredMessage<?>> _unusedMessages;

        private MessageInstanceVisitor(final QueueManagingVirtualHost<?> virtualHost,
                                       final Map<Queue<?>, Integer> queueRecoveries,
                                       final Map<Long, ServerMessage<?>> recoveredMessages,
                                       final Map<Long, StoredMessage<?>> unusedMessages)
        {
            _virtualHost = virtualHost;
            _queueRecoveries = queueRecoveries;
            _recoveredMessages = recoveredMessages;
            _unusedMessages = unusedMessages;
        }

        @Override
        public boolean handle(final MessageEnqueueRecord record)
        {
            final UUID queueId = record.getQueueId();
            final long messageId = record.getMessageNumber();
            final Queue<?> queue = _virtualHost.getAttainedQueue(queueId);
            if (queue == null)
            {
                // The filter is expected to prevent unknown queues reaching this handler.
                return true;
            }

            final ServerMessage<?> message = _recoveredMessages.get(messageId);
            _unusedMessages.remove(messageId);

            if (message == null)
            {
                // The filter is expected to prevent unknown messages reaching this handler.
                return true;
            }

            LOGGER.debug("Delivering message id '{}' to queue '{}'", message.getMessageNumber(), queue.getName());

            _queueRecoveries.merge(queue, 1, (old, unused) -> old + 1);

            queue.recover(message, record);

            return true;
        }
    }

    private static class DistributedTransactionVisitor implements DistributedTransactionHandler
    {

        private final QueueManagingVirtualHost<?> _virtualHost;
        private final EventLogger _eventLogger;
        private final MessageStoreLogSubject _logSubject;

        private final Map<Long, ServerMessage<?>> _recoveredMessages;
        private final Map<Long, StoredMessage<?>> _unusedMessages;

        private DistributedTransactionVisitor(final QueueManagingVirtualHost<?> virtualHost,
                                              final EventLogger eventLogger,
                                              final MessageStoreLogSubject logSubject,
                                              final Map<Long, ServerMessage<?>> recoveredMessages,
                                              final Map<Long, StoredMessage<?>> unusedMessages)
        {
            _virtualHost = virtualHost;
            _eventLogger = eventLogger;
            _logSubject = logSubject;
            _recoveredMessages = recoveredMessages;
            _unusedMessages = unusedMessages;
        }

        @Override
        public boolean handle(final Transaction.StoredXidRecord storedXid,
                              final Transaction.EnqueueRecord[] enqueues,
                              final Transaction.DequeueRecord[] dequeues)
        {
            Xid id = new Xid(storedXid.getFormat(), storedXid.getGlobalId(), storedXid.getBranchId());
            DtxRegistry dtxRegistry = _virtualHost.getDtxRegistry();
            DtxBranch branch = dtxRegistry.getBranch(id);
            if(branch == null)
            {
                branch = new DtxBranch(storedXid, dtxRegistry);
                dtxRegistry.registerBranch(branch);
            }
            for(EnqueueRecord record : enqueues)
            {
                final Queue<?> queue = _virtualHost.getAttainedQueue(record.getResource().getId());
                if(queue != null)
                {
                    final long messageId = record.getMessage().getMessageNumber();
                    final ServerMessage<?> message = _recoveredMessages.get(messageId);
                    _unusedMessages.remove(messageId);

                    if(message != null)
                    {
                        final MessageReference<?> ref = message.newReference();
                        final MessageEnqueueRecord[] records = new MessageEnqueueRecord[1];

                        branch.enqueue(queue, message, record1 -> records[0] = record1);
                        branch.addPostTransactionAction(new ServerTransaction.Action()
                        {
                            @Override
                            public void postCommit()
                            {
                                queue.enqueue(message, null, records[0]);
                                ref.release();
                            }

                            @Override
                            public void onRollback()
                            {
                                ref.release();
                            }
                        });

                    }
                    else
                    {
                        StringBuilder xidString = xidAsString(id);
                        _eventLogger.message(_logSubject,
                                          TransactionLogMessages.XA_INCOMPLETE_MESSAGE(xidString.toString(),
                                                                                       Long.toString(messageId)));
                    }
                }
                else
                {
                    StringBuilder xidString = xidAsString(id);
                    _eventLogger.message(_logSubject,
                                      TransactionLogMessages.XA_INCOMPLETE_QUEUE(xidString.toString(),
                                                                                 record.getResource().getId().toString()));

                }
            }
            for(Transaction.DequeueRecord record : dequeues)
            {
                final Queue<?> queue = _virtualHost.getAttainedQueue(record.getEnqueueRecord().getQueueId());
                if(queue != null)
                {
                    final long messageId = record.getEnqueueRecord().getMessageNumber();
                    final ServerMessage<?> message = _recoveredMessages.get(messageId);
                    _unusedMessages.remove(messageId);

                    if(message != null)
                    {
                        final QueueEntry entry = queue.getMessageOnTheQueue(messageId);

                        if (entry.acquire())
                        {
                            branch.dequeue(entry.getEnqueueRecord());

                            branch.addPostTransactionAction(new ServerTransaction.Action()
                            {

                                @Override
                                public void postCommit()
                                {
                                    entry.delete();
                                }

                                @Override
                                public void onRollback()
                                {
                                    entry.release();
                                }
                            });
                        }
                        else
                        {
                            // Should never happen - dtx recovery is always synchronous and occurs before
                            // any other message actors are allowed to act on the virtualhost.
                            throw new ServerScopedRuntimeException(
                                    "Distributed transaction dequeue handler failed to acquire " + entry +
                                    " during recovery of queue " + queue);
                        }

                    }
                    else
                    {
                        StringBuilder xidString = xidAsString(id);
                        _eventLogger.message(_logSubject,
                                          TransactionLogMessages.XA_INCOMPLETE_MESSAGE(xidString.toString(),
                                                                                       Long.toString(messageId)));

                    }

                }
                else
                {
                    StringBuilder xidString = xidAsString(id);
                    _eventLogger.message(_logSubject,
                                      TransactionLogMessages.XA_INCOMPLETE_QUEUE(xidString.toString(),
                                                                                 record.getEnqueueRecord().getQueueId().toString()));
                }

            }

            branch.setState(DtxBranch.State.PREPARED);
            branch.prePrepareTransaction();
            return true;
        }

        private StringBuilder xidAsString(Xid id)
        {
            return new StringBuilder("(")
                        .append(id.getFormat())
                        .append(',')
                        .append(Functions.str(id.getGlobalId()))
                        .append(',')
                        .append(Functions.str(id.getBranchId()))
                        .append(')');
        }


    }


}
