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

import java.util.UUID;

import org.apache.qpid.server.message.EnqueueableMessage;
import org.apache.qpid.server.store.MessageDurability;
import org.apache.qpid.server.store.MessageEnqueueRecord;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogResource;

/**
 * Represents a transaction record used for distributed transactions.
 * <br>
 * Thread-safety: immutable.
 */
final class RecordImpl implements Transaction.EnqueueRecord,
                                  Transaction.DequeueRecord,
                                  TransactionLogResource,
                                  EnqueueableMessage
{
    private final RocksDBEnqueueRecord _record;
    private final long _messageNumber;
    private final UUID _queueId;

    RecordImpl(final UUID queueId, final long messageNumber)
    {
        _queueId = queueId;
        _messageNumber = messageNumber;
        _record = new RocksDBEnqueueRecord(queueId, messageNumber);
    }

    @Override
    public MessageEnqueueRecord getEnqueueRecord()
    {
        return _record;
    }

    @Override
    public TransactionLogResource getResource()
    {
        return this;
    }

    @Override
    public EnqueueableMessage getMessage()
    {
        return this;
    }

    @Override
    public long getMessageNumber()
    {
        return _messageNumber;
    }

    @Override
    public boolean isPersistent()
    {
        return true;
    }

    @Override
    public StoredMessage getStoredMessage()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName()
    {
        return _queueId.toString();
    }

    @Override
    public UUID getId()
    {
        return _queueId;
    }

    @Override
    public MessageDurability getMessageDurability()
    {
        return MessageDurability.DEFAULT;
    }
}
