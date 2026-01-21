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

import org.apache.qpid.server.store.MessageEnqueueRecord;

/**
 * Represents a queue entry record.
 * <br>
 * Thread-safety: immutable.
 */
final class RocksDBEnqueueRecord implements MessageEnqueueRecord
{
    private final UUID _queueId;
    private final long _messageNumber;

    RocksDBEnqueueRecord(final UUID queueId, final long messageNumber)
    {
        _queueId = queueId;
        _messageNumber = messageNumber;
    }

    @Override
    public UUID getQueueId()
    {
        return _queueId;
    }

    @Override
    public long getMessageNumber()
    {
        return _messageNumber;
    }
}
