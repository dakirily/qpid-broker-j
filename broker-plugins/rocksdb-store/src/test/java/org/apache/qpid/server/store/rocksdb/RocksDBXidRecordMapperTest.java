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

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.store.Transaction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RocksDBXidRecordMapperTest
{
    @Test
    void encodeDecodeXidValueRoundTrip()
    {
        UUID queueId = UUID.randomUUID();
        RecordImpl enqueue = new RecordImpl(queueId, 1L);
        RecordImpl dequeue = new RecordImpl(queueId, 2L);

        byte[] globalId = new byte[]{1, 2, 3};
        byte[] branchId = new byte[]{4, 5};
        byte[] encoded = RocksDBXidRecordMapper.encodeXidValue(7L,
                                                               globalId,
                                                               branchId,
                                                               new Transaction.EnqueueRecord[]{enqueue},
                                                               new Transaction.DequeueRecord[]{dequeue});

        XidRecordData decoded = RocksDBXidRecordMapper.decodeXidValue(encoded,
                                                                      new RocksDBXidRecordMapper.RecordFactory()
                                                                      {
                                                                          @Override
                                                                          public Transaction.EnqueueRecord createEnqueue(final UUID queueId,
                                                                                                                          final long messageId)
                                                                          {
                                                                              return new RecordImpl(queueId, messageId);
                                                                          }

                                                                          @Override
                                                                          public Transaction.DequeueRecord createDequeue(final UUID queueId,
                                                                                                                          final long messageId)
                                                                          {
                                                                              return new RecordImpl(queueId, messageId);
                                                                          }
                                                                      });

        assertNotNull(decoded);
        assertEquals(7L, decoded.getXid().getFormat());
        assertArrayEquals(globalId, decoded.getXid().getGlobalId());
        assertArrayEquals(branchId, decoded.getXid().getBranchId());

        Transaction.EnqueueRecord decodedEnqueue = decoded.getActions().getEnqueues().get(0);
        Transaction.DequeueRecord decodedDequeue = decoded.getActions().getDequeues().get(0);

        assertEquals(queueId, decodedEnqueue.getResource().getId());
        assertEquals(1L, decodedEnqueue.getMessage().getMessageNumber());
        assertEquals(queueId, decodedDequeue.getEnqueueRecord().getQueueId());
        assertEquals(2L, decodedDequeue.getEnqueueRecord().getMessageNumber());
    }
}
