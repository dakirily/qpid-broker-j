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

package org.apache.qpid.systests.jms_3_1.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.jms.JMSContext;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on JMSContext stop/start semantics.
 *
 * The tests in this class are aligned with:
 * - 6.1.5 "Pausing delivery of incoming messages"
 * - 2.8   "Simplified API interfaces"
 */
@JmsSystemTest
@Tag("queue")
@Tag("connection")
@Tag("simplified-api")
class JMSContextStopTest
{
    /**
     * Verifies that JMSContext.stop() suppresses delivery until start() is called,
     * matching the connection pause semantics described in 6.1.5.
     */
    @Test
    void stopSuppressesDeliveryUntilStart(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final JMSContext consumerCtx = jms.builder().connection().connectionFactory()
                .createContext(JMSContext.AUTO_ACKNOWLEDGE);
             final JMSContext producerCtx = jms.builder().connection().connectionFactory()
                     .createContext(JMSContext.AUTO_ACKNOWLEDGE))
        {
            final var consumer = consumerCtx.createConsumer(queue);
            consumerCtx.stop();

            producerCtx.createProducer().send(queue, "payload");

            assertNull(consumer.receiveBody(String.class, Timeouts.noMessagesMillis()),
                    "No message should be delivered while JMSContext is stopped");

            consumerCtx.start();
            assertEquals("payload", consumer.receiveBody(String.class, Timeouts.receiveMillis()),
                    "Message should be delivered after JMSContext.start()");
        }
    }
}
