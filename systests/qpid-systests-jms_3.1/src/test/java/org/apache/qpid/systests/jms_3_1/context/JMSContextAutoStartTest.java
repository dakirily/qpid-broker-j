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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("connection")
@Tag("queue")
@Tag("simplified-api")
public class JMSContextAutoStartTest
{
    @Test
    void autoStartFalseSuppressesImplicitStartUntilStartCalled(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final ConnectionFactory cf1 = jms.builder().connection().connectionFactory();
        final ConnectionFactory cf2 = jms.builder().connection().connectionFactory();

        try (final JMSContext consumerCtx = cf1.createContext(JMSContext.AUTO_ACKNOWLEDGE);
             final JMSContext producerCtx = cf2.createContext(JMSContext.AUTO_ACKNOWLEDGE))
        {
            consumerCtx.setAutoStart(false);

            final CountDownLatch delivered = new CountDownLatch(1);
            consumerCtx.createConsumer(queue).setMessageListener(m -> delivered.countDown());

            producerCtx.createProducer().send(queue, "payload");

            // consumer must not receive the messages before start() is called
            assertFalse(delivered.await(Timeouts.noMessagesMillis(), TimeUnit.MILLISECONDS),
                    "Message should not be delivered before JMSContext.start() when autoStart=false");

            consumerCtx.start();

            assertTrue(delivered.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "Message should be delivered after JMSContext.start()");
        }
    }

    @Test
    void defaultAutoStartDeliversWithoutExplicitStart(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final ConnectionFactory cf = jms.builder().connection().connectionFactory();

        try (final JMSContext ctx = cf.createContext(JMSContext.AUTO_ACKNOWLEDGE))
        {
            final CountDownLatch delivered = new CountDownLatch(1);
            ctx.createConsumer(queue).setMessageListener(m -> delivered.countDown());

            ctx.createProducer().send(queue, "payload");

            assertTrue(delivered.await(Timeouts.receiveMillis(), TimeUnit.MILLISECONDS),
                    "Message should be delivered without explicit start() when autoStart=true (default)");
        }
    }
}
