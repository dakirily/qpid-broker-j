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

package org.apache.qpid.systests.jms_3_1.extensions.routing;

import static org.apache.qpid.systests.EntityTypes.EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.FANOUT_EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.QUEUE;
import static org.apache.qpid.systests.EntityTypes.STANDARD_QUEUE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import jakarta.jms.Session;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("routing")
class AlternateBindingRoutingTest
{
    @Test
    void fanoutExchangeAsAlternateBinding(final JmsSupport jms) throws Exception
    {
        final String queueName = jms.testMethodName();
        final String deadLetterQueueName = queueName + "_DeadLetter";
        final String deadLetterExchangeName = "deadLetterExchange";

        jms.management().create(deadLetterQueueName, STANDARD_QUEUE, Collections.emptyMap());
        jms.management().create(deadLetterExchangeName, FANOUT_EXCHANGE, Collections.emptyMap());

        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", deadLetterQueueName);
        arguments.put("bindingKey", queueName);
        jms.management().perform(deadLetterExchangeName, "bind", EXCHANGE, arguments);

        final var testQueue = jms.builder().queue().alternateBinding(deadLetterExchangeName).create();

        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);

            jms.messages().send(session, testQueue, 1);

            assertEquals(1, jms.queue(testQueue).depthMessages(), "Unexpected number of messages on queue");
            assertEquals(0, jms.queue(deadLetterQueueName).depthMessages(), "Unexpected number of messages on DLQ");

            jms.management().perform(queueName, "DELETE", QUEUE, Map.of());

            assertEquals(1, jms.queue(deadLetterQueueName).depthMessages(), "Unexpected number of messages on DLQ after deletion");
        }
    }
}
