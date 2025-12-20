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

import static org.apache.qpid.systests.EntityTypes.DIRECT_EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.STANDARD_QUEUE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import jakarta.jms.Session;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.Binding;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("routing")
class ExchangeRoutingTest
{
    @Test
    void exchangeToQueueRouting(final JmsSupport jms) throws Exception
    {
        final String queueName = jms.testMethodName() + "Queue";
        final String exchangeName = jms.testMethodName() + "Exchange";
        final String routingKey = "key";

        jms.management().create(queueName, STANDARD_QUEUE, Map.of());
        jms.management().create(exchangeName, DIRECT_EXCHANGE, Map.of());

        final Map<String, Object> bindingArguments = new HashMap<>();
        bindingArguments.put("destination", queueName);
        bindingArguments.put("bindingKey", routingKey);

        jms.management().perform(exchangeName, "bind", EXCHANGE, bindingArguments);

        routeTest(jms, exchangeName, queueName, "unboundKey", 0, 0);
        routeTest(jms, exchangeName, queueName, routingKey, 0, 1);
    }

    @Test
    void exchangeToExchangeToQueueRouting(final JmsSupport jms) throws Exception
    {
        final String queueName = jms.testMethodName() + "Queue";
        final String exchangeName1 = jms.testMethodName() + "Exchange1";
        final String exchangeName2 = jms.testMethodName() + "Exchange2";
        final String bindingKey = "key";

        jms.management().create(queueName, STANDARD_QUEUE, Map.of());
        jms.management().create(exchangeName1, DIRECT_EXCHANGE, Map.of());
        jms.management().create(exchangeName2, DIRECT_EXCHANGE, Map.of());

        final Map<String, Object> binding1Arguments = new HashMap<>();
        binding1Arguments.put("destination", exchangeName2);
        binding1Arguments.put("bindingKey", bindingKey);

        jms.management().perform(exchangeName1, "bind", EXCHANGE, binding1Arguments);

        final Map<String, Object> binding2Arguments = new HashMap<>();
        binding2Arguments.put("destination", queueName);
        binding2Arguments.put("bindingKey", bindingKey);

        jms.management().perform(exchangeName2, "bind", EXCHANGE, binding2Arguments);

        routeTest(jms, exchangeName1, queueName, bindingKey, 0, 1);
    }

    @Test
    void exchangeToExchangeToQueueRoutingWithReplacementRoutingKey(final JmsSupport jms) throws Exception
    {
        final String queueName = jms.testMethodName() + "Queue";
        final String exchangeName1 = jms.testMethodName() + "Exchange1";
        final String exchangeName2 = jms.testMethodName() + "Exchange2";
        final String bindingKey1 = "key1";
        final String bindingKey2 = "key2";

        jms.management().create(queueName, STANDARD_QUEUE, Map.of());
        jms.management().create(exchangeName1, DIRECT_EXCHANGE, Map.of());
        jms.management().create(exchangeName2, DIRECT_EXCHANGE, Map.of());

        final Map<String, Object> binding1Arguments = new HashMap<>();
        binding1Arguments.put("destination", exchangeName2);
        binding1Arguments.put("bindingKey", bindingKey1);
        binding1Arguments.put("arguments",
                              new ObjectMapper().writeValueAsString(Map.of(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY,
                                                                                             bindingKey2)));

        jms.management().perform(exchangeName1, "bind", EXCHANGE, binding1Arguments);

        final Map<String, Object> binding2Arguments = new HashMap<>();
        binding2Arguments.put("destination", queueName);
        binding2Arguments.put("bindingKey", bindingKey2);

        jms.management().perform(exchangeName2,"bind", EXCHANGE, binding2Arguments);

        routeTest(jms, exchangeName1, queueName, bindingKey1, 0, 1);
    }

    private void routeTest(final JmsSupport jms,
                           final String fromExchangeName,
                           final String queueName,
                           final String routingKey,
                           final int expectedDepthBefore,
                           final int expectedDepthAfter) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var ingressExchangeDestination = session.createQueue(getDestinationAddress(fromExchangeName, routingKey));

            assertEquals(expectedDepthBefore, jms.queue(queueName).depthBytes(),
                    "Unexpected number of messages on queue '%s'".formatted(queueName));

            jms.messages().send(connection, ingressExchangeDestination, 1);

            assertEquals(expectedDepthAfter, jms.queue(queueName).depthMessages(),
                    "Unexpected number of messages on queue '%s".formatted(queueName));
        }
    }

    private String getDestinationAddress(final String exchangeName, final String routingKey)
    {
        return "%s/%s".formatted(exchangeName, routingKey);
    }
}
