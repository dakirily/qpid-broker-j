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

package org.apache.qpid.systests.jms_3_1.extensions.autocreation;

import static org.apache.qpid.systests.EntityTypes.VIRTUALHOST;
import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.jms.Session;

import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.server.virtualhost.NodeAutoCreationPolicy;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

@JmsSystemTest
@Tag("routing")
@Tag("policy")
class DefaultAlternateBindingTest
{
    // quadruple of '$' is used intentionally instead of '$$' to account conversion in management layer
    private static final String DEFAULT_ALTERNATE_BINDING = "{\"destination\": \"$$$${this:name}_DLQ\"}";
    private static final int MAXIMUM_DELIVERY_ATTEMPTS = 1;
    private static final int MESSAGE_COUNT = 4;

    private final static ObjectMapper OBJECT_MAPPER = ConfiguredObjectJacksonModule.newObjectMapper(false);

    private String _queueName;

    @BeforeEach
    void setUp(final JmsSupport jms) throws Exception
    {
        _queueName = jms.testMethodName();
        updateVirtualHostForDefaultAlternateBinding(jms);
    }

    @Test
    void defaultAlternateBinding(final JmsSupport jms) throws Exception
    {
        jms.builder().queue().maximumDeliveryAttempts(MAXIMUM_DELIVERY_ATTEMPTS).bind().create();

        try (final var connection = jms.builder().connection().syncPublish(true).prefetch(0).create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED))
        {
            final var queue = session.createQueue(_queueName);
            jms.messages().send(session, queue, MESSAGE_COUNT);

            connection.start();
            final var queueConsumer = session.createConsumer(queue);

            for (int i = 0; i < MESSAGE_COUNT; i++)
            {
                final var message = queueConsumer.receive(Timeouts.receiveMillis());
                assertThat(message, is(notNullValue()));
                int index = message.getIntProperty(INDEX);
                if (index % 2 == 0)
                {
                    session.commit();
                }
                else
                {
                    session.rollback();
                }
            }

            final var dlq = session.createQueue(_queueName + "_DLQ");
            final var dlqConsumer = session.createConsumer(dlq);

            final var message2 = dlqConsumer.receive(Timeouts.receiveMillis());
            assertThat(message2, is(notNullValue()));
            assertThat(message2.getIntProperty(INDEX), is(equalTo(1)));

            final var message4 = dlqConsumer.receive(Timeouts.receiveMillis());
            assertThat(message4, is(notNullValue()));
            assertThat(message4.getIntProperty(INDEX), is(equalTo(3)));
        }
    }

    private void updateVirtualHostForDefaultAlternateBinding(final JmsSupport jms) throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(QueueManagingVirtualHost.CONTEXT,
                       objectToJsonString(Map.of("queue.defaultAlternateBinding", DEFAULT_ALTERNATE_BINDING)));
        attributes.put(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES, createAutoCreationPolicies());
        jms.management().update(jms.virtualHostName(), VIRTUALHOST, attributes);
    }

    private String createAutoCreationPolicies() throws JacksonException
    {
        final List<NodeAutoCreationPolicy> objects = List.of(new NodeAutoCreationPolicy()
        {
            @Override
            public String getPattern()
            {
                return ".*_DLQ";
            }

            @Override
            public boolean isCreatedOnPublish()
            {
                return true;
            }

            @Override
            public boolean isCreatedOnConsume()
            {
                return false;
            }

            @Override
            public String getNodeType()
            {
                return "Queue";
            }

            @Override
            public Map<String, Object> getAttributes()
            {
                return Map.of("alternateBinding", "");
            }
        });
        return objectToJsonString(objects);
    }

    private String objectToJsonString(final Object objects) throws JacksonException
    {
        return OBJECT_MAPPER.writeValueAsString(objects);
    }
}
