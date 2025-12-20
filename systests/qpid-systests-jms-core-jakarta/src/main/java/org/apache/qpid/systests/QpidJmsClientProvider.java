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
 *
 */

package org.apache.qpid.systests;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import org.apache.qpid.systests.builder.ConnectionBuilder;
import org.apache.qpid.systests.builder.QpidJmsClientConnectionBuilder;

public class QpidJmsClientProvider implements JmsProvider
{
    private final AmqpManagementFacade _managementFacade;

    public QpidJmsClientProvider(AmqpManagementFacade managementFacade)
    {
        _managementFacade = managementFacade;
    }

    @Override
    public Queue createQueue(Session session, String queueName) throws JMSException
    {
        _managementFacade.createEntityUsingAmqpManagement(queueName, session, "org.apache.qpid.Queue");
        return session.createQueue(queueName);
    }

    @Override
    public Topic createTopic(final Connection con, final String topicName) throws JMSException
    {
        try (final var session = con.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            _managementFacade.createEntityUsingAmqpManagement(topicName, session, "org.apache.qpid.TopicExchange");
            return session.createTopic(topicName);
        }
    }

    @Override
    public ConnectionBuilder getConnectionBuilder()
    {
        return new QpidJmsClientConnectionBuilder();
    }
}
