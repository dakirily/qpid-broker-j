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

package org.apache.qpid.systests.support;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import org.apache.qpid.systests.AmqpManagementFacade;
import org.apache.qpid.systests.builder.JmsBuilder;
import org.apache.qpid.systests.JmsProvider;
import org.apache.qpid.tests.utils.BrokerAdmin;

public record JmsSupport(AmqpManagementFacade amqpManagementFacade,
                         BrokerAdmin brokerAdmin,
                         JmsProvider jmsProvider,
                         String testClassName,
                         String testMethodName)
{

    public AmqpManagementSupport management()
    {
        return new AmqpManagementSupport(this, amqpManagementFacade());
    }

    public JmsBuilder builder()
    {
        return new JmsBuilder(this);
    }

    public AmqpManagementSupport management(final Connection connection)
    {
        return new AmqpManagementSupport(this, amqpManagementFacade(), connection);
    }

    public MessagesSupport messages()
    {
        return new MessagesSupport();
    }

    public QueueSupport queue()
    {
        return new QueueSupport(this, testMethodName());
    }

    public QueueSupport queue(final Queue queue) throws JMSException
    {
        return new QueueSupport(this, queue.getQueueName());
    }

    public QueueSupport queue(final String queueName)
    {
        return new QueueSupport(this, queueName);
    }

    public VirtualhostSupport virtualhost()
    {
        return new VirtualhostSupport(this, fullTestName());
    }

    public String fullTestName()
    {
        return "%s_%s".formatted(testClassName, testMethodName);
    }

    public String virtualHostName()
    {
        return fullTestName();
    }
}
