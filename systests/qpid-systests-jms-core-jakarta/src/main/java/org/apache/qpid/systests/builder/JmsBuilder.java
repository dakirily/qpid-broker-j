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

package org.apache.qpid.systests.builder;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class JmsBuilder
{
    private final JmsSupport _jmsSupport;

    public JmsBuilder(final JmsSupport jmsSupport)
    {
        _jmsSupport = jmsSupport;
    }

    public ConnectionBuilder connection()
    {
        final var brokerAdmin = _jmsSupport.brokerAdmin();
        final var brokerAddress = brokerAdmin.getBrokerAddress(BrokerAdmin.PortType.AMQP);
        return _jmsSupport.jmsProvider().getConnectionBuilder()
                .host(brokerAddress.getHostName())
                .port(brokerAddress.getPort())
                .username(brokerAdmin.getValidUsername())
                .password(brokerAdmin.getValidPassword())
                .virtualHost(_jmsSupport.virtualHostName());
    }

    public QueueBuilder queue()
    {
        return new QueueBuilder(_jmsSupport);
    }

    public QueueBuilder queue(final String queueName)
    {
        return new QueueBuilder(_jmsSupport, queueName);
    }

    public TopicBuilder topic()
    {
        return new TopicBuilder(_jmsSupport);
    }

    public TopicBuilder topic(final String queueName)
    {
        return new TopicBuilder(_jmsSupport, queueName);
    }
}
