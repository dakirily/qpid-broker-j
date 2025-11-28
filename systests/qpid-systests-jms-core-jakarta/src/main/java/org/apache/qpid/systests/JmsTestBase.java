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
package org.apache.qpid.systests;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.QueueConnection;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnection;
import javax.naming.NamingException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.qpid.systests.Utils.getAmqpManagementFacade;
import static org.apache.qpid.systests.Utils.getJmsProvider;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class JmsTestBase extends BrokerAdminUsingTestBase
{
    public static final String DEFAULT_BROKER_CONFIG = "classpath:config-jms-tests.json";

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsTestBase.class);
    private static JmsProvider _jmsProvider;
    private static AmqpManagementFacade _managementFacade;

    @BeforeAll
    public static void setUpTestBase()
    {
        _managementFacade = getAmqpManagementFacade();
        _jmsProvider = getJmsProvider();
        LOGGER.debug("Test receive timeout is {} milliseconds", getReceiveTimeout());
    }

    protected ConnectionBuilder getConnectionBuilder()
    {
        final InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        return _jmsProvider.getConnectionBuilder()
                           .setHost(brokerAddress.getHostName())
                           .setPort(brokerAddress.getPort())
                           .setUsername(getBrokerAdmin().getValidUsername())
                           .setPassword(getBrokerAdmin().getValidPassword());
    }

    protected void createEntityUsingAmqpManagement(final String entityName,
                                                   final String entityType,
                                                   final Map<String, Object> attributes) throws Exception
    {
        try (Connection connection = getConnection())
        {
            connection.start();
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            _managementFacade.createEntityUsingAmqpManagement(entityName, session, entityType, attributes);
        }
    }

    protected Object performOperationUsingAmqpManagement(final String name,
                                                         final String operation,
                                                         final String type,
                                                         final Map<String, Object> arguments) throws Exception
    {
        try (Connection connection = getConnection())
        {
            connection.start();
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return _managementFacade.performOperationUsingAmqpManagement(name, operation, session, type, arguments);
        }
    }

    protected Connection getConnection() throws JMSException, NamingException
    {
        return getConnectionBuilder().build();
    }

    protected static long getReceiveTimeout()
    {
        return Utils.getReceiveTimeout();
    }

    protected String getVirtualHostName()
    {
        return getClass().getSimpleName() + "_" + getTestName();
    }

    protected Queue getQueue(String queueName) throws Exception
    {
        return _jmsProvider.getTestQueue(queueName);
    }

    protected Topic createTopic(final String topicName) throws Exception
    {
        try (Connection connection = getConnection())
        {
            return _jmsProvider.createTopic(connection, topicName);
        }
    }

    protected Queue createQueue(final String queueName) throws Exception
    {
        return createQueue(getVirtualHostName(), queueName);
    }

    protected Queue createQueue(final String virtualHostName, final String queueName) throws Exception
    {
        try (Connection connection = getConnectionBuilder().setVirtualHost(virtualHostName).build())
        {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
            {
                return _jmsProvider.createQueue(session, queueName);
            }
        }
    }

    protected int getQueueCount() throws Exception
    {
        final Map<String, Object> statisticsMap = getVirtualHostStatistics("queueCount");
        return ((Number) statisticsMap.get("queueCount")).intValue();
    }

    protected long getTotalDepthOfQueuesMessages() throws Exception
    {
        final Map<String, Object> statisticsMap = getVirtualHostStatistics("totalDepthOfQueuesMessages");
        return ((Number) statisticsMap.get("totalDepthOfQueuesMessages")).intValue();
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getVirtualHostStatistics(final String... statisticsName) throws Exception
    {
        final Map<String, Object> arguments = Collections.singletonMap("statistics", Arrays.asList(statisticsName));
        Object statistics = performOperationUsingAmqpManagement(getVirtualHostName(),
                                                                "getStatistics",
                                                                "org.apache.qpid.VirtualHost",
                                                                arguments);

        assertNotNull(statistics, "Statistics is null");
        assertInstanceOf(Map.class, statistics, "Statistics is not map");

        return (Map<String, Object>) statistics;
    }

    protected void updateEntityUsingAmqpManagement(final String entityName,
                                                   final String entityType,
                                                   final Map<String, Object> attributes) throws Exception
    {
        try (Connection connection = getConnection())
        {
            connection.start();
            updateEntityUsingAmqpManagement(entityName, entityType, attributes, connection);
        }
    }

    protected void updateEntityUsingAmqpManagement(final String entityName,
                                                 final String entityType,
                                                 final Map<String, Object> attributes,
                                                 final Connection connection) throws JMSException
    {
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            _managementFacade.updateEntityUsingAmqpManagementAndReceiveResponse(entityName, entityType, attributes, session);
        }
    }

    protected void deleteEntityUsingAmqpManagement(final String entityName,
                                                   final String entityType) throws Exception
    {
        try (Connection connection = getConnection())
        {
            connection.start();
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            _managementFacade.deleteEntityUsingAmqpManagement(entityName, session, entityType);
        }
    }

    protected Map<String, Object> readEntityUsingAmqpManagement(String name, String type, boolean actuals)
            throws Exception
    {
        try (Connection connection = getConnection())
        {
            connection.start();
            return readEntityUsingAmqpManagement(name, type, actuals, connection);
        }
    }

    protected Map<String, Object> readEntityUsingAmqpManagement(final String name,
                                                                final String type,
                                                                final boolean actuals,
                                                                final Connection connection) throws JMSException
    {
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            return _managementFacade.readEntityUsingAmqpManagement(session, type, name, actuals);
        }
    }

    protected List<Map<String, Object>> queryEntitiesUsingAmqpManagement(final String type, final Connection connection)
            throws JMSException
    {
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            return _managementFacade.managementQueryObjects(session, type);
        }
    }

    protected Map<String, Object> createEntity(final String entityName,
                                               final String entityType,
                                               final Map<String, Object> attributes,
                                               final Connection connection) throws Exception
    {
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            return _managementFacade.createEntityAndAssertResponse(entityName, entityType, attributes, session);
        }
    }

    protected TopicConnection getTopicConnection() throws JMSException, NamingException
    {
        return (TopicConnection) getConnection();
    }

    protected static Protocol getProtocol()
    {
        return Utils.getProtocol();
    }

    public QueueConnection getQueueConnection() throws JMSException, NamingException
    {
        return (QueueConnection)getConnection();
    }
}
