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
import jakarta.jms.Session;
import org.apache.qpid.systests.AmqpManagementFacade;

import java.util.Map;

public class AmqpManagementSupport
{
    private final JmsSupport _jmsSupport;
    private final AmqpManagementFacade _amqpManagementFacade;
    private final Connection _connection;

    public AmqpManagementSupport(final JmsSupport jmsSupport, final AmqpManagementFacade amqpManagementFacade)
    {
        _jmsSupport = jmsSupport;
        _amqpManagementFacade = amqpManagementFacade;
        _connection = null;
    }

    public AmqpManagementSupport(final JmsSupport jmsSupport,
                                 final AmqpManagementFacade amqpManagementFacade,
                                 final Connection connection)
    {
        _jmsSupport = jmsSupport;
        _amqpManagementFacade = amqpManagementFacade;
        _connection = connection;
    }

    public void create(final String entityName,
                       final String entityType,
                       final Map<String, Object> attributes) throws Exception
    {
        if (_connection != null)
        {
            create(_connection, entityName, entityType, attributes);
            return;
        }
        try (final var connection = _jmsSupport.builder().connection().create())
        {
            connection.start();
            create(connection, entityName, entityType, attributes);
        }
    }

    private void create(final Connection connection,
                        final String entityName,
                        final String entityType,
                        final Map<String, Object> attributes) throws Exception
    {
        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            _amqpManagementFacade.createEntityAndAssertResponse(entityName, entityType, attributes, session);
        }
    }

    public Map<String, Object> read(final String name, final String type, final boolean actuals)
            throws Exception
    {
        if (_connection != null)
        {
            return read(_connection, name, type, actuals);
        }
        try (final var connection = _jmsSupport.builder().connection().create())
        {
            connection.start();
            return read(connection, name, type, actuals);
        }
    }

    private Map<String, Object> read(final Connection connection,
                                     final String name,
                                     final String type,
                                     final boolean actuals) throws Exception
    {
        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            return _amqpManagementFacade.readEntityUsingAmqpManagement(session, type, name, actuals);
        }
    }

    public void update(final String entityName,
                       final String entityType,
                       final Map<String, Object> attributes) throws Exception
    {
        if (_connection != null)
        {
            update(_connection, entityName, entityType, attributes);
            return;
        }
        try (final var connection = _jmsSupport.builder().connection().create())
        {
            connection.start();
            update(connection, entityName, entityType, attributes);
        }
    }

    private void update(final Connection connection,
                        final String entityName,
                        final String entityType,
                        final Map<String, Object> attributes) throws Exception
    {
        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            _amqpManagementFacade.updateEntityUsingAmqpManagementAndReceiveResponse(entityName, entityType, attributes, session);
        }
    }

    public void delete(final String entityName, final String entityType) throws Exception
    {
        if (_connection != null)
        {
            delete(_connection, entityName, entityType);
            return;
        }
        try (final var connection =_jmsSupport.builder().connection().create())
        {
            connection.start();
            delete(connection, entityName, entityType);
        }
    }

    private void delete(final Connection connection,
                        final String entityName,
                        final String entityType) throws Exception
    {
        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            _amqpManagementFacade.deleteEntityUsingAmqpManagement(entityName, session, entityType);
        }
    }

    public <T> T perform(final String name,
                          final String operation,
                          final String type,
                          final Map<String, Object> arguments) throws Exception
    {
        if (_connection != null)
        {
            return perform(_connection, name, operation, type, arguments);
        }
        try (final var connection =_jmsSupport.builder().connection().create())
        {
            connection.start();
            return perform(connection, name, operation, type, arguments);
        }
    }

    public <T> T perform(final Connection connection,
                          final String name,
                          final String operation,
                          final String type,
                          final Map<String, Object> arguments) throws Exception
    {
        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            return (T) _amqpManagementFacade.performOperationUsingAmqpManagement(name, operation, session, type, arguments);
        }
    }
}
