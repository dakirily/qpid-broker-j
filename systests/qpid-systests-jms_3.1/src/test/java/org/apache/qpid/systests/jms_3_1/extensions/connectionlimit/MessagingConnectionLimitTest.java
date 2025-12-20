/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.qpid.systests.jms_3_1.extensions.connectionlimit;

import org.apache.qpid.server.user.connection.limits.plugins.ConnectionLimitRule;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tools.jackson.databind.ObjectMapper;

import static org.apache.qpid.systests.EntityTypes.RULE_BASED_VIRTUAL_HOST_USER_CONNECTION_LIMIT_PROVIDER;
import static org.junit.jupiter.api.Assertions.assertThrows;

@JmsSystemTest
@Tag("connection")
@Tag("policy")
class MessagingConnectionLimitTest
{
    private static final String USER = "admin";
    private static final String USER_SECRET = "admin";
    private static final String FREQUENCY_PERIOD = "frequencyPeriod";
    private static final String RULES = "rules";

    @Test
    void authorizationWithConnectionLimit(final JmsSupport jms) throws Exception
    {
        final int connectionLimit = 2;
        configureCLT(jms, new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return null;
            }

            @Override
            public String getIdentity()
            {
                return USER;
            }

            @Override
            public Boolean getBlocked()
            {
                return Boolean.FALSE;
            }

            @Override
            public Integer getCountLimit()
            {
                return connectionLimit;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return null;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return null;
            }
        });

        final List<Connection> establishedConnections = new ArrayList<>();
        try
        {
            establishConnections(jms, connectionLimit, establishedConnections);
            verifyConnectionEstablishmentFails(jms, connectionLimit);
            establishedConnections.remove(0).close();
            establishConnection(jms, establishedConnections, connectionLimit);
        }
        finally
        {
            closeConnections(establishedConnections);
        }
    }

    @Test
    void authorizationWithConnectionFrequencyLimit(final JmsSupport jms) throws Exception
    {
        final int connectionFrequencyLimit = 1;
        configureCLT(jms, new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return null;
            }

            @Override
            public String getIdentity()
            {
                return USER;
            }

            @Override
            public Boolean getBlocked()
            {
                return Boolean.FALSE;
            }

            @Override
            public Integer getCountLimit()
            {
                return null;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return connectionFrequencyLimit;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return 60L * 1000L;
            }
        });

        final List<Connection> establishedConnections = new ArrayList<>();
        try
        {
            establishConnections(jms, connectionFrequencyLimit, establishedConnections);
            verifyConnectionEstablishmentFails(jms, connectionFrequencyLimit);
            establishedConnections.remove(0).close();
            verifyConnectionEstablishmentFails(jms, connectionFrequencyLimit);
        }
        finally
        {
            closeConnections(establishedConnections);
        }
    }

    @Test
    void authorizationWithConnectionLimitAndFrequencyLimit(final JmsSupport jms) throws Exception
    {
        final int connectionFrequencyLimit = 2;
        final int connectionLimit = 3;
        configureCLT(jms, new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return null;
            }

            @Override
            public String getIdentity()
            {
                return USER;
            }

            @Override
            public Boolean getBlocked()
            {
                return Boolean.FALSE;
            }

            @Override
            public Integer getCountLimit()
            {
                return connectionLimit;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return connectionFrequencyLimit;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return 60L * 1000L;
            }
        });

        final List<Connection> establishedConnections = new ArrayList<>();
        try
        {
            establishConnections(jms, connectionFrequencyLimit, establishedConnections);
            verifyConnectionEstablishmentFails(jms, connectionFrequencyLimit);
            establishedConnections.remove(0).close();
            verifyConnectionEstablishmentFails(jms, connectionFrequencyLimit);
        }
        finally
        {
            closeConnections(establishedConnections);
        }
    }

    @Test
    void authorizationWithBlockedUser(final JmsSupport jms) throws Exception
    {
        configureCLT(jms, new ConnectionLimitRule()
        {
            @Override
            public String getPort()
            {
                return null;
            }

            @Override
            public String getIdentity()
            {
                return USER;
            }

            @Override
            public Boolean getBlocked()
            {
                return Boolean.TRUE;
            }

            @Override
            public Integer getCountLimit()
            {
                return null;
            }

            @Override
            public Integer getFrequencyLimit()
            {
                return null;
            }

            @Override
            public Long getFrequencyPeriod()
            {
                return null;
            }
        });
        verifyConnectionEstablishmentFails(jms, 0);
    }

    private void establishConnections(final JmsSupport jms, final int connectionNumber, final List<Connection> establishedConnections)
            throws NamingException, JMSException
    {
        for (int i = 0; i < connectionNumber; i++)
        {
            establishConnection(jms, establishedConnections, i);
        }
    }

    private void establishConnection(final JmsSupport jms, List<Connection> establishedConnections, int index)
            throws NamingException, JMSException
    {
        establishedConnections.add(jms.builder().connection().username(USER)
                .password(USER_SECRET)
                .clientId(jms.testMethodName() + index)
                .create());
    }

    private void closeConnections(final List<Connection> establishedConnections) throws JMSException
    {
        for (final Connection c : establishedConnections)
        {
            try
            {
                c.close();
            }
            catch (RuntimeException e)
            {
                // Close all connections
            }
        }
    }

    private void verifyConnectionEstablishmentFails(final JmsSupport jms, final int frequencyLimit) throws NamingException
    {
        assertThrows(JMSException.class, () ->
        {
            try (final var connection = jms.builder().connection().username(USER)
                    .password(USER_SECRET)
                    .clientId(jms.testMethodName() + frequencyLimit)
                    .create())
            {
                connection.start();
            }
        }, "Connection creation should fail due to exceeding limit");
    }

    private void configureCLT(final JmsSupport jms, ConnectionLimitRule... rules) throws Exception
    {
        final String serializedRules = new ObjectMapper().writeValueAsString(rules);
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(RULES, serializedRules);
        attributes.put(FREQUENCY_PERIOD, "60000");
        jms.management().create("clt", RULE_BASED_VIRTUAL_HOST_USER_CONNECTION_LIMIT_PROVIDER, attributes);
    }
}
