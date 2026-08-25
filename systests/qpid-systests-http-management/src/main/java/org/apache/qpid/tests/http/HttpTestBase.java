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

package org.apache.qpid.tests.http;

import static org.apache.qpid.systests.Utils.getJmsProvider;

import java.net.InetSocketAddress;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.jupiter.api.BeforeEach;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.ConnectionBuilder;
import org.apache.qpid.systests.JmsProvider;
import org.apache.qpid.systests.Utils;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;

public abstract class HttpTestBase extends BrokerAdminUsingTestBase
{
    public static final String DEFAULT_BROKER_CONFIG = "classpath:config-http-management-tests.json";

    private HttpTestHelper _brokerHelper;
    private HttpTestHelper _virtualHostHelper;

    private JmsProvider _jmsProvider;

    @BeforeEach
    public void setUpTestBase()
    {
        _brokerHelper = new HttpTestHelper(getBrokerAdmin(), BrokerAdmin.PortType.HTTP_BROKER);
        _virtualHostHelper = new HttpTestHelper(getBrokerAdmin(), BrokerAdmin.PortType.HTTP_VIRTUAL_HOST);
        _jmsProvider = getJmsProvider();
    }

    protected String getVirtualHost()
    {
        return getClass().getSimpleName() + "_" + getTestName();
    }

    protected String getVirtualHostNode()
    {
        return getClass().getSimpleName() + "_" + getTestName();
    }

    public HttpTestHelper getBrokerHelper()
    {
        return _brokerHelper;
    }

    public HttpTestHelper getVirtualHostHelper()
    {
        return _virtualHostHelper;
    }

    protected Connection getConnection() throws JMSException, NamingException
    {
        return getConnectionBuilder().build();
    }

    protected ConnectionBuilder getConnectionBuilder()
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        return _jmsProvider.getConnectionBuilder()
                           .setHost(brokerAddress.getHostName())
                           .setPort(brokerAddress.getPort())
                           .setUsername(getBrokerAdmin().getValidUsername())
                           .setPassword(getBrokerAdmin().getValidPassword());
    }

    protected static long getReceiveTimeout()
    {
        return Utils.getReceiveTimeout();
    }

    protected static Protocol getProtocol()
    {
        return Utils.getProtocol();
    }
}
