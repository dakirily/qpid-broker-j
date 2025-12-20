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

package org.apache.qpid.systests.builder;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import javax.naming.NamingException;
import java.util.Map;

public interface ConnectionBuilder
{
    String USERNAME = "guest";
    String PASSWORD = "guest";

    ConnectionBuilder host(String host);
    ConnectionBuilder port(int port);

    ConnectionBuilder prefetch(int prefetch);
    ConnectionBuilder clientId(String clientId);
    ConnectionBuilder username(String username);
    ConnectionBuilder password(String password);
    ConnectionBuilder virtualHost(String virtualHostName);
    ConnectionBuilder failover(boolean enableFailover);
    ConnectionBuilder failoverPort(int port);
    ConnectionBuilder failoverReconnectAttempts(int reconnectAttempts);
    ConnectionBuilder failoverReconnectDelay(int connectDelay);
    ConnectionBuilder tls(boolean enableTls);
    ConnectionBuilder syncPublish(boolean syncPublish);

    @Deprecated
    ConnectionBuilder options(Map<String, String> options);
    ConnectionBuilder populateJMSXUserID(boolean populateJMSXUserID);
    ConnectionBuilder deserializationPolicyAllowList(String allowList);
    ConnectionBuilder deserializationPolicyDenyList(String denyList);
    ConnectionBuilder keyStoreLocation(String keyStoreLocation);
    ConnectionBuilder keyStorePassword(String keyStorePassword);
    ConnectionBuilder trustStoreLocation(String trustStoreLocation);
    ConnectionBuilder trustStorePassword(String trustStorePassword);
    ConnectionBuilder verifyHostName(boolean verifyHostName);
    ConnectionBuilder keyAlias(String alias);
    ConnectionBuilder saslMechanisms(String... mechanism);

    Connection create() throws NamingException, JMSException;
    <T extends Connection> T create(Class<T> type) throws NamingException, JMSException;
    ConnectionFactory connectionFactory() throws NamingException;
    String connectionURL();

    ConnectionBuilder transport(String transport);
}
