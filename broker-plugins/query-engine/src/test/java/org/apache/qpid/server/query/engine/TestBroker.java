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

package org.apache.qpid.server.query.engine;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.security.CertificateDetails;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.server.virtualhostnode.TestVirtualHostNode;

/**
 * Helper class for creating broker mock
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBroker
{
    private static Broker<?> _broker;

    private static final List<Connection> connections = new ArrayList<>();

    public static synchronized Broker<?> createBroker()
    {
        try
        {
            if (_broker != null)
            {
                return _broker;
            }

            _broker = BrokerTestHelper.createBrokerMock();

            final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Broker.class));
            doReturn(attributeNames).when(_broker).getAttributeNames();
            doReturn("mock").when(_broker).getAttribute(eq("name"));
            doReturn("mock").when(_broker).getName();

            final Map<String, Object> attributesMap = new HashMap<>();
            attributesMap.put(AuthenticationProvider.NAME, "ScramSHA256AuthenticationManager");
            attributesMap.put(AuthenticationProvider.TYPE, "SCRAM-SHA-256");
            attributesMap.put(AuthenticationProvider.ID, UUID.randomUUID());
            final ScramSHA256AuthenticationManager authProvider =
                (ScramSHA256AuthenticationManager) _broker.getObjectFactory()
                    .create(AuthenticationProvider.class, attributesMap, _broker);
            doReturn(List.of(authProvider)).when(_broker).getAuthenticationProviders();

            final List<String> portAttributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Port.class));
            final HttpPort<?> httpPort = mock(HttpPort.class);
            doReturn(portAttributeNames).when(httpPort).getAttributeNames();
            doReturn(40606).when(httpPort).getPort();
            doReturn(40606).when(httpPort).getBoundPort();
            doReturn("admin").when(httpPort).getCreatedBy();
            doReturn(new Date()).when(httpPort).getCreatedTime();
            doReturn("httpPort").when(httpPort).getName();
            doReturn("httpPort").when(httpPort).getAttribute(Port.NAME);
            doReturn(State.ACTIVE).when(httpPort).getState();
            doReturn("HTTP").when(httpPort).getType();
            doReturn(authProvider).when(httpPort).getAttribute(HttpPort.AUTHENTICATION_PROVIDER);
            doReturn(new HashMap<>()).when(httpPort).getStatistics();

            final Port<?> amqpPort = mock(Port.class);
            doReturn(portAttributeNames).when(amqpPort).getAttributeNames();
            doReturn(connections).when(amqpPort).getConnections();
            doReturn(connections).when(amqpPort).getChildren(Connection.class);

            doReturn(40206).when(amqpPort).getPort();
            doReturn(40206).when(amqpPort).getBoundPort();
            doReturn("admin").when(amqpPort).getCreatedBy();
            doReturn(new Date()).when(amqpPort).getCreatedTime();
            doReturn("amqpPort").when(amqpPort).getName();
            doReturn("amqpPort").when(amqpPort).getAttribute(Port.NAME);
            doReturn(State.ACTIVE).when(amqpPort).getState();
            doReturn("AMQP").when(amqpPort).getType();
            final Map<String, Object> portStatistics = new HashMap<>();
            portStatistics.put("connectionCount", 30);
            portStatistics.put("totalConnectionCount", 30);
            doReturn(portStatistics).when(httpPort).getStatistics();

            doReturn(Arrays.asList(httpPort, amqpPort)).when(_broker).getPorts();
            doReturn(Arrays.asList(httpPort, amqpPort)).when(_broker).getChildren(eq(Port.class));

            final List<String> vhnAttributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(VirtualHostNode.class));
            final VirtualHostNode<?> virtualHostNode = mock(VirtualHostNode.class);
            doReturn(vhnAttributeNames).when(virtualHostNode).getAttributeNames();
            doReturn(UUID.randomUUID()).when(virtualHostNode).getAttribute(VirtualHostNode.ID);
            doReturn("default").when(virtualHostNode).getAttribute(eq(VirtualHostNode.NAME));
            doReturn(TestVirtualHostNode.VIRTUAL_HOST_NODE_TYPE).when(virtualHostNode).getAttribute(eq(VirtualHostNode.TYPE));
            doReturn(new Date()).when(virtualHostNode).getAttribute(eq(VirtualHostNode.CREATED_TIME));
            doReturn(new Date()).when(virtualHostNode).getCreatedTime();

            final VirtualHostNode mockVirtualHostNode = mock(VirtualHostNode.class);
            doReturn(vhnAttributeNames).when(mockVirtualHostNode).getAttributeNames();
            doReturn(UUID.randomUUID()).when(mockVirtualHostNode).getAttribute(VirtualHostNode.ID);
            doReturn("mock").when(mockVirtualHostNode).getAttribute(eq(VirtualHostNode.NAME));
            doReturn("mock").when(mockVirtualHostNode).getAttribute(eq(VirtualHostNode.TYPE));
            doReturn(createDate("2001-01-01 12:55:30")).when(mockVirtualHostNode).getAttribute(eq(VirtualHostNode.CREATED_TIME));
            doReturn(createDate("2001-01-01 12:55:30")).when(mockVirtualHostNode).getCreatedTime();

            doReturn(List.of(mockVirtualHostNode, virtualHostNode)).when(_broker).getVirtualHostNodes();
            doReturn(List.of(virtualHostNode, mockVirtualHostNode)).when(_broker).getChildren(eq(VirtualHostNode.class));

            final List<String> vhAttributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(VirtualHost.class));
            final VirtualHost virtualHost = mock(VirtualHost.class);
            doReturn(vhAttributeNames).when(virtualHost).getAttributeNames();
            doReturn(UUID.randomUUID()).when(virtualHost).getAttribute(VirtualHost.ID);
            doReturn(TestMemoryVirtualHost.VIRTUAL_HOST_TYPE).when(virtualHost).getAttribute(VirtualHost.TYPE);
            doReturn("default").when(virtualHost).getAttribute(VirtualHost.NAME);
            doReturn(List.of(virtualHost)).when(virtualHostNode).getChildren(eq(VirtualHost.class));
            doReturn(virtualHost).when(virtualHostNode).getVirtualHost();

            for (int i = 1 ; i < 11; i ++)
            {
                connections.add(createAMQPConnection(i, "127.0.0.1", "principal1"));
            }
            for (int i = 11 ; i < 21; i ++)
            {
                connections.add(createAMQPConnection(i, "127.0.0.2", "principal2"));
            }
            for (int i = 21 ; i < 31; i ++)
            {
                connections.add(createAMQPConnection(i, "127.0.0.3", "principal3"));
            }

            final List<Queue> queues = new ArrayList<>();
            for (int i = 0; i < 10; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.RING, "test description 1");
                queues.add(queue);
            }
            for (int i = 10; i < 20; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.REJECT, "test description 2");
                doReturn(1024 * 1024).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES);
                doReturn(100).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES);
                queues.add(queue);
            }
            for (int i = 20; i < 30; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.FLOW_TO_DISK, "test description 3");
                doReturn(1024 * 1024).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES);
                doReturn(100).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES);
                queues.add(queue);
            }
            for (int i = 30; i < 40; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.PRODUCER_FLOW_CONTROL, null);
                doReturn(1024 * 1024).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES);
                doReturn(100).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES);
                queues.add(queue);
            }
            for (int i = 40; i < 50; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.NONE, null);
                doReturn(1024 * 1024).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES);
                doReturn(100).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES);
                if (i % 2 == 0)
                {
                    doReturn(Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE).when(queue).getAttribute(Queue.EXPIRY_POLICY);
                    doReturn(LifetimePolicy.PERMANENT).when(queue).getAttribute(Queue.LIFETIME_POLICY);
                }
                else
                {
                    doReturn(LifetimePolicy.IN_USE).when(queue).getAttribute(Queue.LIFETIME_POLICY);
                }
                queues.add(queue);
            }
            for (int i = 50; i < 60; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.NONE, null);
                doReturn(1024 * 1024).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES);
                doReturn(100).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES);
                if (i % 2 == 0)
                {
                    doReturn(Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE).when(queue).getAttribute(Queue.EXPIRY_POLICY);
                    doReturn(LifetimePolicy.PERMANENT).when(queue).getAttribute(Queue.LIFETIME_POLICY);
                }
                else
                {
                    doReturn(LifetimePolicy.IN_USE).when(queue).getAttribute(Queue.LIFETIME_POLICY);
                }

                final Map<String, Object> statistics = new HashMap<>();
                statistics.put("availableMessages", 65);
                statistics.put("bindingCount", 0);
                statistics.put("queueDepthMessages", 65);
                statistics.put("queueDepthBytes", 0);
                statistics.put("totalExpiredBytes", 0);
                doReturn(statistics).when(queue).getStatistics();

                queues.add(queue);
            }

            for (int i = 60; i < 70; i++)
            {
                final Queue queue = createQueue(i, OverflowPolicy.NONE, null);
                doReturn(1024 * 1024).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES);
                doReturn(100).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES);

                final Map<String, Object> statistics = new HashMap<>();
                statistics.put("availableMessages", 95);
                statistics.put("bindingCount", 0);
                statistics.put("queueDepthMessages", 95);
                statistics.put("queueDepthBytes", 0);
                statistics.put("totalExpiredBytes", 0);
                doReturn(statistics).when(queue).getStatistics();

                queues.add(queue);
            }
            doReturn(queues).when(virtualHost).getChildren(eq(Queue.class));

            final List<Exchange> exchanges = new ArrayList<>();
            for (int i = 0; i < 10; i++)
            {
                exchanges.add(creatExchange(i));
            }
            doReturn(exchanges).when(virtualHost).getChildren(eq(Exchange.class));

            final Collection<TrustStore> trustStores = createTruststores();
            doReturn(trustStores).when(_broker).getChildren(eq(TrustStore.class));

            doReturn(10_000_000_000L).when(_broker).getAttribute("maximumHeapMemorySize");
            doReturn(1_500_000_000L).when(_broker).getAttribute("maximumDirectMemorySize");
            doReturn(getBrokerStatistics()).when(_broker).getStatistics();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        return _broker;
    }

    protected static Queue createQueue(int number, OverflowPolicy overflowPolicy, String description)
    {
        final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Queue.class));
        final Queue<?> queue = mock(Queue.class);
        doReturn(attributeNames).when(queue).getAttributeNames();
        doReturn(UUID.randomUUID()).when(queue).getAttribute(Queue.ID);
        doReturn("QUEUE_" + number).when(queue).getAttribute(Queue.NAME);
        doReturn("standard").when(queue).getAttribute(Queue.TYPE);
        final Date createdTime = new Date();
        doReturn(createdTime).when(queue).getAttribute(Queue.CREATED_TIME);
        doReturn(createdTime).when(queue).getCreatedTime();
        doReturn(new Date()).when(queue).getAttribute(Queue.LAST_UPDATED_TIME);
        doReturn(overflowPolicy).when(queue).getAttribute(Queue.OVERFLOW_POLICY);
        if (number % 2 == 0)
        {
            doReturn(Queue.ExpiryPolicy.ROUTE_TO_ALTERNATE).when(queue).getAttribute(Queue.EXPIRY_POLICY);
        }
        else
        {
            doReturn(Queue.ExpiryPolicy.DELETE).when(queue).getAttribute(Queue.EXPIRY_POLICY);
        }
        doReturn(LifetimePolicy.PERMANENT).when(queue).getAttribute(Queue.LIFETIME_POLICY);
        doReturn(description).when(queue).getAttribute(Queue.DESCRIPTION);
        doReturn(-1).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_BYTES);
        doReturn(-1).when(queue).getAttribute(Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES);

        final Map<String, Object> statistics = new HashMap<>();
        statistics.put("availableMessages", 0);
        statistics.put("bindingCount", number == 1 ? 10 : 0);
        statistics.put("queueDepthMessages", 0);
        statistics.put("queueDepthBytes", 0);
        statistics.put("totalExpiredBytes", 0);
        doReturn(statistics).when(queue).getStatistics();

        if (number > 0 && number < 11)
        {
            final Consumer consumer = createConsumer(number, "QUEUE_" + number);
            doReturn(new ArrayList<>(List.of(consumer))).when(queue).getChildren(eq(Consumer.class));
        }

        return queue;
    }

    protected static Exchange creatExchange(int number)
    {
        final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Exchange.class));
        final Exchange<?> exchange = mock(Exchange.class);
        doReturn(attributeNames).when(exchange).getAttributeNames();
        doReturn(UUID.randomUUID()).when(exchange).getAttribute(Exchange.ID);
        doReturn(ExchangeDefaults.TOPIC_EXCHANGE_CLASS).when(exchange).getAttribute(Exchange.TYPE);
        doReturn("EXCHANGE_" + number).when(exchange).getAttribute(Exchange.NAME);
        doReturn("EXCHANGE_" + number).when(exchange).getName();
        doReturn("test description " + number).when(exchange).getAttribute(Exchange.DESCRIPTION);

        final Binding binding = mock(Binding.class);
        doReturn("#").when(binding).getName();
        doReturn("#").when(binding).getBindingKey();
        doReturn("binding").when(binding).getType();
        doReturn("QUEUE_1").when(binding).getDestination();
        doReturn(new HashMap<>()).when(binding).getArguments();

        doReturn(List.of(binding)).when(exchange).getAttribute("bindings");
        doReturn(List.of(binding)).when(exchange).getBindings();

        return exchange;
    }

    protected static Collection<TrustStore> createTruststores()
    {
        final Collection<TrustStore> trustStores = new ArrayList<>();
        final List<CertificateDetails> certificateDetails = createCertificateDetails();

        final TrustStore<?> trustStore = mock(TrustStore.class);
        doReturn("peersTruststore").when(trustStore).getName();
        doReturn(UUID.randomUUID()).when(trustStore).getAttribute(TrustStore.ID);
        doReturn("peersTruststore").when(trustStore).getAttribute(TrustStore.NAME);
        doReturn("mock").when(trustStore).getAttribute(TrustStore.DESCRIPTION);
        doReturn(certificateDetails).when(trustStore).getCertificateDetails();

        trustStores.add(trustStore);

        return trustStores;
    }

    protected static List<CertificateDetails> createCertificateDetails()
    {
        final List<CertificateDetails> list = new ArrayList<>();

        final CertificateDetails certificateDetails1 = mock(CertificateDetails.class);
        doReturn("aaa_mock").when(certificateDetails1).getAlias();
        doReturn("CN=aaa_mock").when(certificateDetails1).getIssuerName();
        doReturn(String.valueOf(1L)).when(certificateDetails1).getSerialNumber();
        doReturn("SHA512withRSA").when(certificateDetails1).getSignatureAlgorithm();
        doReturn(List.of()).when(certificateDetails1).getSubjectAltNames();
        doReturn("CN=aaa_mock").when(certificateDetails1).getSubjectName();
        doReturn(createDate("2020-01-01 00:00:00")).when(certificateDetails1).getValidFrom();
        doReturn(createDate("2022-12-31 23:59:59")).when(certificateDetails1).getValidUntil();
        doReturn(3).when(certificateDetails1).getVersion();
        list.add(certificateDetails1);

        final CertificateDetails certificateDetails2 = mock(CertificateDetails.class);
        doReturn("bbb_mock").when(certificateDetails2).getAlias();
        doReturn("CN=bbb_mock").when(certificateDetails2).getIssuerName();
        doReturn(String.valueOf(100L)).when(certificateDetails2).getSerialNumber();
        doReturn("SHA512withRSA").when(certificateDetails2).getSignatureAlgorithm();
        doReturn(List.of()).when(certificateDetails2).getSubjectAltNames();
        doReturn("CN=bbb_mock").when(certificateDetails2).getSubjectName();
        doReturn(createDate("2020-01-02 00:00:00")).when(certificateDetails2).getValidFrom();
        doReturn(createDate("2023-01-01 23:59:59")).when(certificateDetails2).getValidUntil();
        list.add(certificateDetails2);

        final CertificateDetails certificateDetails3 = mock(CertificateDetails.class);
        doReturn("ccc_mock").when(certificateDetails3).getAlias();
        doReturn("CN=ccc_mock").when(certificateDetails3).getIssuerName();
        doReturn(String.valueOf(1000L)).when(certificateDetails3).getSerialNumber();
        doReturn("SHA512withRSA").when(certificateDetails3).getSignatureAlgorithm();
        doReturn(List.of()).when(certificateDetails3).getSubjectAltNames();
        doReturn("CN=ccc_mock").when(certificateDetails3).getSubjectName();
        doReturn(createDate("2020-01-03 00:00:00")).when(certificateDetails3).getValidFrom();
        doReturn(createDate("2023-01-02 23:59:59")).when(certificateDetails3).getValidUntil();
        list.add(certificateDetails3);

        final CertificateDetails certificateDetails4 = mock(CertificateDetails.class);
        doReturn("ddd_mock").when(certificateDetails4).getAlias();
        doReturn("CN=ddd_mock").when(certificateDetails4).getIssuerName();
        doReturn(String.valueOf(10_000L)).when(certificateDetails4).getSerialNumber();
        doReturn("SHA512withRSA").when(certificateDetails4).getSignatureAlgorithm();
        doReturn(List.of()).when(certificateDetails4).getSubjectAltNames();
        doReturn("CN=ddd_mock").when(certificateDetails4).getSubjectName();
        doReturn(createDate("2020-01-04 00:00:00")).when(certificateDetails4).getValidFrom();
        doReturn(createDate("2023-01-03 23:59:59")).when(certificateDetails4).getValidUntil();
        list.add(certificateDetails4);

        final CertificateDetails certificateDetails5 = mock(CertificateDetails.class);
        doReturn("eee_mock").when(certificateDetails5).getAlias();
        doReturn("CN=eee_mock").when(certificateDetails5).getIssuerName();
        doReturn(String.valueOf(100_000L)).when(certificateDetails5).getSerialNumber();
        doReturn("SHA512withRSA").when(certificateDetails5).getSignatureAlgorithm();
        doReturn(List.of()).when(certificateDetails5).getSubjectAltNames();
        doReturn("CN=eee_mock").when(certificateDetails5).getSubjectName();
        doReturn(createDate("2020-01-05 00:00:00")).when(certificateDetails5).getValidFrom();
        doReturn(createDate("2023-01-04 23:59:59")).when(certificateDetails5).getValidUntil();
        list.add(certificateDetails5);

        final CertificateDetails certificateDetails6 = mock(CertificateDetails.class);
        doReturn("fff_mock").when(certificateDetails6).getAlias();
        doReturn("CN=fff_mock").when(certificateDetails6).getIssuerName();
        doReturn(String.valueOf(1000_000L)).when(certificateDetails6).getSerialNumber();
        doReturn("SHA512withRSA").when(certificateDetails6).getSignatureAlgorithm();
        doReturn(List.of()).when(certificateDetails6).getSubjectAltNames();
        doReturn("CN=fff_mock").when(certificateDetails6).getSubjectName();
        doReturn(createDate("2020-01-06 00:00:00")).when(certificateDetails6).getValidFrom();
        doReturn(createDate("2023-01-05 23:59:59")).when(certificateDetails6).getValidUntil();
        list.add(certificateDetails6);

        final CertificateDetails certificateDetails7 = mock(CertificateDetails.class);
        doReturn("ggg_mock").when(certificateDetails7).getAlias();
        doReturn("CN=ggg_mock").when(certificateDetails7).getIssuerName();
        doReturn(String.valueOf(10_000_000L)).when(certificateDetails7).getSerialNumber();
        doReturn("SHA512withRSA").when(certificateDetails7).getSignatureAlgorithm();
        doReturn(List.of()).when(certificateDetails7).getSubjectAltNames();
        doReturn("CN=ggg_mock").when(certificateDetails7).getSubjectName();
        doReturn(createDate("2020-01-07 00:00:00")).when(certificateDetails7).getValidFrom();
        doReturn(createDate("2023-01-06 23:59:59")).when(certificateDetails7).getValidUntil();
        list.add(certificateDetails7);

        final CertificateDetails certificateDetails8 = mock(CertificateDetails.class);
        doReturn("hhh_mock").when(certificateDetails8).getAlias();
        doReturn("CN=hhh_mock").when(certificateDetails8).getIssuerName();
        doReturn(String.valueOf(100_000_000L)).when(certificateDetails8).getSerialNumber();
        doReturn("SHA512withRSA").when(certificateDetails8).getSignatureAlgorithm();
        doReturn(List.of()).when(certificateDetails8).getSubjectAltNames();
        doReturn("CN=hhh_mock").when(certificateDetails8).getSubjectName();
        doReturn(createDate("2020-01-08 00:00:00")).when(certificateDetails8).getValidFrom();
        doReturn(createDate("2023-01-07 23:59:59")).when(certificateDetails8).getValidUntil();
        list.add(certificateDetails8);

        final CertificateDetails certificateDetails9 = mock(CertificateDetails.class);
        doReturn("iii_mock").when(certificateDetails9).getAlias();
        doReturn("CN=iii_mock").when(certificateDetails9).getIssuerName();
        doReturn(String.valueOf(1000_000_000L)).when(certificateDetails9).getSerialNumber();
        doReturn("SHA512withRSA").when(certificateDetails9).getSignatureAlgorithm();
        doReturn(List.of()).when(certificateDetails9).getSubjectAltNames();
        doReturn("CN=iii_mock").when(certificateDetails9).getSubjectName();
        doReturn(createDate("2020-01-09 00:00:00")).when(certificateDetails9).getValidFrom();
        doReturn(createDate("2023-01-08 23:59:59")).when(certificateDetails9).getValidUntil();
        list.add(certificateDetails9);

        final CertificateDetails certificateDetails10 = mock(CertificateDetails.class);
        doReturn("jjj_mock").when(certificateDetails10).getAlias();
        doReturn("CN=jjj_mock").when(certificateDetails10).getIssuerName();
        doReturn(new BigInteger("17593798617727720249").toString(10)).when(certificateDetails10).getSerialNumber();
        doReturn("SHA512withRSA").when(certificateDetails10).getSignatureAlgorithm();
        doReturn(List.of()).when(certificateDetails10).getSubjectAltNames();
        doReturn("CN=jjj_mock").when(certificateDetails10).getSubjectName();
        doReturn(createDate("2020-01-10 00:00:00")).when(certificateDetails10).getValidFrom();
        doReturn(createDate("2023-01-09 23:59:59")).when(certificateDetails10).getValidUntil();
        list.add(certificateDetails10);

        return list;
    }

    protected static Connection createAMQPConnection(int number, String hostname, String principal)
    {
        final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Connection.class));
        AMQPConnection<?> connection = mock(AMQPConnection.class);
        doReturn(attributeNames).when(connection).getAttributeNames();
        final UUID id = UUID.randomUUID();
        doReturn(id).when(connection).getAttribute(Connection.ID);
        doReturn(id).when(connection).getId();
        doReturn(String.format("[%d] %s:%d", number, hostname, (47290 + number))).when(connection).getAttribute(Connection.NAME);
        doReturn(null).when(connection).getAttribute(Connection.DESCRIPTION);
        doReturn("AMQP_1_0").when(connection).getAttribute(Connection.TYPE);
        doReturn(State.ACTIVE).when(connection).getAttribute(Connection.STATE);
        doReturn(false).when(connection).getAttribute(Connection.DURABLE);
        doReturn(LifetimePolicy.PERMANENT).when(connection).getAttribute(Connection.LIFETIME_POLICY);
        doReturn(new HashMap<>()).when(connection).getAttribute(Connection.CONTEXT);
        doReturn("ID:" + UUID.randomUUID() + ":1").when(connection).getAttribute(Connection.CLIENT_ID);
        doReturn("QpidJMS").when(connection).getClientProduct();
        doReturn("0.32.0").when(connection).getAttribute(Connection.CLIENT_VERSION);
        doReturn(true).when(connection).getAttribute(Connection.INCOMING);
        doReturn(principal).when(connection).getAttribute(Connection.PRINCIPAL);
        doReturn(Protocol.AMQP_1_0).when(connection).getProtocol();
        doReturn(String.format("/%s:%d", hostname, (47290 + number))).when(connection).getAttribute(Connection.REMOTE_ADDRESS);
        doReturn(Transport.TCP).when(connection).getAttribute(Connection.TRANSPORT);

        final Session session = createSession(0);
        doReturn(List.of(session)).when(connection).getSessions();
        doReturn(List.of(session)).when(connection).getChildren(eq(Session.class));

        return connection;
    }

    protected static Consumer<?, ?> createConsumer(int connectionNumber, String queueName)
    {
        final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Consumer.class));
        final Consumer<?, ?> consumer = mock(Consumer.class);
        doReturn(attributeNames).when(consumer).getAttributeNames();
        doReturn(UUID.randomUUID()).when(consumer).getAttribute(Consumer.ID);
        doReturn("%d|1|qpid-jms:receiver:ID:%s:1:1:1:%s".formatted(connectionNumber, UUID.randomUUID(), queueName))
                .when(consumer).getAttribute(Consumer.NAME);
        final Session session = (Session) connections.get(connectionNumber).getSessions().iterator().next();
        doReturn(session).when(consumer).getSession();
        doReturn(session).when(consumer).getAttribute("session");
        return consumer;
    }

    protected static Session<?> createSession(int sessionNumber)
    {
        final List<String> attributeNames = new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Session.class));
        final Session<?> session = mock(Session.class);
        doReturn(attributeNames).when(session).getAttributeNames();
        final UUID id = UUID.randomUUID();
        doReturn(id).when(session).getAttribute(Session.ID);
        doReturn(id).when(session).getId();
        doReturn(sessionNumber).when(session).getAttribute(Session.NAME);
        doReturn(String.valueOf(sessionNumber)).when(session).getName();
        doReturn("Session").when(session).getType();
        doReturn("test description").when(session).getDescription();
        doReturn(State.ACTIVE).when(session).getDesiredState();
        doReturn(State.ACTIVE).when(session).getState();
        doReturn(true).when(session).isDurable();
        doReturn(LifetimePolicy.PERMANENT).when(session).getLifetimePolicy();
        doReturn(0).when(session).getChannelId();
        doReturn(new Date()).when(session).getLastOpenedTime();
        doReturn(false).when(session).isProducerFlowBlocked();
        doReturn(new Date()).when(session).getLastUpdatedTime();
        doReturn("admin").when(session).getLastUpdatedBy();
        doReturn("admin").when(session).getCreatedBy();
        doReturn(new Date()).when(session).getCreatedTime();
        doReturn(new HashMap<>()).when(session).getStatistics();
        return session;
    }

    protected static Map<String, Object> getBrokerStatistics()
    {
        final Map<String, Object> map = new HashMap<>();
        map.put("usedHeapMemorySize", 6_000_000_000L);
        map.put("usedDirectMemorySize", 500_000_000L);
        map.put("processCpuLoad", 0.052);
        return map;
    }

    protected static Date createDate(String date)
    {
        final LocalDateTime dateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"));
        return Date.from(dateTime.toInstant(ZoneOffset.UTC));
    }
}
