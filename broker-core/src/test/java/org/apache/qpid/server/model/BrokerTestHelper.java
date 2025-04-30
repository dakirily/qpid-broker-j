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
package org.apache.qpid.server.model;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.MessageReference;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class BrokerTestHelper
{
    private static final Principal PRINCIPAL = () -> "TEST";
    private static final Subject SYSTEM_SUBJECT = new Subject(true, Set.of(PRINCIPAL), Set.of(), Set.of());
    private static final List<VirtualHost> CREATED_VIRTUAL_HOSTS = new ArrayList<>();
    private static final ThreadLocal<TaskExecutor> TASK_EXECUTOR = ThreadLocal
            .withInitial(CurrentThreadTaskExecutor::newStartedInstance);

    private static final Runnable CLOSE_VIRTUAL_HOSTS = () ->
    {
        CREATED_VIRTUAL_HOSTS.forEach(VirtualHost::close);
        CREATED_VIRTUAL_HOSTS.clear();
    };

    private static final ThreadLocal<Broker> BROKER = ThreadLocal.withInitial(() -> null);

    public static Broker<?> createBrokerMock()
    {
        if (BROKER.get() != null)
        {
            return BROKER.get();
        }
        BROKER.set(createBrokerMock(createAccessControlMock()));
        return BROKER.get();
    }

    public static Broker<?> createNewBrokerMock()
    {
        return createBrokerMock(createAccessControlMock());
    }

    public static AccessControl createAccessControlMock()
    {
        final AccessControl accessControl = mock(AccessControl.class);
        doReturn(Result.DEFER).when(accessControl).authorise(any(SecurityToken.class), any(Operation.class), any(PermissionedObject.class));
        doReturn(Result.DEFER).when(accessControl).authorise(isNull(), any(Operation.class), any(PermissionedObject.class));
        doReturn(Result.DEFER).when(accessControl).authorise(any(SecurityToken.class), any(Operation.class), any(PermissionedObject.class), any(Map.class));
        doReturn(Result.DEFER).when(accessControl).authorise(isNull(), any(Operation.class), any(PermissionedObject.class), any(Map.class));
        doReturn(Result.ALLOWED).when(accessControl).getDefault();
        return accessControl;
    }

    private static Broker<?> createBrokerMock(final AccessControl accessControl)
    {
        final ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        final EventLogger eventLogger = new EventLogger();

        final SystemConfig systemConfig = mock(SystemConfig.class);
        doReturn(eventLogger).when(systemConfig).getEventLogger();
        doReturn(objectFactory).when(systemConfig).getObjectFactory();
        doReturn(objectFactory.getModel()).when(systemConfig).getModel();
        doReturn(SystemConfig.class).when(systemConfig).getCategoryClass();
        doReturn(SystemConfig.class).when(systemConfig).getTypeClass();

        final Broker broker = mockWithSystemPrincipalAndAccessControl(Broker.class, PRINCIPAL, accessControl);
        doReturn(UUID.randomUUID()).when(broker).getId();
        doReturn(objectFactory).when(broker).getObjectFactory();
        doReturn(objectFactory.getModel()).when(broker).getModel();
        doReturn(BrokerModel.MODEL_VERSION).when(broker).getModelVersion();
        doReturn(eventLogger).when(broker).getEventLogger();
        doReturn(Broker.class).when(broker).getCategoryClass();
        doReturn(Broker.class).when(broker).getTypeClass();
        doReturn(systemConfig).when(broker).getParent();
        doReturn(0L).when(broker).getContextValue(eq(Long.class), eq(Broker.CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT));
        doReturn(Long.MAX_VALUE).when(broker).getFlowToDiskThreshold();
        doReturn(TASK_EXECUTOR.get()).when(broker).getTaskExecutor();
        doReturn(TASK_EXECUTOR.get()).when(broker).getChildExecutor();
        doReturn(TASK_EXECUTOR.get()).when(systemConfig).getTaskExecutor();
        doReturn(TASK_EXECUTOR.get()).when(systemConfig).getChildExecutor();
        doReturn(mock(PreferenceStore.class)).when(systemConfig).createPreferenceStore();
        doReturn(Set.of(broker)).when(systemConfig).getChildren(Broker.class);

        return broker;
    }

    public static QueueManagingVirtualHost<?> createVirtualHost(final Map<String, Object> attributes,
                                                                final UnitTestBase testBase)
    {
        final Broker<?> broker = createBrokerMock(createAccessControlMock());
        return createVirtualHost(attributes, broker, false, createAccessControlMock(), testBase);
    }

    private static QueueManagingVirtualHost<?> createVirtualHost(final Map<String, Object> attributes,
                                                                 final Broker<?> broker,
                                                                 final boolean defaultVHN,
                                                                 final AccessControl accessControl,
                                                                 final UnitTestBase testBase)
    {
        final ConfiguredObjectFactory objectFactory = broker.getObjectFactory();
        final String virtualHostNodeName = String.format("%s_%s", attributes.get(VirtualHostNode.NAME), "_node");
        final VirtualHostNode virtualHostNode =
                createVirtualHostNodeMock(virtualHostNodeName, defaultVHN, accessControl, broker);

        final AbstractVirtualHost host = (AbstractVirtualHost) objectFactory
                .create(VirtualHost.class, attributes, virtualHostNode );
        host.start();
        doReturn(host).when(virtualHostNode).getVirtualHost();
        CREATED_VIRTUAL_HOSTS.add(host);

        testBase.registerAfterAllTearDown(CLOSE_VIRTUAL_HOSTS);
        return host;
    }

    public static VirtualHostNode createVirtualHostNodeMock(final String virtualHostNodeName,
                                                            final boolean defaultVHN,
                                                            final AccessControl accessControl,
                                                            final Broker<?> broker)
    {
        final ConfiguredObjectFactory objectFactory = broker.getObjectFactory();

        final VirtualHostNode virtualHostNode = mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class,
                PRINCIPAL, accessControl);
        doReturn(virtualHostNodeName).when(virtualHostNode).getName();
        doReturn(TASK_EXECUTOR.get()).when(virtualHostNode).getTaskExecutor();
        doReturn(TASK_EXECUTOR.get()).when(virtualHostNode).getChildExecutor();
        doReturn(defaultVHN).when(virtualHostNode).isDefaultVirtualHostNode();

        doReturn(broker).when(virtualHostNode).getParent();

        Collection<VirtualHostNode<?>> nodes = broker.getVirtualHostNodes();
        nodes = new ArrayList<>(nodes != null ? nodes : List.of());
        nodes.add(virtualHostNode);
        doReturn(nodes).when(broker).getVirtualHostNodes();

        final DurableConfigurationStore dcs = mock(DurableConfigurationStore.class);
        doReturn(dcs).when(virtualHostNode).getConfigurationStore();
        doReturn(objectFactory.getModel()).when(virtualHostNode).getModel();
        doReturn(objectFactory).when(virtualHostNode).getObjectFactory();
        doReturn(VirtualHostNode.class).when(virtualHostNode).getCategoryClass();
        doReturn(TASK_EXECUTOR.get()).when(virtualHostNode).getTaskExecutor();
        doReturn(TASK_EXECUTOR.get()).when(virtualHostNode).getChildExecutor();
        doReturn(mock(PreferenceStore.class)).when(virtualHostNode).createPreferenceStore();
        return virtualHostNode;
    }

    public static QueueManagingVirtualHost<?> createVirtualHost(final String name,
                                                                final UnitTestBase testBase) throws Exception
    {
        return createVirtualHost(name, createBrokerMock(createAccessControlMock()), false, createAccessControlMock(),
                                 testBase);
    }

    public static QueueManagingVirtualHost<?> createVirtualHost(final String name,
                                                                final Broker<?> broker,
                                                                final boolean defaultVHN,
                                                                final UnitTestBase testBase) throws Exception
    {
        return createVirtualHost(name, broker, defaultVHN, createAccessControlMock(), testBase);
    }

    private static QueueManagingVirtualHost<?> createVirtualHost(final String name,
                                                                 final Broker<?> broker,
                                                                 final boolean defaultVHN,
                                                                 final AccessControl accessControl,
                                                                 final UnitTestBase testBase)
    {
        final Map<String,Object> attributes = Map.of(
                org.apache.qpid.server.model.VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE,
                org.apache.qpid.server.model.VirtualHost.NAME, name);

        return createVirtualHost(attributes, broker, defaultVHN, accessControl, testBase);
    }

    public static AMQPSession<?,?> createSession(final int channelId, final AMQPConnection<?> connection)
    {
        final AMQPSession session = mock(AMQPSession.class);
        doReturn(connection).when(session).getAMQPConnection();
        doReturn(channelId).when(session).getChannelId();
        return session;
    }

    public static AMQPSession<?,?> createSession(final int channelId) throws Exception
    {
        final AMQPConnection<?> session = createConnection();
        return createSession(channelId, session);
    }

    public static AMQPSession<?,?> createSession() throws Exception
    {
        return createSession(1);
    }

    public static AMQPConnection<?> createConnection()
    {
        return createConnection("test");
    }

    public static AMQPConnection<?> createConnection(final String hostName)
    {
        return mock(AMQPConnection.class);
    }

    public static Exchange<?> createExchange(final String hostName, final boolean durable, final EventLogger eventLogger)
    {
        final QueueManagingVirtualHost virtualHost =  mockWithSystemPrincipal(QueueManagingVirtualHost.class, PRINCIPAL);
        doReturn(hostName).when(virtualHost).getName();
        doReturn(eventLogger).when(virtualHost).getEventLogger();
        doReturn(mock(DurableConfigurationStore.class)).when(virtualHost).getDurableConfigurationStore();
        final ConfiguredObjectFactory objectFactory = new ConfiguredObjectFactoryImpl(BrokerModel.getInstance());
        doReturn(objectFactory).when(virtualHost).getObjectFactory();
        doReturn(objectFactory.getModel()).when(virtualHost).getModel();
        doReturn(TASK_EXECUTOR.get()).when(virtualHost).getTaskExecutor();
        doReturn(TASK_EXECUTOR.get()).when(virtualHost).getChildExecutor();
        doReturn(VirtualHost.class).when(virtualHost).getCategoryClass();
        final Map<String,Object> attributes = Map.of(
                Exchange.ID, UUIDGenerator.generateExchangeUUID("amp.direct", virtualHost.getName()),
                Exchange.NAME, "amq.direct",
                Exchange.TYPE, "direct",
                Exchange.DURABLE, durable);

        return Subject.doAs(SYSTEM_SUBJECT, (PrivilegedAction<Exchange<?>>) () ->
                (Exchange<?>) objectFactory.create(Exchange.class, attributes, virtualHost));

    }

    public static Queue<?> createQueue(final String queueName, final VirtualHost<?> virtualHost)
    {
        final Map<String,Object> attributes = Map.of(Queue.ID, UUIDGenerator.generateRandomUUID(),
                Queue.NAME, queueName);
        return virtualHost.createChild(Queue.class, attributes);
    }

    // The generated classes can't directly inherit from SystemPricipalSource / AccessControlSource as
    // these are package local, and package local visibility is prevented between classes loaded from different
    // class loaders.  Using these "public" interfaces gets around the problem.
    public interface TestableSystemPrincipalSource extends SystemPrincipalSource {}
    public interface TestableAccessControlSource extends AccessControlSource {}

    public static <X extends ConfiguredObject> X mockWithSystemPrincipal(final Class<X> clazz, final Principal principal)
    {
        final X mock = mock(clazz, withSettings().extraInterfaces(TestableSystemPrincipalSource.class));
        doReturn(principal).when((SystemPrincipalSource) mock).getSystemPrincipal();
        return mock;
    }

    public static <X extends ConfiguredObject> X mockWithSystemPrincipalAndAccessControl(final Class<X> clazz,
                                                                                         final Principal principal,
                                                                                         final AccessControl accessControl)
    {
        final X mock = mock(clazz, withSettings()
                .extraInterfaces(TestableSystemPrincipalSource.class, TestableAccessControlSource.class));
        doReturn(principal).when((SystemPrincipalSource) mock).getSystemPrincipal();
        doReturn(accessControl).when((AccessControlSource) mock).getAccessControl();
        return mock;
    }

    public static <X extends ConfiguredObject> X mockAsSystemPrincipalSource(final Class<X> clazz)
    {
        return mockWithSystemPrincipal(clazz, PRINCIPAL);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static ServerMessage<?> createMessage(final Long id)
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        doReturn(String.valueOf(id)).when(header).getMessageId();

        final ServerMessage<?> message = mock(ServerMessage.class);
        doReturn(id).when(message).getMessageNumber();
        doReturn(header).when(message).getMessageHeader();
        doReturn(true).when(message).checkValid();
        doReturn(100L).when(message).getSizeIncludingHeader();
        doReturn(System.currentTimeMillis()).when(message).getArrivalTime();

        final StoredMessage storedMessage = mock(StoredMessage.class);
        doReturn(storedMessage).when(message).getStoredMessage();

        final MessageReference ref = mock(MessageReference.class);
        doReturn(message).when(ref).getMessage();
        doReturn(ref).when(message).newReference();
        doReturn(ref).when(message).newReference(any(TransactionLogResource.class));

        return message;
    }
}
