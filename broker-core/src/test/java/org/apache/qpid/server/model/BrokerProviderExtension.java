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

import java.lang.reflect.Modifier;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.apache.commons.lang3.stream.Streams;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.platform.commons.support.AnnotationSupport;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.virtualhost.AbstractVirtualHost;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhost.TestMemoryVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class BrokerProviderExtension implements AfterAllCallback, BeforeAllCallback, TestInstancePostProcessor
{
    private static final Principal PRINCIPAL = () -> "TEST";
    private static final Subject SYSTEM_SUBJECT = new Subject(true, Set.of(PRINCIPAL), Set.of(), Set.of());

    private TaskExecutor _taskExecutor;
    private Broker _broker;

    private final List<VirtualHost> _virtualHosts = new ArrayList<>();

    @Override
    public void beforeAll(final ExtensionContext ctx) throws Exception
    {
        _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();
        _broker = createBrokerMock(createAccessControlMock());
        AnnotationSupport.findAnnotatedFields(ctx.getRequiredTestClass(), ProvidedMock.class)
                .stream()
                .filter(field -> Modifier.isStatic(field.getModifiers()))
                .forEach(field ->
                {
                    try
                    {
                        field.setAccessible(true);
                        if (field.getType().equals(Broker.class))
                        {
                            field.set(null, _broker);
                        }
                        else if (field.getType().equals(ConfiguredObjectFactory.class))
                        {
                            field.set(null, _broker.getObjectFactory());
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new RuntimeException(ex);
                    }
                });
    }

    @Override
    public void postProcessTestInstance(final Object testInstance, final ExtensionContext ctx)
    {
        final String className = ctx.getRequiredTestClass().getName();
        AnnotationSupport.findAnnotatedFields(ctx.getRequiredTestClass(), ProvidedMock.class)
            .forEach(field ->
            {
                try
                {
                    field.setAccessible(true);
                    if (field.getType().equals(Broker.class))
                    {
                        field.set(testInstance, _broker);
                    }
                    else if (field.getType().equals(ConfiguredObjectFactory.class))
                    {
                        field.set(testInstance, _broker.getObjectFactory());
                    }
                    else if (VirtualHostNode.class.isAssignableFrom(field.getType()))
                    {
                        final ProvidedMock providedMock = field.getAnnotation(ProvidedMock.class);
                        final Map<String, String> providedAttributes = Streams.of(providedMock.attributes()).collect(Collectors.toMap(Attribute::name, Attribute::value));
                        String name = providedAttributes.getOrDefault(VirtualHostNode.NAME, className);
                        boolean isDefault = Boolean.parseBoolean(providedAttributes.getOrDefault(VirtualHostNode.DEFAULT_VIRTUAL_HOST_NODE, "false"));
                        final VirtualHostNode<?> virtualHostNode = createVirtualHostNodeMock(name, isDefault, createAccessControlMock(), _broker);
                        field.set(testInstance, virtualHostNode);
                    }
                    else if (VirtualHost.class.isAssignableFrom(field.getType()))
                    {
                        final ProvidedMock providedMock = field.getAnnotation(ProvidedMock.class);
                        final Map<String, String> providedAttributes = Streams.of(providedMock.attributes()).collect(Collectors.toMap(Attribute::name, Attribute::value));
                        final Map<String,Object> attributes = new HashMap<>();
                        attributes.put(org.apache.qpid.server.model.VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE);
                        attributes.put(org.apache.qpid.server.model.VirtualHost.NAME, className);
                        attributes.putAll(providedAttributes);
                        final VirtualHost<?> virtualHost = createVirtualHost(attributes, _broker, false, createAccessControlMock(), (UnitTestBase) testInstance);
                        _virtualHosts.add(virtualHost);
                        field.set(testInstance, virtualHost);
                    }
                }
                catch (Exception ex)
                {
                    throw new RuntimeException(ex);
                }
            });
    }

    @Override
    public void afterAll(final ExtensionContext context)
    {
        if (_taskExecutor != null)
        {
            _taskExecutor.stop();
        }

        _virtualHosts.forEach(VirtualHost::close);
    }

    public Broker<?> getBroker()
    {
        return _broker;
    }

    private AccessControl createAccessControlMock()
    {
        final AccessControl accessControl = mock(AccessControl.class);
        doReturn(Result.DEFER).when(accessControl).authorise(any(SecurityToken.class), any(Operation.class), any(PermissionedObject.class));
        doReturn(Result.DEFER).when(accessControl).authorise(isNull(), any(Operation.class), any(PermissionedObject.class));
        doReturn(Result.DEFER).when(accessControl).authorise(any(SecurityToken.class), any(Operation.class), any(PermissionedObject.class), any(
                Map.class));
        doReturn(Result.DEFER).when(accessControl).authorise(isNull(), any(Operation.class), any(PermissionedObject.class), any(Map.class));
        doReturn(Result.ALLOWED).when(accessControl).getDefault();
        return accessControl;
    }

    private Broker<?> createBrokerMock(final AccessControl accessControl)
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
        doReturn(_taskExecutor).when(broker).getTaskExecutor();
        doReturn(_taskExecutor).when(broker).getChildExecutor();
        doReturn(_taskExecutor).when(systemConfig).getTaskExecutor();
        doReturn(_taskExecutor).when(systemConfig).getChildExecutor();
        doReturn(mock(PreferenceStore.class)).when(systemConfig).createPreferenceStore();
        doReturn(Set.of(broker)).when(systemConfig).getChildren(Broker.class);

        return broker;
    }

    private QueueManagingVirtualHost<?> createVirtualHost(final String name,
                                                                 final Broker broker,
                                                                 final boolean defaultVHN,
                                                                 final AccessControl accessControl,
                                                                 final UnitTestBase testBase)
    {
        final Map<String,Object> attributes = Map.of(
                org.apache.qpid.server.model.VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE,
                org.apache.qpid.server.model.VirtualHost.NAME, name);

        return createVirtualHost(attributes, broker, defaultVHN, accessControl, testBase);
    }

    private QueueManagingVirtualHost<?> createVirtualHost(final Map<String, Object> attributes,
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
                .create(VirtualHost.class, attributes, virtualHostNode);
        host.start();
        doReturn(host).when(virtualHostNode).getVirtualHost();

        return host;
    }

    public VirtualHostNode createVirtualHostNodeMock(final String virtualHostNodeName,
                                                            final boolean defaultVHN,
                                                            final AccessControl accessControl,
                                                            final Broker<?> broker)
    {
        final ConfiguredObjectFactory objectFactory = broker.getObjectFactory();

        final VirtualHostNode virtualHostNode = mockWithSystemPrincipalAndAccessControl(VirtualHostNode.class,
                PRINCIPAL, accessControl);
        doReturn(virtualHostNodeName).when(virtualHostNode).getName();
        doReturn(_taskExecutor).when(virtualHostNode).getTaskExecutor();
        doReturn(_taskExecutor).when(virtualHostNode).getChildExecutor();
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
        doReturn(_taskExecutor).when(virtualHostNode).getTaskExecutor();
        doReturn(_taskExecutor).when(virtualHostNode).getChildExecutor();
        doReturn(mock(PreferenceStore.class)).when(virtualHostNode).createPreferenceStore();
        return virtualHostNode;
    }

    private  <X extends ConfiguredObject> X mockWithSystemPrincipalAndAccessControl(final Class<X> clazz,
                                                                                    final Principal principal,
                                                                                    final AccessControl accessControl)
    {
        final X mock = mock(clazz, withSettings()
                .extraInterfaces(BrokerTestHelper.TestableSystemPrincipalSource.class, BrokerTestHelper.TestableAccessControlSource.class));
        doReturn(principal).when((SystemPrincipalSource) mock).getSystemPrincipal();
        doReturn(accessControl).when((AccessControlSource) mock).getAccessControl();
        return mock;
    }
}
