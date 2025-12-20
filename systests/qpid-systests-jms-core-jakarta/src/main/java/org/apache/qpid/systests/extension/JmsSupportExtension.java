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

package org.apache.qpid.systests.extension;

import org.apache.qpid.systests.AmqpManagementFacade;
import org.apache.qpid.systests.JmsProvider;
import org.apache.qpid.systests.QpidJmsClientProvider;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminExtension;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.reflect.Method;

public class JmsSupportExtension implements ParameterResolver
{
    @Override
    public boolean supportsParameter(@NonNull ParameterContext parameterContext,
                                     @NonNull ExtensionContext extensionContext)
            throws ParameterResolutionException
    {
        return JmsSupport.class.equals(parameterContext.getParameter().getType());
    }

    @Override
    public @Nullable Object resolveParameter(@NonNull ParameterContext parameterContext,
                                             @NonNull ExtensionContext extensionContext)
            throws ParameterResolutionException
    {
        return jmsSupport(extensionContext);
    }

    private JmsSupport jmsSupport(final @NonNull ExtensionContext extensionContext)
    {
        final var className = extensionContext.getRequiredTestClass().getCanonicalName();
        final var testName = extensionContext.getTestMethod().map(Method::getName).orElse("");
        final var amqpManagementFacade = amqpManagementFacade(extensionContext);
        final var brokerAdmin = brokerAdmin(extensionContext);
        final var jmsProvider = jmsProvider(extensionContext);
        return new JmsSupport(amqpManagementFacade, brokerAdmin, jmsProvider, className, testName);
    }

    private BrokerAdmin brokerAdmin(final @NonNull ExtensionContext extensionContext)
    {
        final var className = extensionContext.getRequiredTestClass().getCanonicalName();
        final var namespace = ExtensionContext.Namespace.create(BrokerAdminExtension.class, className);
        final var store = extensionContext.getStore(ExtensionContext.StoreScope.EXTENSION_CONTEXT, namespace);
        final var result = store.get(BrokerAdmin.class, BrokerAdmin.class);
        if (result == null)
        {
            throw new ParameterResolutionException("Could not resolve BrokerAdmin instance from the extension context store");
        }
        return result;
    }

    private JmsProvider jmsProvider(final @NonNull ExtensionContext extensionContext)
    {
        final var store = store(extensionContext);
        return store.computeIfAbsent(JmsProvider.class, key ->
                new QpidJmsClientProvider(amqpManagementFacade(extensionContext)), JmsProvider.class);
    }

    private AmqpManagementFacade amqpManagementFacade(final @NonNull ExtensionContext extensionContext)
    {
        final var store = store(extensionContext);
        return store.computeIfAbsent(AmqpManagementFacade.class,
                key -> new AmqpManagementFacade(),
                AmqpManagementFacade.class);
    }

    protected ExtensionContext.Store store(final @NonNull ExtensionContext context)
    {
        final var className = context.getRequiredTestClass().getCanonicalName();
        final var namespace = ExtensionContext.Namespace.create(JmsSupportExtension.class, className);
        return context.getStore(ExtensionContext.StoreScope.EXTENSION_CONTEXT, namespace);
    }
}
