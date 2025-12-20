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

package org.apache.qpid.tests.utils;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.lang.reflect.Method;

public class BrokerAdminExtension implements InvocationInterceptor, ParameterResolver, BeforeAllCallback, BeforeEachCallback, AfterAllCallback
{
    private static final String BROKER_ADMIN_TYPE_PROPERTY_NAME = "qpid.tests.brokerAdminType";

    private static final ThreadLocal<Boolean> BEFORE_EACH_CALLBACK_CALLED = ThreadLocal.withInitial(() -> Boolean.FALSE);

    @Override
    public boolean supportsParameter(final @NonNull ParameterContext parameterContext,
                                     final @NonNull ExtensionContext extensionContext)
            throws ParameterResolutionException
    {
        return BrokerAdmin.class.isAssignableFrom(parameterContext.getParameter().getType());
    }

    @Override
    public @Nullable Object resolveParameter(final @NonNull ParameterContext parameterContext,
                                             final @NonNull ExtensionContext extensionContext)
            throws ParameterResolutionException
    {
        return getOrCreateFromStore(extensionContext);
    }

    private BrokerAdmin getOrCreateFromStore(final @NonNull ExtensionContext extensionContext)
    {
        final var store = store(extensionContext);
        return store.computeIfAbsent(BrokerAdmin.class, key -> brokerAdmin(extensionContext), BrokerAdmin.class);
    }

    private BrokerAdmin brokerAdmin(final @NonNull ExtensionContext context)
    {
        final var testClass = context.getRequiredTestClass();
        final RunBrokerAdmin runBrokerAdmin = testClass.getAnnotation(RunBrokerAdmin.class);
        final String type = runBrokerAdmin == null ?
                System.getProperty(BROKER_ADMIN_TYPE_PROPERTY_NAME, EmbeddedBrokerPerRunAdminImpl.TYPE) :
                runBrokerAdmin.type();
        final BrokerAdmin original = new BrokerAdminFactory().createInstance(type);
        return new LoggingBrokerAdminDecorator(original);
    }

    /**
     * Creates {@link ExtensionContext.Store} from {@link ExtensionContext} instance
     * @param context {@link ExtensionContext} instance
     * @return {@link ExtensionContext.Store} instance
     */
    private ExtensionContext.Store store(final @NonNull ExtensionContext context)
    {
        final var className = context.getRequiredTestClass().getCanonicalName();
        final var namespace = ExtensionContext.Namespace.create(BrokerAdminExtension.class, className);
        return context.getStore(ExtensionContext.StoreScope.EXTENSION_CONTEXT, namespace);
    }

    @Override
    public void beforeAll(final @NonNull ExtensionContext extensionContext)
    {
        final var testClass = extensionContext.getRequiredTestClass();
        final var brokerAdmin = getOrCreateFromStore(extensionContext);
        brokerAdmin.beforeTestClass(testClass);
        BEFORE_EACH_CALLBACK_CALLED.set(false);
    }

    @Override
    public void afterAll(final @NonNull ExtensionContext extensionContext)
    {
        final var testClass = extensionContext.getRequiredTestClass();
        final var brokerAdmin = getOrCreateFromStore(extensionContext);
        brokerAdmin.afterTestClass(testClass);
    }

    @Override
    public void interceptBeforeEachMethod(final InvocationInterceptor.@NonNull Invocation<Void> invocation,
                                          final @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                          final @NonNull ExtensionContext extensionContext) throws Throwable
    {
        final var testClass = extensionContext.getRequiredTestClass();
        final var method = extensionContext.getRequiredTestMethod();
        final var brokerAdmin = getOrCreateFromStore(extensionContext);
        if (!BEFORE_EACH_CALLBACK_CALLED.get())
        {
            brokerAdmin.beforeTestMethod(testClass, method);
            BEFORE_EACH_CALLBACK_CALLED.set(true);
        }
        invocation.proceed();
    }

    @Override
    public void beforeEach(final @NonNull ExtensionContext extensionContext)
    {
        if (!BEFORE_EACH_CALLBACK_CALLED.get())
        {
            final var testClass = extensionContext.getRequiredTestClass();
            final var method = extensionContext.getRequiredTestMethod();
            final var brokerAdmin = getOrCreateFromStore(extensionContext);
            brokerAdmin.beforeTestMethod(testClass, method);
            BEFORE_EACH_CALLBACK_CALLED.set(true);
        }
    }

    @Override
    public void interceptTestMethod(final InvocationInterceptor.@NonNull Invocation<Void> invocation,
                                    final @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                    final @NonNull ExtensionContext extensionContext) throws Throwable {
        final var testClass = extensionContext.getRequiredTestClass();
        final var method = extensionContext.getRequiredTestMethod();
        final var brokerSpecific = method.getAnnotation(BrokerSpecific.class) == null
                ? method.getDeclaringClass().getAnnotation(BrokerSpecific.class)
                : method.getAnnotation(BrokerSpecific.class);
        final var brokerAdmin = getOrCreateFromStore(extensionContext);

        if (!BEFORE_EACH_CALLBACK_CALLED.get())
        {
            brokerAdmin.beforeTestMethod(testClass, method);
            BEFORE_EACH_CALLBACK_CALLED.set(true);
        }

        if (brokerSpecific != null && !brokerSpecific.kind().equalsIgnoreCase(brokerAdmin.getKind()))
        {
            // log skipping
            invocation.skip();
        }
        try
        {
            invocation.proceed();
        }
        finally
        {
            brokerAdmin.afterTestMethod(testClass, method);
            BEFORE_EACH_CALLBACK_CALLED.set(false);
        }
    }

    @Override
    public void interceptTestTemplateMethod(@NonNull Invocation<@Nullable Void> invocation,
                                            @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                            @NonNull ExtensionContext extensionContext) throws Throwable
    {
        final var testClass = extensionContext.getRequiredTestClass();
        final var method = extensionContext.getRequiredTestMethod();
        final var brokerSpecific = method.getAnnotation(BrokerSpecific.class) == null
                ? method.getDeclaringClass().getAnnotation(BrokerSpecific.class)
                : method.getAnnotation(BrokerSpecific.class);
        final var brokerAdmin = getOrCreateFromStore(extensionContext);

        if (!BEFORE_EACH_CALLBACK_CALLED.get())
        {
            brokerAdmin.beforeTestMethod(testClass, method);
            BEFORE_EACH_CALLBACK_CALLED.set(true);
        }

        if (brokerSpecific != null && !brokerSpecific.kind().equalsIgnoreCase(brokerAdmin.getKind()))
        {
            // log skipping
            invocation.skip();
        }
        try
        {
            invocation.proceed();
        }
        finally
        {
            brokerAdmin.afterTestMethod(testClass, method);
            BEFORE_EACH_CALLBACK_CALLED.set(false);
        }
    }
}
