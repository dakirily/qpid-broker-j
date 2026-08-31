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

package org.apache.qpid.tests.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.junit.jupiter.api.extension.TestInstanceFactory;
import org.junit.jupiter.api.extension.TestInstanceFactoryContext;
import org.junit.jupiter.api.extension.TestInstancePreDestroyCallback;
import org.junit.jupiter.api.extension.TestInstantiationException;

import org.apache.qpid.test.utils.TestUtils;

public class QpidTestExtension implements Extension, InvocationInterceptor, TestInstanceFactory,
        TestInstancePreDestroyCallback
{
    private final BrokerAdminFactory _brokerAdminFactory;
    private BrokerAdmin _brokerAdmin;
    private Class<?> _testClass;
    private boolean _testClassStarted;
    private boolean _beforeMethod;

    public QpidTestExtension()
    {
        this(new BrokerAdminFactory());
    }

    QpidTestExtension(final BrokerAdminFactory brokerAdminFactory)
    {
        _brokerAdminFactory = Objects.requireNonNull(brokerAdminFactory);
    }

    public Object createTestInstance(final TestInstanceFactoryContext factoryCtx, final ExtensionContext ctx)
            throws TestInstantiationException
    {
        _testClass = TestUtils.getTestClass(ctx);
        final RunBrokerAdmin runBrokerAdmin = _testClass.getAnnotation(RunBrokerAdmin.class);
        final String type = runBrokerAdmin == null ?
                System.getProperty("qpid.tests.brokerAdminType", EmbeddedBrokerPerClassAdminImpl.TYPE) :
                runBrokerAdmin.type();
        final BrokerAdmin original = _brokerAdminFactory.createInstance(type);
        _brokerAdmin = new LoggingBrokerAdminDecorator(original);
        _brokerAdmin.beforeTestClass(_testClass);
        _testClassStarted = true;
        try
        {
            final BrokerAdminUsingTestBase test =
                    (BrokerAdminUsingTestBase) factoryCtx.getTestClass().getConstructor().newInstance();
            return test.setBrokerAdmin(original);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e)
        {
            throw cleanUpAfterInstantiationFailure(new RuntimeException(
                    "Failed to instantiate test class " + factoryCtx.getTestClass().getSimpleName(), e));
        }
        catch (RuntimeException e)
        {
            throw cleanUpAfterInstantiationFailure(e);
        }
    }

    public void interceptBeforeEachMethod(final InvocationInterceptor.Invocation<Void> invocation,
                                          final ReflectiveInvocationContext<Method> invocationContext,
                                          final ExtensionContext extensionContext) throws Throwable
    {
        if (!_beforeMethod)
        {
            final Method method = TestUtils.getTestMethod(extensionContext);
            _brokerAdmin.beforeTestMethod(_testClass, method);
            _beforeMethod = true;
        }
        invocation.proceed();
    }

    public void preDestroyTestInstance(final ExtensionContext ctx)
    {
        if (_testClassStarted)
        {
            _testClassStarted = false;
            _brokerAdmin.afterTestClass(_testClass);
        }
    }

    public void interceptTestMethod(final InvocationInterceptor.Invocation<Void> invocation,
                                    final ReflectiveInvocationContext<Method> invocationContext,
                                    final ExtensionContext extensionContext) throws Throwable
    {
        final Method method = TestUtils.getTestMethod(extensionContext);
        BrokerSpecific brokerSpecific = method.getAnnotation(BrokerSpecific.class);
        if (brokerSpecific == null)
        {
            brokerSpecific = method.getDeclaringClass().getAnnotation(BrokerSpecific.class);
        }
        if (brokerSpecific != null && !brokerSpecific.kind().equalsIgnoreCase(_brokerAdmin.getKind()))
        {
            // log skipping
            invocation.skip();
        }
        if (!_beforeMethod)
        {
            _brokerAdmin.beforeTestMethod(_testClass, method);
        }
        try
        {
            invocation.proceed();
        }
        finally
        {
            _brokerAdmin.afterTestMethod(_testClass, method);
            _beforeMethod = false;
        }
    }

    private <T extends RuntimeException> T cleanUpAfterInstantiationFailure(final T instantiationFailure)
    {
        _testClassStarted = false;
        try
        {
            _brokerAdmin.afterTestClass(_testClass);
        }
        catch (RuntimeException cleanupFailure)
        {
            instantiationFailure.addSuppressed(cleanupFailure);
        }
        return instantiationFailure;
    }
}
