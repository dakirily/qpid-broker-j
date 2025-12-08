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

package org.apache.qpid.test.utils;

import java.lang.reflect.Method;
import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * JUnit's extension. Logs test method execution start and end
 */
public class QpidUnitTestExtension implements AfterAllCallback, BeforeAllCallback,
        InvocationInterceptor
{
    /** Logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidUnitTestExtension.class);

    /** Class qualified test name */
    public static final String CLASS_QUALIFIED_TEST_NAME = "classQualifiedTestName";

    /**
     * Callback executed before all testing methods
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void beforeAll(final ExtensionContext extensionContext)
    {
        final Class<?> testClass = extensionContext.getRequiredTestClass();
        MDC.put(CLASS_QUALIFIED_TEST_NAME, testClass.getName());
    }

    /**
     * Callback executed before after all testing methods
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void afterAll(@NonNull final ExtensionContext extensionContext)
    {
        MDC.remove(CLASS_QUALIFIED_TEST_NAME);
    }

    /**
     * Callback wraps single testing method
     * @param invocation the invocation that is being intercepted; never {@code null}
     * @param invocationContext the context of the invocation that is being intercepted; never {@code null}
     * @param extensionContext the current extension context; never {@code null}
     * @throws Throwable Exception thrown
     */
    @Override
    public void interceptTestTemplateMethod(@NonNull Invocation<@Nullable Void> invocation,
                                            @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                            @NonNull ExtensionContext extensionContext) throws Throwable

    {
        interceptTestMethod(invocation, invocationContext, extensionContext);
    }

    /**
     * Callback wraps single testing method
     * @param invocation the invocation that is being intercepted; never {@code null}
     * @param invocationContext the context of the invocation that is being intercepted; never {@code null}
     * @param extensionContext the current extension context; never {@code null}
     * @throws Throwable Exception thrown
     */
    @Override
    public void interceptTestMethod(@NonNull Invocation<@Nullable Void> invocation,
                                    @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                    @NonNull ExtensionContext extensionContext) throws Throwable
    {
        final Class<?> testClass = extensionContext.getRequiredTestClass();
        final List<Class<?>> enclosingClasses = extensionContext.getEnclosingTestClasses();
        final StringBuilder stringBuilder = new StringBuilder();

        if (enclosingClasses.isEmpty())
        {
            stringBuilder.append(testClass.getCanonicalName());
        }
        else
        {
            stringBuilder.append(enclosingClasses.get(0).getCanonicalName());
            for (int i = 1; i < enclosingClasses.size(); i ++)
            {
                stringBuilder.append('#').append(enclosingClasses.get(i).getSimpleName());
            }
            stringBuilder.append('#').append(testClass.getSimpleName());
        }

        final String testClassName = stringBuilder.toString();

        final Method testMethod = extensionContext.getRequiredTestMethod();
        stringBuilder.append('#').append(testMethod.getName());

        final ParameterizedTest parameterizedTest = testMethod.getAnnotation(ParameterizedTest.class);
        final RepeatedTest repeatedTest = testMethod.getAnnotation(RepeatedTest.class);

        if (parameterizedTest != null || repeatedTest != null)
        {
            final List<Object> args = invocationContext.getArguments();
            for (final Object arg : args)
            {
                stringBuilder.append('_').append(arg);
            }
        }

        final String fullTestName =  stringBuilder.toString();

        try
        {
            LOGGER.info("========================= executing test : {}", fullTestName);
            MDC.put(CLASS_QUALIFIED_TEST_NAME, fullTestName);
            LOGGER.info("========================= start executing test : {}", fullTestName);

            invocation.proceed();
        }
        finally
        {
            LOGGER.info("========================= stop executing test : {} ", fullTestName);
            MDC.put(CLASS_QUALIFIED_TEST_NAME, testClassName);
            LOGGER.info("========================= cleaning up test environment for test : {}", fullTestName);
        }
    }
}
