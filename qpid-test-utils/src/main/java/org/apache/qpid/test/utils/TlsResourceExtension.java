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

import org.apache.qpid.test.utils.tls.TlsResource;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.reflect.Method;

/**
 * JUnit extension allowing to inject {@link TlsResource} as test method parameter. Created {@link TlsResource} instance
 * is stored in {@link ExtensionContext.Store} and closed automatically as it implements {@link AutoCloseable}.
 * To enable extension on test class add annotation on the class level:
 * <pre> {@code @ExtendWith({ TlsResourceExtension.class }) } </pre>
 * To inject {@link TlsResource} into the test method pass it as a method parameter:
 * <pre> {@code
 * void test(final TlsResource tls)
 * {
 *
 * } } </pre>
 * Using this scenario new {@link TlsResource} instance will be created before test method invocation and will be closed
 * after test method invocation.
 * Though it's possible to inject {@link TlsResource} instance into the methods annotated as {@link BeforeAll} or {@link BeforeEach},
 * but such {@link TlsResource} instances will be closed after all test method invocations in the end of the test class
 * lifecycle. Generally it's not recommended to inject {@link TlsResource} instances into {@link BeforeAll} or
 * {@link BeforeEach} methods.
 */
public class TlsResourceExtension implements ParameterResolver
{
    /**
     * Returns true when test method argument is of type {@link TlsResource}
     * @param parameterContext the context for the parameter for which an argument should
     * be resolved; never {@code null}
     * @param extensionContext the extension context for the {@code Executable}
     * about to be invoked; never {@code null}
     * @return true when method parameter is a {@link TlsResource} instance, false otherwise
     * @throws ParameterResolutionException When failed to resolve method parameter
     */
    @Override
    public boolean supportsParameter(final @NonNull ParameterContext parameterContext,
                                     final @NonNull ExtensionContext extensionContext) throws ParameterResolutionException
    {
        return TlsResource.class.equals(parameterContext.getParameter().getType());
    }

    /**
     * Retrieves {@link TlsResource} from the {@link ExtensionContext.Store} or creates it when absent
     * @param parameterContext {@link ParameterContext} instance
     * @param extensionContext {@link ExtensionContext} instance
     * @return {@link TlsResource} instance
     * @throws ParameterResolutionException When failed to resolve method parameter
     */
    @Override
    public Object resolveParameter(final @NonNull ParameterContext parameterContext,
                                   final @NonNull ExtensionContext extensionContext) throws ParameterResolutionException
    {
        final var store = store(parameterContext, extensionContext);
        return store.computeIfAbsent(TlsResource.class, key -> new TlsResource(), TlsResource.class);
    }

    /**
     * Creates {@link ExtensionContext.Store} from {@link ExtensionContext} instance
     * @param context {@link ExtensionContext} instance
     * @return {@link ExtensionContext.Store} instance
     */
    private ExtensionContext.Store store(final @NonNull ParameterContext parameterContext,
                                         final @NonNull ExtensionContext context)
    {
        final var className = context.getRequiredTestClass().getCanonicalName();
        final var methodName = context.getTestMethod().map(Method::getName).orElse("");
        final var index = parameterContext.getIndex();
        final var key = "%s%s%d".formatted(className, (methodName.isEmpty() ? "" : "#" + methodName), index);
        final var namespace = ExtensionContext.Namespace.create(key);
        return context.getStore(ExtensionContext.StoreScope.EXTENSION_CONTEXT, namespace);
    }
}
