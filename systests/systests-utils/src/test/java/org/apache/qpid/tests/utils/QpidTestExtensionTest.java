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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstanceFactoryContext;

import org.apache.qpid.test.utils.UnitTestBase;

public class QpidTestExtensionTest extends UnitTestBase
{
    @Test
    public void constructorFailureCleansUpStartedBroker()
    {
        final BrokerAdminFactory brokerAdminFactory = mock(BrokerAdminFactory.class);
        final BrokerAdmin brokerAdmin = mock(BrokerAdmin.class);
        final BrokerAdminException cleanupFailure = new BrokerAdminException("cleanup failed");
        when(brokerAdminFactory.createInstance("TEST")).thenReturn(brokerAdmin);
        doThrow(cleanupFailure).when(brokerAdmin).afterTestClass(FailingTest.class);

        final TestInstanceFactoryContext factoryContext = mock(TestInstanceFactoryContext.class);
        doReturn(FailingTest.class).when(factoryContext).getTestClass();
        final ExtensionContext extensionContext = mock(ExtensionContext.class);
        when(extensionContext.getTestClass()).thenReturn(Optional.of(FailingTest.class));

        final QpidTestExtension extension = new QpidTestExtension(brokerAdminFactory);
        final RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> extension.createTestInstance(factoryContext, extensionContext));

        verify(brokerAdmin).beforeTestClass(FailingTest.class);
        verify(brokerAdmin).afterTestClass(FailingTest.class);
        assertEquals(1, exception.getSuppressed().length);
        assertSame(cleanupFailure, exception.getSuppressed()[0]);
    }

    @RunBrokerAdmin(type = "TEST")
    public static class FailingTest extends BrokerAdminUsingTestBase
    {
        public FailingTest()
        {
            throw new IllegalStateException("expected constructor failure");
        }
    }
}
