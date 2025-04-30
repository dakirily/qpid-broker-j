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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * JUnit's extension. Logs test method execution start and end
 */
public class QpidUnitTestExtension implements AfterAllCallback, BeforeAllCallback, TestWatcher
{
    /** Logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidUnitTestExtension.class);

    /**
     * Callback executed before all testing methods
     *
     * @param ctx ExtensionContext
     */
    @Override
    public void beforeAll(final ExtensionContext ctx)
    {
        MDC.put(LogbackPropertyValueDiscriminator.CLASS_QUALIFIED_TEST_NAME, ctx.getRequiredTestClass().getName());
    }

    /**
     * Callback executed before after all testing methods
     *
     * @param extensionContext ExtensionContext
     */
    @Override
    public void afterAll(final ExtensionContext extensionContext)
    {
        MDC.remove(LogbackPropertyValueDiscriminator.CLASS_QUALIFIED_TEST_NAME);
    }

    @Override
    public void testSuccessful(final ExtensionContext ctx)
    {
        log(ctx, "PASSED");
    }

    @Override
    public void testFailed(final ExtensionContext ctx, final Throwable throwable)
    {
        log(ctx, "FAILED");
    }

    @Override
    public void testAborted(final ExtensionContext ctx, final Throwable throwable)
    {
        log(ctx, "ABORTED");
    }

    private void log(final ExtensionContext ctx, final String phase)
    {
        LOGGER.info("{} {}", phase, ctx.getDisplayName());
    }
}
