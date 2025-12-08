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
*/

package org.apache.qpid.test.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

/**
 * Functional tests for {@link QpidUnitTestExtension} after switching the sifting key from
 * Logback LoggerContext properties to SLF4J MDC.
 * <br>
 * These tests validate the observable behavior: where log lines end up on disk.
 */
public class QpidUnitTestExtensionMdcLoggingTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidUnitTestExtensionMdcLoggingTest.class);

    /**
     * Use a dedicated directory to avoid interfering with other modules/tests.
     * We set this system property early (static init) to influence Logback configuration.
     */
    private static final Path OUTPUT_DIR = createAndBindOutputDir();

    /**
     * This helper extension runs after {@link QpidUnitTestExtension} (because AfterEach/AfterAll callbacks run
     * in reverse registration order). It validates that QpidUnitTestExtension resets/cleans MDC properly and
     * that the reset affects where log lines are written.
     */
    @RegisterExtension
    static final MdcPostConditionsExtension POST_CONDITIONS = new MdcPostConditionsExtension(OUTPUT_DIR);

    /**
     * The extension under test. Registered after POST_CONDITIONS so that POST_CONDITIONS can observe
     * its AfterEach / AfterAll effects.
     */
    @RegisterExtension
    static final QpidUnitTestExtension QPID = new QpidUnitTestExtension();

    @BeforeAll
    static void reconfigureLogbackOnce() throws Exception
    {
        // Ensure Logback uses our output directory and the bundled logback.xml.
        // This makes the tests resilient if the logging system was initialized earlier.
        resetAndReconfigureLogbackFromClasspath("logback.xml");
    }

    @Test
    void routesLogsToPerTestFileAndMdcIsSet(final TestInfo testInfo) throws Exception
    {
        final String methodName = testInfo.getTestMethod().orElseThrow().getName();
        final String expectedKey = getClass().getName() + "#" + methodName;

        // The extension is expected to set the MDC key before the test body runs.
        assertEquals(expectedKey, MDC.get(QpidUnitTestExtension.CLASS_QUALIFIED_TEST_NAME),
                "MDC must contain the per-test key set by QpidUnitTestExtension.beforeEach");

        final String marker = "PER_TEST_MARKER_" + UUID.randomUUID();
        LOGGER.info(marker);

        final Path perTestFile = OUTPUT_DIR.resolve("TEST-" + expectedKey + ".txt");
        awaitFileContains(perTestFile, marker, Duration.ofSeconds(5));
    }

    @Test
    void logsFromThreadWithoutMdcRouteToDefaultFile(final TestInfo testInfo) throws Exception
    {
        final String methodName = testInfo.getTestMethod().orElseThrow().getName();
        final String perTestKey = getClass().getName() + "." + methodName;

        final String marker = "NO_MDC_THREAD_MARKER_" + UUID.randomUUID();

        final Thread t = new Thread(() -> LOGGER.info(marker), "no-mdc-thread");
        t.start();
        t.join(TimeUnit.SECONDS.toMillis(10));

        final Path defaultFile = OUTPUT_DIR.resolve("TEST-testrun.txt");
        awaitFileContains(defaultFile, marker, Duration.ofSeconds(5));

        // The marker must not appear in the per-test file if MDC is not propagated.
        final Path perTestFile = OUTPUT_DIR.resolve("TEST-" + perTestKey + ".txt");
        final String perTestContent = readFileIfExists(perTestFile);
        assertFalse(perTestContent.contains(marker),
                "A log line from a thread without MDC must not be routed to the per-test file");
    }

    @Test
    void logsFromThreadWithPropagatedMdcRouteToPerTestFile(final TestInfo testInfo) throws Exception
    {
        final String methodName = testInfo.getTestMethod().orElseThrow().getName();
        final String perTestKey = getClass().getName() + "#" + methodName;

        final String marker = "PROPAGATED_MDC_THREAD_MARKER_" + UUID.randomUUID();

        // Capture MDC from the test thread (should contain the per-test key).
        final Map<String, String> contextMap = MDC.getCopyOfContextMap();
        assertNotNull(contextMap, "Test thread MDC must not be null when QpidUnitTestExtension is active");

        final Thread t = new Thread(() ->
        {
            // Explicit propagation: emulate what a task decorator / executor wrapper would do.
            MDC.setContextMap(contextMap);
            try
            {
                LOGGER.info(marker);
            }
            finally
            {
                MDC.clear();
            }
        }, "propagated-mdc-thread");

        t.start();
        t.join(TimeUnit.SECONDS.toMillis(10));

        final Path perTestFile = OUTPUT_DIR.resolve("TEST-" + perTestKey + ".txt");
        awaitFileContains(perTestFile, marker, Duration.ofSeconds(5));

        // Also ensure it did not go to the default file.
        final Path defaultFile = OUTPUT_DIR.resolve("TEST-testrun.txt");
        final String defaultContent = readFileIfExists(defaultFile);
        assertFalse(defaultContent.contains(marker),
                "A log line from a thread with propagated MDC must not be routed to the default file");
    }

    private static Path createAndBindOutputDir()
    {
        try
        {
            final Path dir = Files.createTempDirectory("qpid-test-utils-mdc-logs-");
            System.setProperty("test.output.dir", dir.toString());
            return dir;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to create temporary test output directory", e);
        }
    }

    private static void resetAndReconfigureLogbackFromClasspath(final String resourceName) throws JoranException
    {
        final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();

        final URL configUrl = QpidUnitTestExtensionMdcLoggingTest.class.getClassLoader().getResource(resourceName);
        assertNotNull(configUrl, "Unable to find " + resourceName + " on the classpath");

        final JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        configurator.doConfigure(configUrl);
    }

    private static String readFileIfExists(final Path path) throws IOException
    {
        if (!Files.exists(path))
        {
            return "";
        }
        return Files.readString(path, StandardCharsets.UTF_8);
    }

    private static void awaitFileContains(final Path file, final String token, final Duration timeout) throws IOException, InterruptedException
    {
        final long deadlineNanos = System.nanoTime() + timeout.toNanos();
        String lastContent = "";

        while (System.nanoTime() < deadlineNanos)
        {
            if (Files.exists(file))
            {
                lastContent = Files.readString(file, StandardCharsets.UTF_8);
                if (lastContent.contains(token))
                {
                    return;
                }
            }
            Thread.sleep(25);
        }

        fail("Expected file '" + file + "' to contain token '" + token + "' but it did not. "
                + "Last observed content was:\\n" + lastContent);
    }

    /**
     * Assertions that must hold after each test and after all tests.
     * <br>
     * This extension is designed to observe the effects of {@link QpidUnitTestExtension} (MDC reset/cleanup).
     */
    static final class MdcPostConditionsExtension implements AfterEachCallback, AfterAllCallback
    {
        private final Logger logger = LoggerFactory.getLogger(MdcPostConditionsExtension.class);
        private final Path outputDir;

        MdcPostConditionsExtension(final Path outputDir)
        {
            this.outputDir = outputDir;
        }

        @Override
        public void afterEach(final ExtensionContext context) throws Exception
        {
            final String className = context.getRequiredTestClass().getName();
            final String methodName = context.getRequiredTestMethod().getName();

            // QpidUnitTestExtension.afterEach is expected to reset MDC back to the class-level key.
            assertEquals(className, MDC.get(QpidUnitTestExtension.CLASS_QUALIFIED_TEST_NAME),
                    "After each test, MDC must be reset to the class-level key");

            // Emit a marker after the reset and ensure it lands in the class-level file (not the method file).
            final String marker = "POST_AFTER_EACH_MARKER_" + methodName + "_" + UUID.randomUUID();
            logger.info(marker);

            final Path classFile = outputDir.resolve("TEST-" + className + ".txt");
            awaitFileContains(classFile, marker, Duration.ofSeconds(5));

            final Path perTestFile = outputDir.resolve("TEST-" + className + "." + methodName + ".txt");
            final String perTestContent = readFileIfExists(perTestFile);
            assertFalse(perTestContent.contains(marker),
                    "A post-afterEach marker must not be routed to the per-test file");
        }

        @Override
        public void afterAll(final ExtensionContext context)
        {
            // QpidUnitTestExtension.afterAll is expected to remove the MDC key entirely.
            assertNull(MDC.get(QpidUnitTestExtension.CLASS_QUALIFIED_TEST_NAME),
                    "After all tests, MDC must be cleared by QpidUnitTestExtension.afterAll");
        }
    }
}