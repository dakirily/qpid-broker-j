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
package org.apache.qpid.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Characterizes the real standalone {@link Main} process rather than an overridden or in-process launcher.
 */
public class StandaloneProcessBaselineTest extends UnitTestBase
{
    private static final String STARTUP_MESSAGE = "BRK-1001 : Startup";
    private static final String READY_MESSAGE = "BRK-1004 : Qpid Broker Ready";
    private static final String STOPPED_MESSAGE = "BRK-1005 : Stopped";
    private static final String CONFIG_STORE_FILE = "config.json";
    private static final String BROKER_LOG_FILE = "broker.log";

    @Test
    public void testVersionReportsFullProtocolSetAndExitsNormally() throws Exception
    {
        final Path root = Files.createTempDirectory("qpid-standalone-version-");
        try (StandaloneBrokerProcess process =
                     StandaloneBrokerProcess.start(root.resolve("process.log"),
                                                   List.of(),
                                                   Map.of(),
                                                   List.of("--version")))
        {
            assertEquals(0, process.waitForExit());
            assertTrue(process.getOutput().contains(
                    "AMQP version(s) [major.minor]: 0-8, 0-9, 0-9-1, 0-10, 1.0"));
        }
        finally
        {
            FileUtils.delete(root.toFile(), true);
        }
    }

    @Test
    public void testStartupFailureIsReportedAndExitsWithFailure() throws Exception
    {
        final Path root = Files.createTempDirectory("qpid-standalone-failure-");
        try (StandaloneBrokerProcess process =
                     StandaloneBrokerProcess.start(root.resolve("process.log"),
                                                   List.of(),
                                                   Map.of(),
                                                   List.of("--store-type", "missing-store-type")))
        {
            assertEquals(1, process.waitForExit());
            final String output = process.getOutput();
            assertTrue(output.contains("Exception during startup"));
            assertTrue(output.contains("Unknown config store type 'missing-store-type'"));
        }
        finally
        {
            FileUtils.delete(root.toFile(), true);
        }
    }

    @Test
    public void testStartupLoggingAndTerminationSignal() throws Exception
    {
        final Path root = Files.createTempDirectory("qpid-standalone-lifecycle-");
        final Path workDirectory = root.resolve("work");
        Files.createDirectories(workDirectory);
        final Path initialConfiguration = writeInitialConfiguration(root);
        final List<String> arguments =
                List.of("--initial-config-path", initialConfiguration.toString(),
                        "--store-type", "Memory",
                        "--config-property", "qpid.work_dir=" + workDirectory);
        try (StandaloneBrokerProcess process =
                     StandaloneBrokerProcess.start(root.resolve("process.log"),
                                                   List.of(),
                                                   Map.of(),
                                                   arguments))
        {
            process.awaitOutput(READY_MESSAGE);
            assertTrue(process.getOutput().contains(STARTUP_MESSAGE));

            assertTerminationExitCode(process.terminate());
            if (!isWindows())
            {
                final String brokerLog = Files.readString(workDirectory.resolve(BROKER_LOG_FILE),
                                                          StandardCharsets.UTF_8);
                assertTrue(brokerLog.contains(STOPPED_MESSAGE),
                           () -> "Missing stopped message in broker log:" +
                                 System.lineSeparator() + brokerLog);
            }
        }
        finally
        {
            FileUtils.delete(root.toFile(), true);
        }
    }

    @Test
    public void testCliJvmPropertiesFileAndEnvironmentPrecedence() throws Exception
    {
        final Path root = Files.createTempDirectory("qpid-standalone-precedence-");
        try
        {
            assertWorkDirectorySelection(root.resolve("cli"), PrecedenceSource.CLI);
            assertWorkDirectorySelection(root.resolve("jvm"), PrecedenceSource.JVM);
            assertWorkDirectorySelection(root.resolve("properties-file"), PrecedenceSource.PROPERTIES_FILE);
            assertWorkDirectorySelection(root.resolve("environment"), PrecedenceSource.ENVIRONMENT);
        }
        finally
        {
            FileUtils.delete(root.toFile(), true);
        }
    }

    private static void assertWorkDirectorySelection(final Path root,
                                                     final PrecedenceSource expectedSource) throws Exception
    {
        Files.createDirectories(root);
        final Path initialConfiguration = writeInitialConfiguration(root);
        final Path cliWork = root.resolve("cli-work");
        final Path jvmWork = root.resolve("jvm-work");
        final Path propertiesFileWork = root.resolve("properties-file-work");
        final Path environmentWork = root.resolve("environment-work");
        final List<Path> workDirectories =
                List.of(cliWork, jvmWork, propertiesFileWork, environmentWork);
        for (final Path workDirectory : workDirectories)
        {
            Files.createDirectories(workDirectory);
        }

        final List<String> jvmArguments = new ArrayList<>();
        final List<String> mainArguments =
                new ArrayList<>(List.of("--initial-config-path", initialConfiguration.toString()));
        if (expectedSource != PrecedenceSource.ENVIRONMENT)
        {
            final Path propertiesFile = root.resolve("system.properties");
            final Properties properties = new Properties();
            properties.setProperty("QPID_WORK", propertiesFileWork.toString());
            try (OutputStream output = Files.newOutputStream(propertiesFile))
            {
                properties.store(output, null);
            }
            mainArguments.add("--system-properties-file");
            mainArguments.add(propertiesFile.toString());
        }
        if (expectedSource == PrecedenceSource.CLI || expectedSource == PrecedenceSource.JVM)
        {
            jvmArguments.add("-DQPID_WORK=" + jvmWork);
        }
        if (expectedSource == PrecedenceSource.CLI)
        {
            mainArguments.add("--config-property");
            mainArguments.add("qpid.work_dir=" + cliWork);
        }

        final Map<String, String> environment = Map.of("QPID_WORK", environmentWork.toString());
        try (StandaloneBrokerProcess process =
                     StandaloneBrokerProcess.start(root.resolve("process.log"),
                                                   jvmArguments,
                                                   environment,
                                                   mainArguments))
        {
            process.awaitOutput(READY_MESSAGE);
            final Path expectedWorkDirectory = expectedSource.select(cliWork, jvmWork,
                                                                     propertiesFileWork, environmentWork);
            assertTrue(Files.isRegularFile(expectedWorkDirectory.resolve(CONFIG_STORE_FILE)),
                       () -> "Expected configuration store in " + expectedWorkDirectory +
                             System.lineSeparator() + processOutput(process));
            for (final Path workDirectory : workDirectories)
            {
                if (!workDirectory.equals(expectedWorkDirectory))
                {
                    assertFalse(Files.exists(workDirectory.resolve(CONFIG_STORE_FILE)),
                                () -> "Unexpected configuration store in " + workDirectory);
                }
            }
            assertTerminationExitCode(process.terminate());
        }
    }

    private static Path writeInitialConfiguration(final Path root) throws Exception
    {
        final Path initialConfiguration = root.resolve("initial-config.json");
        final String configuration =
                "{\"name\":\"standalone-baseline\",\"modelVersion\":\"" + BrokerModel.MODEL_VERSION + "\"," +
                "\"brokerloggers\":[{\"name\":\"baseline-log\",\"type\":\"File\"," +
                "\"fileName\":\"${qpid.work_dir}${file.separator}" + BROKER_LOG_FILE + "\"," +
                "\"brokerloginclusionrules\":[{\"name\":\"Operational\",\"type\":\"NameAndLevel\"," +
                "\"level\":\"INFO\",\"loggerName\":\"qpid.message.*\"}]}]}";
        Files.writeString(initialConfiguration, configuration, StandardCharsets.UTF_8);
        return initialConfiguration;
    }

    private static String processOutput(final StandaloneBrokerProcess process)
    {
        try
        {
            return process.getOutput();
        }
        catch (final Exception e)
        {
            return "Unable to read process output: " + e;
        }
    }

    private static void assertTerminationExitCode(final int exitCode)
    {
        assertEquals(isWindows() ? 1 : 143, exitCode);
    }

    private static boolean isWindows()
    {
        return System.getProperty("os.name").startsWith("Windows");
    }

    private enum PrecedenceSource
    {
        CLI,
        JVM,
        PROPERTIES_FILE,
        ENVIRONMENT;

        Path select(final Path cliWork,
                    final Path jvmWork,
                    final Path propertiesFileWork,
                    final Path environmentWork)
        {
            if (this == CLI)
            {
                return cliWork;
            }
            else if (this == JVM)
            {
                return jvmWork;
            }
            else if (this == PROPERTIES_FILE)
            {
                return propertiesFileWork;
            }
            else
            {
                return environmentWork;
            }
        }
    }
}
