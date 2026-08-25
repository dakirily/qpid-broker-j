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
package org.apache.qpid.server.embedded;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import tools.jackson.databind.ObjectMapper;

final class FreshJvmBrokerProbe
{
    private static final long PROBE_TIMEOUT_MINUTES = 3L;

    private FreshJvmBrokerProbe()
    {
    }

    static Map<String, Object> run(final String mode) throws Exception
    {
        final Path resultFile = Files.createTempFile("qpid-embedded-probe-", ".json");
        final Path outputFile = Files.createTempFile("qpid-embedded-probe-", ".log");
        try
        {
            final String executableName =
                    System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
            final Path javaExecutable = Path.of(System.getProperty("java.home"), "bin", executableName);
            final String classPath = System.getProperty("surefire.test.class.path",
                                                        System.getProperty("java.class.path"));
            final List<String> command = new ArrayList<>();
            command.add(javaExecutable.toString());
            command.add("--add-opens=java.base/java.lang=ALL-UNNAMED");
            command.add("-cp");
            command.add(classPath);
            command.add(EmbeddedBrokerIsolationProbe.class.getName());
            command.add(mode);
            command.add(resultFile.toAbsolutePath().toString());

            final Process process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .redirectOutput(outputFile.toFile())
                    .start();
            if (!process.waitFor(PROBE_TIMEOUT_MINUTES, TimeUnit.MINUTES))
            {
                process.destroyForcibly();
                throw new AssertionError("Embedded broker probe did not finish within " +
                                         PROBE_TIMEOUT_MINUTES + " minutes");
            }

            final String processOutput = Files.readString(outputFile, StandardCharsets.UTF_8);
            if (process.exitValue() != 0)
            {
                throw new AssertionError("Embedded broker probe exited with " + process.exitValue() +
                                         System.lineSeparator() + processOutput);
            }
            if (!Files.isRegularFile(resultFile))
            {
                throw new AssertionError("Embedded broker probe produced no result" +
                                         System.lineSeparator() + processOutput);
            }

            @SuppressWarnings("unchecked")
            final Map<String, Object> result =
                    new ObjectMapper().readValue(resultFile.toFile(), Map.class);
            return result;
        }
        finally
        {
            Files.deleteIfExists(resultFile);
            Files.deleteIfExists(outputFile);
        }
    }
}
