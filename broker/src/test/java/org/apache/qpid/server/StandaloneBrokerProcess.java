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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

final class StandaloneBrokerProcess implements AutoCloseable
{
    private static final long PROCESS_TIMEOUT_SECONDS = 120L;
    private static final long OUTPUT_POLL_MILLIS = 25L;

    private final Process _process;
    private final Path _outputFile;

    private StandaloneBrokerProcess(final Process process, final Path outputFile)
    {
        _process = process;
        _outputFile = outputFile;
    }

    static StandaloneBrokerProcess start(final Path outputFile,
                                         final List<String> jvmArguments,
                                         final Map<String, String> environment,
                                         final List<String> mainArguments) throws IOException
    {
        final String executableName =
                System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        final Path javaExecutable = Path.of(System.getProperty("java.home"), "bin", executableName);
        final List<String> command = new ArrayList<>();
        command.add(javaExecutable.toString());
        command.addAll(jvmArguments);
        command.add("-cp");
        command.add(standaloneClassPath());
        command.add(Main.class.getName());
        command.addAll(mainArguments);

        final Path parent = outputFile.toAbsolutePath().getParent();
        if (parent != null)
        {
            Files.createDirectories(parent);
        }

        final ProcessBuilder processBuilder = new ProcessBuilder(command);
        if (parent != null)
        {
            processBuilder.directory(parent.toFile());
        }
        final Map<String, String> processEnvironment = processBuilder.environment();
        processEnvironment.remove("QPID_HOME");
        processEnvironment.remove("QPID_WORK");
        processEnvironment.remove("qpid.home_dir");
        processEnvironment.remove("qpid.work_dir");
        processEnvironment.putAll(environment);
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(outputFile.toFile());
        return new StandaloneBrokerProcess(processBuilder.start(), outputFile);
    }

    private static String standaloneClassPath()
    {
        final String classPath = System.getProperty("surefire.test.class.path",
                                                    System.getProperty("java.class.path"));
        return Arrays.stream(classPath.split(Pattern.quote(File.pathSeparator)))
                .filter(entry -> !entry.contains("target" + File.separator + "test-classes"))
                .filter(entry -> !entry.contains("qpid-test-utils"))
                .collect(Collectors.joining(File.pathSeparator));
    }

    int waitForExit() throws Exception
    {
        if (!_process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS))
        {
            _process.destroyForcibly();
            throw new AssertionError("Standalone broker did not exit within " +
                                     PROCESS_TIMEOUT_SECONDS + " seconds" +
                                     System.lineSeparator() + getOutput());
        }
        return _process.exitValue();
    }

    void awaitOutput(final String expected) throws Exception
    {
        final long deadline = System.nanoTime() +
                TimeUnit.SECONDS.toNanos(PROCESS_TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline)
        {
            final String output = getOutput();
            if (output.contains(expected))
            {
                return;
            }
            if (!_process.isAlive())
            {
                throw new AssertionError("Standalone broker exited with " + _process.exitValue() +
                                         " before producing '" + expected + "'" +
                                         System.lineSeparator() + output);
            }
            Thread.sleep(OUTPUT_POLL_MILLIS);
        }
        throw new AssertionError("Standalone broker did not produce '" + expected + "' within " +
                                 PROCESS_TIMEOUT_SECONDS + " seconds" +
                                 System.lineSeparator() + getOutput());
    }

    int terminate() throws Exception
    {
        _process.destroy();
        return waitForExit();
    }

    String getOutput() throws IOException
    {
        return Files.exists(_outputFile)
                ? Files.readString(_outputFile, StandardCharsets.UTF_8)
                : "";
    }

    @Override
    public void close() throws Exception
    {
        if (_process.isAlive())
        {
            _process.destroy();
            if (!_process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS))
            {
                _process.destroyForcibly();
                _process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }
    }
}
