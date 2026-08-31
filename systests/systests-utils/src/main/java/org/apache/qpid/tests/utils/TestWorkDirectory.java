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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.TestFileUtils;

/**
 * Creates and removes the work directories used by system-test brokers.
 *
 * The parent directory is resolved for every invocation.  This is important for
 * forked test JVMs, where Surefire can set {@code java.io.tmpdir} after classes
 * from the JDK have already cached its value.
 */
public final class TestWorkDirectory
{
    public static final String WORK_DIRECTORY_ROOT_PROPERTY = "qpid.tests.work_dir";
    public static final String PRESERVE_WORK_DIRECTORY_PROPERTY = "qpid.tests.preserve_work_dir";

    private static final Logger LOGGER = LoggerFactory.getLogger(TestWorkDirectory.class);
    private static final int DELETE_ATTEMPTS = 3;
    private static final long DELETE_RETRY_DELAY_MILLIS = 100L;
    private static final int MAX_REMAINING_PATHS_TO_LOG = 20;

    private TestWorkDirectory()
    {
    }

    public static Path create(final String prefix) throws IOException
    {
        final String configuredParent = System.getProperty(WORK_DIRECTORY_ROOT_PROPERTY,
                                                            System.getProperty("java.io.tmpdir"));
        if (configuredParent == null || configuredParent.isBlank())
        {
            throw new IOException(String.format("Neither '%s' nor 'java.io.tmpdir' specifies a work directory",
                                                WORK_DIRECTORY_ROOT_PROPERTY));
        }

        final Path parent = Path.of(configuredParent).toAbsolutePath().normalize();
        Files.createDirectories(parent);
        final Path workDirectory = Files.createTempDirectory(parent, Objects.requireNonNull(prefix));
        LOGGER.info("Created broker test work directory {}", workDirectory);
        return workDirectory;
    }

    public static void delete(final Path workDirectory)
    {
        if (workDirectory == null || Files.notExists(workDirectory))
        {
            return;
        }

        final Path normalizedWorkDirectory = workDirectory.toAbsolutePath().normalize();
        if (Boolean.getBoolean(PRESERVE_WORK_DIRECTORY_PROPERTY))
        {
            LOGGER.warn("Preserving broker test work directory {} because '{}' is true",
                        normalizedWorkDirectory,
                        PRESERVE_WORK_DIRECTORY_PROPERTY);
            return;
        }

        IOException failure = null;
        for (int attempt = 1; attempt <= DELETE_ATTEMPTS; attempt++)
        {
            try
            {
                TestFileUtils.deleteRecursively(normalizedWorkDirectory);
                LOGGER.info("Deleted broker test work directory {}", normalizedWorkDirectory);
                return;
            }
            catch (IOException e)
            {
                failure = e;
                if (attempt < DELETE_ATTEMPTS)
                {
                    LOGGER.warn("Failed to delete broker test work directory {} on attempt {}/{}; retrying",
                                normalizedWorkDirectory,
                                attempt,
                                DELETE_ATTEMPTS,
                                e);
                    waitBeforeRetry(normalizedWorkDirectory, e);
                }
            }
        }

        final List<String> remainingPaths = describeRemainingPaths(normalizedWorkDirectory);
        LOGGER.error("Failed to delete broker test work directory {} after {} attempts. Remaining paths: {}",
                     normalizedWorkDirectory,
                     DELETE_ATTEMPTS,
                     remainingPaths,
                     failure);
        throw new BrokerAdminException(String.format(
                "Failed to delete broker test work directory '%s'. Remaining paths: %s",
                normalizedWorkDirectory,
                remainingPaths),
                                       failure);
    }

    private static void waitBeforeRetry(final Path workDirectory, final IOException deletionFailure)
    {
        try
        {
            Thread.sleep(DELETE_RETRY_DELAY_MILLIS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            final BrokerAdminException interrupted = new BrokerAdminException(
                    String.format("Interrupted while deleting broker test work directory '%s'", workDirectory),
                    e);
            interrupted.addSuppressed(deletionFailure);
            throw interrupted;
        }
    }

    private static List<String> describeRemainingPaths(final Path workDirectory)
    {
        if (Files.notExists(workDirectory))
        {
            return List.of();
        }

        final List<String> paths = new ArrayList<>();
        try (Stream<Path> remaining = Files.walk(workDirectory))
        {
            remaining.limit(MAX_REMAINING_PATHS_TO_LOG).forEach(path -> paths.add(describePath(path)));
        }
        catch (IOException e)
        {
            paths.add(String.format("<unable to inspect remaining paths: %s>", e));
        }
        return paths;
    }

    private static String describePath(final Path path)
    {
        try
        {
            return Files.isRegularFile(path)
                    ? String.format("%s (%d bytes)", path, Files.size(path))
                    : path.toString();
        }
        catch (IOException e)
        {
            return String.format("%s (size unavailable: %s)", path, e.getMessage());
        }
    }
}
