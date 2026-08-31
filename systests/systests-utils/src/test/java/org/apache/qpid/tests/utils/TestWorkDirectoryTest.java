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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import org.apache.qpid.test.utils.UnitTestBase;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
public class TestWorkDirectoryTest extends UnitTestBase
{
    @TempDir
    private Path _tempDirectory;

    private String _originalWorkDirectoryRoot;
    private String _originalJavaIoTmpDir;
    private String _originalPreserveWorkDirectory;
    private String _originalCleanBetweenTests;

    @BeforeEach
    public void saveSystemProperties()
    {
        _originalWorkDirectoryRoot = System.getProperty(TestWorkDirectory.WORK_DIRECTORY_ROOT_PROPERTY);
        _originalJavaIoTmpDir = System.getProperty("java.io.tmpdir");
        _originalPreserveWorkDirectory = System.getProperty(TestWorkDirectory.PRESERVE_WORK_DIRECTORY_PROPERTY);
        _originalCleanBetweenTests = System.getProperty("broker.clean.between.tests");
    }

    @AfterEach
    public void restoreSystemProperties()
    {
        restoreProperty(TestWorkDirectory.WORK_DIRECTORY_ROOT_PROPERTY, _originalWorkDirectoryRoot);
        restoreProperty("java.io.tmpdir", _originalJavaIoTmpDir);
        restoreProperty(TestWorkDirectory.PRESERVE_WORK_DIRECTORY_PROPERTY, _originalPreserveWorkDirectory);
        restoreProperty("broker.clean.between.tests", _originalCleanBetweenTests);
    }

    @Test
    public void createUsesConfiguredParent() throws Exception
    {
        final Path parent = _tempDirectory.resolve("configured-parent").toAbsolutePath().normalize();
        System.setProperty(TestWorkDirectory.WORK_DIRECTORY_ROOT_PROPERTY, parent.toString());

        final Path workDirectory = TestWorkDirectory.create("qpid-work-configured-");
        assertEquals(parent, workDirectory.getParent());

        TestWorkDirectory.delete(workDirectory);
        assertFalse(Files.exists(workDirectory));
    }

    @Test
    public void createResolvesJavaIoTmpDirForEveryInvocation() throws Exception
    {
        System.clearProperty(TestWorkDirectory.WORK_DIRECTORY_ROOT_PROPERTY);
        final Path firstParent = _tempDirectory.resolve("first-parent").toAbsolutePath().normalize();
        final Path secondParent = _tempDirectory.resolve("second-parent").toAbsolutePath().normalize();

        System.setProperty("java.io.tmpdir", firstParent.toString());
        final Path firstWorkDirectory = TestWorkDirectory.create("qpid-work-first-");
        System.setProperty("java.io.tmpdir", secondParent.toString());
        final Path secondWorkDirectory = TestWorkDirectory.create("qpid-work-second-");

        assertEquals(firstParent, firstWorkDirectory.getParent());
        assertEquals(secondParent, secondWorkDirectory.getParent());
        TestWorkDirectory.delete(firstWorkDirectory);
        TestWorkDirectory.delete(secondWorkDirectory);
    }

    @Test
    public void deleteIsRecursiveAndIdempotent() throws Exception
    {
        System.setProperty(TestWorkDirectory.WORK_DIRECTORY_ROOT_PROPERTY, _tempDirectory.toString());
        System.setProperty("broker.clean.between.tests", "false");
        final Path workDirectory = TestWorkDirectory.create("qpid-work-recursive-");
        Files.writeString(Files.createDirectories(workDirectory.resolve("store").resolve("nested"))
                                     .resolve("data.db"),
                          "data");

        TestWorkDirectory.delete(workDirectory);
        TestWorkDirectory.delete(workDirectory);

        assertFalse(Files.exists(workDirectory));
    }

    @Test
    public void deleteCanPreserveWorkDirectoryForDiagnostics() throws Exception
    {
        System.setProperty(TestWorkDirectory.WORK_DIRECTORY_ROOT_PROPERTY, _tempDirectory.toString());
        final Path workDirectory = TestWorkDirectory.create("qpid-work-preserved-");
        Files.writeString(workDirectory.resolve("artifact.log"), "diagnostic data");

        System.setProperty(TestWorkDirectory.PRESERVE_WORK_DIRECTORY_PROPERTY, "true");
        TestWorkDirectory.delete(workDirectory);
        assertTrue(Files.exists(workDirectory.resolve("artifact.log")));

        System.clearProperty(TestWorkDirectory.PRESERVE_WORK_DIRECTORY_PROPERTY);
        TestWorkDirectory.delete(workDirectory);
        assertFalse(Files.exists(workDirectory));
    }

    private static void restoreProperty(final String name, final String value)
    {
        if (value == null)
        {
            System.clearProperty(name);
        }
        else
        {
            System.setProperty(name, value);
        }
    }
}
