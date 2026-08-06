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
package org.apache.qpid.test.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.EnumSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestFileUtilsTest
{
    @TempDir
    private Path _tempDirectory;

    @Test
    public void testDeleteRecursivelyRestoresRestrictedOwnerPermissions() throws Exception
    {
        final Path root = Files.createDirectory(_tempDirectory.resolve("restricted"));
        final Path child = Files.createDirectory(root.resolve("child"));
        final Path file = Files.writeString(child.resolve("key"), "secret");

        restrictFile(file);
        restrictDirectory(child);
        restrictDirectory(root);

        TestFileUtils.deleteRecursively(root);

        assertFalse(Files.exists(root), "Restricted test directory was not deleted");
    }

    @Test
    public void testDeleteDerbyLogs() throws Exception
    {
        final Path workingDirectory = Files.createDirectory(_tempDirectory.resolve("module"));
        final Path defaultLog = Files.createFile(workingDirectory.resolve("derby.log"));
        final Path configuredLog = Files.createDirectories(workingDirectory.resolve("target")).resolve("derby.log");
        Files.createFile(configuredLog);

        TestFileUtils.deleteDerbyLogs(workingDirectory, "target/derby.log");

        assertFalse(Files.exists(defaultLog), "Default Derby log was not deleted");
        assertFalse(Files.exists(configuredLog), "Configured Derby log was not deleted");
    }

    @Test
    public void testDeleteDerbyLogsDoesNotDeleteOutsideWorkingDirectory() throws Exception
    {
        final Path workingDirectory = Files.createDirectory(_tempDirectory.resolve("module"));
        final Path externalLog = Files.createFile(_tempDirectory.resolve("derby.log"));

        TestFileUtils.deleteDerbyLogs(workingDirectory, ".." + System.getProperty("file.separator") + "derby.log");

        assertTrue(Files.exists(externalLog), "Derby log outside the working directory was deleted");
    }

    @Test
    public void testDeleteDerbyLogsDoesNotDeleteDirectory() throws Exception
    {
        final Path workingDirectory = Files.createDirectory(_tempDirectory.resolve("module"));
        final Path directory = Files.createDirectory(workingDirectory.resolve("derby.log"));

        TestFileUtils.deleteDerbyLogs(workingDirectory, null);

        assertTrue(Files.isDirectory(directory), "Directory named derby.log was deleted");
    }

    @Test
    public void testUnitTestCleanupDeletesDerbyLogWhenCallbackFails() throws Exception
    {
        final Path workingDirectory = Files.createDirectory(_tempDirectory.resolve("module"));
        final Path derbyLog = Files.createFile(workingDirectory.resolve("derby.log"));
        final String originalWorkingDirectory = System.getProperty("user.dir");
        final String originalBaseDirectory = System.getProperty("basedir");
        final UnitTestBase testBase = new UnitTestBase();
        testBase.registerAfterAllTearDown(() ->
        {
            throw new IllegalStateException("Expected callback failure");
        });

        try
        {
            System.setProperty("user.dir", workingDirectory.toString());
            System.setProperty("basedir", workingDirectory.toString());
            assertThrows(IllegalStateException.class, testBase::cleanupAfterAll);
        }
        finally
        {
            System.setProperty("user.dir", originalWorkingDirectory);
            if (originalBaseDirectory == null)
            {
                System.clearProperty("basedir");
            }
            else
            {
                System.setProperty("basedir", originalBaseDirectory);
            }
        }

        assertFalse(Files.exists(derbyLog), "Derby log was not deleted after a cleanup callback failure");
    }

    private void restrictFile(final Path file) throws Exception
    {
        final DosFileAttributeView dosView = Files.getFileAttributeView(file, DosFileAttributeView.class);
        if (dosView != null)
        {
            dosView.setReadOnly(true);
        }

        final PosixFileAttributeView posixView = Files.getFileAttributeView(file, PosixFileAttributeView.class);
        if (posixView != null)
        {
            posixView.setPermissions(EnumSet.of(PosixFilePermission.OWNER_READ));
        }
        else
        {
            final AclFileAttributeView aclView = Files.getFileAttributeView(file, AclFileAttributeView.class);
            if (aclView != null)
            {
                final UserPrincipal owner = Files.getOwner(file);
                aclView.setAcl(List.of(AclEntry.newBuilder()
                        .setType(AclEntryType.ALLOW)
                        .setPrincipal(owner)
                        .setPermissions(AclEntryPermission.READ_DATA,
                                        AclEntryPermission.READ_ATTRIBUTES,
                                        AclEntryPermission.READ_ACL,
                                        AclEntryPermission.SYNCHRONIZE)
                        .build()));
            }
        }
    }

    private void restrictDirectory(final Path directory) throws Exception
    {
        final PosixFileAttributeView posixView = Files.getFileAttributeView(directory, PosixFileAttributeView.class);
        if (posixView != null)
        {
            posixView.setPermissions(EnumSet.of(PosixFilePermission.OWNER_READ,
                                                PosixFilePermission.OWNER_EXECUTE));
        }
        else
        {
            final AclFileAttributeView aclView = Files.getFileAttributeView(directory, AclFileAttributeView.class);
            if (aclView != null)
            {
                final UserPrincipal owner = Files.getOwner(directory);
                aclView.setAcl(List.of(AclEntry.newBuilder()
                        .setType(AclEntryType.ALLOW)
                        .setPrincipal(owner)
                        .setPermissions(AclEntryPermission.ADD_FILE,
                                        AclEntryPermission.ADD_SUBDIRECTORY,
                                        AclEntryPermission.LIST_DIRECTORY)
                        .build()));
            }
        }
    }
}
