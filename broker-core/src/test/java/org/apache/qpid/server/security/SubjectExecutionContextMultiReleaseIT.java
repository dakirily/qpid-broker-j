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
package org.apache.qpid.server.security;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.junit.jupiter.api.Test;

class SubjectExecutionContextMultiReleaseIT
{
    @Test
    void testJava24MultiReleaseClassIsResolvedFromJar() throws Exception
    {
        final String jarPathProperty = System.getProperty("mr.test.jar.path");
        assertNotNull(jarPathProperty, "Missing system property mr.test.jar.path");

        final Path jarPath = Path.of(jarPathProperty);
        assertTrue(Files.exists(jarPath), "Core jar does not exist: " + jarPath);

        final String classResource = "org/apache/qpid/server/security/SubjectExecutionContext.class";
        final String versionedClassResource = "META-INF/versions/24/" + classResource;
        final byte[] versionedBytes;

        try (JarFile jarFile = new JarFile(jarPath.toFile(), true, JarFile.OPEN_READ, Runtime.version()))
        {
            final String multiRelease = String.valueOf(jarFile.getManifest().getMainAttributes().getValue("Multi-Release"))
                    .toLowerCase(Locale.ROOT);
            assertEquals("true", multiRelease, "Jar must be marked as Multi-Release");

            final JarEntry versionedEntry = jarFile.getJarEntry(versionedClassResource);
            assertNotNull(versionedEntry, "Missing Java 24 class entry in multi-release jar");

            try (InputStream in = jarFile.getInputStream(versionedEntry))
            {
                versionedBytes = in.readAllBytes();
            }
        }

        try (URLClassLoader classLoader = new URLClassLoader(new URL[]{jarPath.toUri().toURL()}, null))
        {
            final Class<?> contextClass = Class.forName("org.apache.qpid.server.security.SubjectExecutionContext",
                                                        true,
                                                        classLoader);
            try (InputStream in = contextClass.getResourceAsStream("SubjectExecutionContext.class"))
            {
                assertNotNull(in, "Cannot resolve SubjectExecutionContext class bytes from jar");
                final byte[] resolvedBytes = in.readAllBytes();
                assertArrayEquals(versionedBytes, resolvedBytes,
                                  "Runtime did not resolve SubjectExecutionContext from META-INF/versions/24");
            }
            final Method currentSubject = contextClass.getMethod("currentSubject");
            assertNull(currentSubject.invoke(null), "Unexpected subject outside execution context");
        }
    }
}
