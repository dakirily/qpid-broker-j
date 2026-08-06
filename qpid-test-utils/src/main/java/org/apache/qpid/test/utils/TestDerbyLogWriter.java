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

import java.io.IOException;
import java.io.Writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a Derby error writer which does not hold a file open for the lifetime of a test JVM.
 */
public final class TestDerbyLogWriter
{
    private static final Logger DERBY_LOG = LoggerFactory.getLogger("DERBY");
    private static final Writer DERBY_LOG_WRITER = new DerbyLogWriter();

    private TestDerbyLogWriter()
    {
    }

    /**
     * Method referenced by the {@code derby.stream.error.method} system property.
     */
    @SuppressWarnings("unused")
    public static Writer getLogWriter()
    {
        return DERBY_LOG_WRITER;
    }

    private static final class DerbyLogWriter extends Writer
    {
        private static final String DERBY_STARTUP_MESSAGE = "Booting Derby version ";
        private static final String DERBY_SHUTDOWN_MESSAGE = "Shutting down instance ";
        private static final String DERBY_CLASS_LOADER_STARTED_MESSAGE = "Database Class Loader started";
        private static final String DERBY_SYSTEM_HOME = "derby.system.home";
        private static final String DERBY_STREAM_ERROR_METHOD = "derby.stream.error.method";
        private static final String DASHED_LINE = "\\s*-*\\s*";

        private final ThreadLocal<StringBuilder> _threadLocalBuffer = ThreadLocal.withInitial(StringBuilder::new);

        @Override
        public void write(final char[] characters, final int offset, final int length)
        {
            _threadLocalBuffer.get().append(characters, offset, length);
        }

        @Override
        public void flush()
        {
            String logMessage = _threadLocalBuffer.get().toString();
            if (!logMessage.matches(DASHED_LINE))
            {
                if (logMessage.contains(DERBY_STARTUP_MESSAGE))
                {
                    logMessage = logMessage.substring(logMessage.indexOf('\n') + 1);
                }

                if (logMessage.startsWith(DERBY_STARTUP_MESSAGE)
                        || logMessage.startsWith(DERBY_SHUTDOWN_MESSAGE))
                {
                    DERBY_LOG.info(logMessage);
                }
                else if (logMessage.startsWith(DERBY_SYSTEM_HOME)
                        || logMessage.startsWith(DERBY_STREAM_ERROR_METHOD)
                        || logMessage.startsWith("java.vendor")
                        || logMessage.startsWith(DERBY_CLASS_LOADER_STARTED_MESSAGE))
                {
                    DERBY_LOG.debug(logMessage);
                }
                else
                {
                    DERBY_LOG.warn(logMessage);
                }
            }
            _threadLocalBuffer.set(new StringBuilder());
        }

        @Override
        public void close() throws IOException
        {
            flush();
        }
    }
}
