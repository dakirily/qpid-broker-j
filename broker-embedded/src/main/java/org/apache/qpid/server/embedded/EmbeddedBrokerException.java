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

/**
 * Indicates that an embedded broker could not be started, configured, or stopped.
 */
public class EmbeddedBrokerException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates an exception with a message.
     *
     * @param message detail message
     */
    public EmbeddedBrokerException(final String message)
    {
        super(message);
    }

    /**
     * Creates an exception with a message and cause.
     *
     * @param message detail message
     * @param cause cause
     */
    public EmbeddedBrokerException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Creates an exception with a cause.
     *
     * @param cause cause
     */
    public EmbeddedBrokerException(final Throwable cause)
    {
        super(cause);
    }
}
