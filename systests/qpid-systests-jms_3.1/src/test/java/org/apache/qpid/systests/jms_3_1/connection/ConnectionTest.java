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

package org.apache.qpid.systests.jms_3_1.connection;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import jakarta.jms.JMSSecurityException;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("connection")
class ConnectionTest
{
    @Test
    void successfulConnection(final JmsSupport jms) throws Exception
    {
        try (final var con = jms.builder().connection().create())
        {
            assertThat(con, is(notNullValue()));
        }
    }

    @Test
    void badPassword(final JmsSupport jms)
    {
        var e = assertThrows(JMSException.class, () ->
                {
                    try (final var connection = jms.builder().connection().username("user").password("badpassword").create())
                    {
                        connection.start();
                    }
                },
                "Expected exception not thrown");

        if (!(e instanceof JMSSecurityException))
        {
            assertThat(e.getMessage().toLowerCase(), containsString("authentication failed"));
        }
    }

    @Test
    void unresolvableHost(final JmsSupport jms)
    {
        /* RFC-2606 guarantees that .invalid address will never resolve */
        assertThrows(JMSException.class, () ->
                {
                    try (final var connection = jms.builder().connection().host("unknownhost.unknowndomain.invalid").create())
                    {
                        connection.start();
                    }
                },
                "Expected exception not thrown");
    }

    @Test
    void unknownVirtualHost(final JmsSupport jms)
    {
        assertThrows(JMSException.class, () ->
                {
                    try (final var connection = jms.builder().connection().virtualHost("unknown").create())
                    {
                        connection.start();
                    }
                },
                "Expected exception not thrown");
    }

    @Test
    void connectionFactorySuppliedClientIdImmutable(final JmsSupport jms) throws Exception
    {
        final String clientId = UUID.randomUUID().toString();
        try (final var con = jms.builder().connection().clientId(clientId) .create())
        {
            assertThat(con.getClientID(), is(equalTo(clientId)));

            assertThrows(IllegalStateException.class,
                    () -> con.setClientID(UUID.randomUUID().toString()),
                    "Expected exception not thrown");

            assertThat(con.getClientID(), is(equalTo(clientId)));
        }
    }
}
