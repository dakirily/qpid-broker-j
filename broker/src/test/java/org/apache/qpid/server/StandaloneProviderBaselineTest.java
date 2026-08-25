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

import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.plugin.Pluggable;
import org.apache.qpid.server.plugin.ProtocolEngineCreator;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.plugin.SystemConfigFactory;
import org.apache.qpid.server.plugin.TransportProviderFactory;
import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Freezes the principal provider families in the dependency closure copied into the standalone distribution.
 */
public class StandaloneProviderBaselineTest extends UnitTestBase
{
    @Test
    public void testProtocolProviderSet()
    {
        final Set<Protocol> protocols = new TreeSet<>();
        for (final ProtocolEngineCreator creator :
                new QpidServiceLoader().instancesOf(ProtocolEngineCreator.class))
        {
            protocols.add(creator.getVersion());
        }

        assertEquals(Set.of(Protocol.AMQP_0_8, Protocol.AMQP_0_9, Protocol.AMQP_0_9_1,
                            Protocol.AMQP_0_10, Protocol.AMQP_1_0),
                     protocols);
    }

    @Test
    public void testSystemConfigProviderSet()
    {
        assertEquals(Set.of("BDB", "DERBY", "JDBC", "JSON", "Memory"),
                     providerTypes(SystemConfigFactory.class));
    }

    @Test
    public void testTransportProviderSet()
    {
        assertEquals(Set.of("TCPandSSL", "Websocket"),
                     providerTypes(TransportProviderFactory.class));
    }

    private static <P extends Pluggable> Set<String> providerTypes(final Class<P> providerClass)
    {
        final Set<String> providerTypes = new TreeSet<>();
        for (final P provider : new QpidServiceLoader().instancesOf(providerClass))
        {
            providerTypes.add(provider.getType());
        }
        return providerTypes;
    }
}
