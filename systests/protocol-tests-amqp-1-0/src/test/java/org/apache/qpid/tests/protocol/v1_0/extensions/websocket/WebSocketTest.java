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

package org.apache.qpid.tests.protocol.v1_0.extensions.websocket;

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.apache.qpid.tests.utils.QpidTestInfo;
import org.apache.qpid.tests.utils.QpidTestInfoExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.protocol.v1_0.FrameTransport;
import org.apache.qpid.tests.protocol.v1_0.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminExtension;

@ExtendWith({ BrokerAdminExtension.class, QpidTestInfoExtension.class })
public class WebSocketTest
{
    @BeforeEach
    public void setUp(final BrokerAdmin brokerAdmin)
    {
        assumeTrue(brokerAdmin.isWebSocketSupported(), "Broker support for AMQP over websockets is required");
        assumeTrue(brokerAdmin.isAnonymousSupported(), "Broker support for Anonymous open is required");
    }

    @Test
    @SpecificationTest(section = "2.1", description = "Opening a WebSocket Connection")
    public void protocolHeader(final BrokerAdmin brokerAdmin) throws Exception
    {
        try (FrameTransport transport = new WebSocketFrameTransport(brokerAdmin).connect())
        {
            final byte[] response = transport.newInteraction().negotiateProtocol().consumeResponse().getLatestResponse(byte[].class);
            assertArrayEquals(transport.getProtocolHeader(), response, "Unexpected protocol header response");
        }
    }

    @Test
    @SpecificationTest(section = "2.4", description = "[...] a single AMQP frame MAY be split over one or more consecutive WebSocket messages. ")
    public void amqpFramesSplitOverManyWebSocketFrames(final BrokerAdmin brokerAdmin, final QpidTestInfo testInfo) throws Exception
    {
        try (FrameTransport transport = new WebSocketFrameTransport(brokerAdmin).splitAmqpFrames().connect())
        {
            Interaction interaction = transport.newInteraction();
            final Open responseOpen = interaction.openHostname(testInfo.virtualHostName())
                    .negotiateOpen().getLatestResponse(Open.class);

            assertThat(responseOpen.getContainerId(), is(notNullValue()));
            assertThat(responseOpen.getMaxFrameSize().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseOpen.getChannelMax().intValue(),
                       is(both(greaterThanOrEqualTo(0)).and(lessThan(UnsignedShort.MAX_VALUE.intValue()))));

            interaction.doCloseConnection();
        }
    }

    @Test
    @SpecificationTest(section = "2.1", description = "Opening a WebSocket Connection")
    public void successfulOpen(final BrokerAdmin brokerAdmin, final QpidTestInfo testInfo) throws Exception
    {
        assumeTrue(brokerAdmin.isWebSocketSupported());

        try (FrameTransport transport = new WebSocketFrameTransport(brokerAdmin).connect())
        {
            Interaction interaction = transport.newInteraction();
            final Open responseOpen = interaction.openHostname(testInfo.virtualHostName())
                    .negotiateOpen().getLatestResponse(Open.class);

            assertThat(responseOpen.getContainerId(), is(notNullValue()));
            assertThat(responseOpen.getMaxFrameSize().longValue(),
                       is(both(greaterThanOrEqualTo(0L)).and(lessThan(UnsignedInteger.MAX_VALUE.longValue()))));
            assertThat(responseOpen.getChannelMax().intValue(),
                       is(both(greaterThanOrEqualTo(0)).and(lessThan(UnsignedShort.MAX_VALUE.intValue()))));

            interaction.doCloseConnection();
        }
    }
}
