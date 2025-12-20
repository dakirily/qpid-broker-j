/*
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

package org.apache.qpid.tests.protocol.v0_8.extension.authtimeout;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;

import org.apache.qpid.tests.utils.RunBrokerAdmin;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.tests.protocol.v0_8.FrameTransport;
import org.apache.qpid.tests.protocol.v0_8.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminExtension;
import org.apache.qpid.tests.utils.ConfigItem;

@RunBrokerAdmin(type = "EMBEDDED_BROKER_PER_CLASS")
@ExtendWith({ BrokerAdminExtension.class })
@ConfigItem(name = Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY, value = "500")
public class AuthenticationTimeoutTest
{
    @Test
    public void authenticationTimeout(final BrokerAdmin brokerAdmin) throws Exception
    {
        assumeTrue(brokerAdmin.isSASLMechanismSupported("PLAIN"));

        try (FrameTransport transport = new FrameTransport(brokerAdmin, BrokerAdmin.PortType.AMQP).connect())
        {
            final Interaction interaction = transport.newInteraction();
            final ConnectionStartBody start = interaction.negotiateProtocol()
                                                         .consumeResponse()
                                                         .getLatestResponse(ConnectionStartBody.class);

            assertThat(Arrays.asList(new String(start.getMechanisms()).split(" ")), hasItem("PLAIN"));

            interaction.connection()
                       .startOkMechanism("PLAIN")
                       .startOk()
                       .consumeResponse(ConnectionSecureBody.class);

            transport.assertNoMoreResponsesAndChannelClosed();
        }
    }
}
