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

package org.apache.qpid.tests.protocol.v0_8;

import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.QpidTestInfo;
import org.apache.qpid.tests.utils.QpidTestInfoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.tests.protocol.SpecificationTest;
import org.apache.qpid.tests.utils.BrokerAdminExtension;

@ExtendWith({ BrokerAdminExtension.class, QpidTestInfoExtension.class })
public class ChannelTest
{

    @Test
    @SpecificationTest(section = "1.5.2.1", description = "open a channel for use")
    public void channelOpen(final BrokerAdmin brokerAdmin, final QpidTestInfo testInfo) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(brokerAdmin).connect())
        {
            final Interaction interaction = transport.newInteraction();

            interaction.negotiateOpen(testInfo.virtualHostName())
                       .channelId(1)
                       .channel().open().consumeResponse(ChannelOpenOkBody.class);
        }
    }

    @Test
    @SpecificationTest(section = "1.5.2.5", description = "request a channel close")
    public void noFrameCanBeSentOnClosedChannel(final BrokerAdmin brokerAdmin, final QpidTestInfo testInfo) throws Exception
    {
        try (FrameTransport transport = new FrameTransport(brokerAdmin).connect())
        {
            final Interaction interaction = transport.newInteraction();

            interaction.negotiateOpen(testInfo.virtualHostName())
                       .channelId(1)
                       .channel().open().consumeResponse(ChannelOpenOkBody.class)
                       .channel().close().consumeResponse(ChannelCloseOkBody.class)
                       .channel().flow(true).consumeResponse(ConnectionCloseBody.class);
        }
    }
}
