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
package org.apache.qpid.server.protocol.v1_0;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.PublishAuthorisationCache;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings("unchecked")
class AnonymousRelayDestinationTest extends UnitTestBase
{
    @Test
    void sendDelegatesToDestinationViaCache() throws UnroutableMessageException
    {
        final Queue<?> queue = mock(Queue.class);
        doReturn("myQueue").when(queue).getName();

        final RoutingResult routingResult = mock(RoutingResult.class);
        doReturn(1).when(routingResult).send(any(), any());
        doReturn(Collections.emptyList()).when(routingResult).getRoutes();
        doReturn(routingResult).when(queue).route(any(), anyString(), any());

        final NamedAddressSpace addressSpace = mock(NamedAddressSpace.class);
        doReturn("myQueue").when(addressSpace).getLocalAddress("myQueue");
        doReturn(queue).when(addressSpace).getAttainedMessageDestination("myQueue", true);

        final Target target = mock(Target.class);
        final EventLogger eventLogger = mock(EventLogger.class);
        final AnonymousRelayDestination relay = new AnonymousRelayDestination(addressSpace, target, eventLogger);

        final SecurityToken token = mock(SecurityToken.class);
        final PublishAuthorisationCache cache = new PublishAuthorisationCache(token, 5000L, 20);

        final ServerMessage<?> message = mock(ServerMessage.class);
        doReturn("myQueue").when(message).getTo();
        doReturn("").when(message).getInitialRoutingAddress();
        final ServerTransaction txn = mock(ServerTransaction.class);

        relay.send(message, txn, cache, 1000L);

        verify(queue).authorisePublish(any(SecurityToken.class), any(Map.class));

        relay.send(message, txn, cache, 2000L);

        verify(queue).authorisePublish(any(SecurityToken.class), any(Map.class));
    }

    @Test
    void sendThrowsForUnknownDestination()
    {
        final NamedAddressSpace addressSpace = mock(NamedAddressSpace.class);
        doReturn("unknown").when(addressSpace).getLocalAddress("unknown");
        doReturn(null).when(addressSpace).getAttainedMessageDestination("unknown", true);

        final Target target = mock(Target.class);
        final EventLogger eventLogger = mock(EventLogger.class);
        final AnonymousRelayDestination relay = new AnonymousRelayDestination(addressSpace, target, eventLogger);

        final SecurityToken token = mock(SecurityToken.class);
        final PublishAuthorisationCache cache = new PublishAuthorisationCache(token, 5000L, 20);

        final ServerMessage<?> message = mock(ServerMessage.class);
        doReturn("unknown").when(message).getTo();
        final ServerTransaction txn = mock(ServerTransaction.class);

        assertThrows(UnroutableMessageException.class,
                     () -> relay.send(message, txn, cache, 1000L));
    }
}
