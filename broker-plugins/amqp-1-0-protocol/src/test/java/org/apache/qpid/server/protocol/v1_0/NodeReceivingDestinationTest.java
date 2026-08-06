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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Map;

import org.apache.qpid.server.security.AccessDeniedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.message.RoutingResult;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.DestinationAddress;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.PublishAuthorisationCache;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings("unchecked")
class NodeReceivingDestinationTest extends UnitTestBase
{
    private Exchange<?> _exchange;
    private SecurityToken _token;
    private ServerTransaction _txn;

    @BeforeEach
    void setUp()
    {
        _exchange = mock(Exchange.class);
        doReturn(Exchange.UnroutableMessageBehaviour.REJECT).when(_exchange).getUnroutableMessageBehaviour();
        doReturn("testExchange").when(_exchange).getName();

        final RoutingResult routingResult = mock(RoutingResult.class);
        doReturn(1).when(routingResult).send(any(), any());
        doReturn(Collections.emptyList()).when(routingResult).getRoutes();
        doReturn(routingResult).when(_exchange).route(any(), anyString(), any());

        _token = mock(SecurityToken.class);
        _txn = mock(ServerTransaction.class);
    }

    @Test
    void authorisePublishUsesCache() throws UnroutableMessageException
    {
        final NodeReceivingDestination destination = createDestination(_exchange);
        final PublishAuthorisationCache cache = new PublishAuthorisationCache(_token, 5000L, 20);

        final ServerMessage<?> message = createMessage("myKey");

        destination.send(message, _txn, cache, 1000L);
        verify(_exchange).authorisePublish(any(SecurityToken.class), any(Map.class));

        destination.send(message, _txn, cache, 2000L);
        verify(_exchange).authorisePublish(any(SecurityToken.class), any(Map.class));
    }

    @Test
    void authorisePublishCalledAgainAfterCacheExpiry() throws UnroutableMessageException
    {
        final NodeReceivingDestination destination = createDestination(_exchange);
        final PublishAuthorisationCache cache = new PublishAuthorisationCache(_token, 5000L, 20);

        final ServerMessage<?> message = createMessage("myKey");

        destination.send(message, _txn, cache, 1000L);

        destination.send(message, _txn, cache, 7000L);
        verify(_exchange, org.mockito.Mockito.times(2)).authorisePublish(any(SecurityToken.class), any(Map.class));
    }

    @Test
    void authorisePublishDifferentRoutingKeysAreCachedSeparately() throws UnroutableMessageException
    {
        final NodeReceivingDestination destination = createDestination(_exchange);
        final PublishAuthorisationCache cache = new PublishAuthorisationCache(_token, 5000L, 20);

        destination.send(createMessage("key1"), _txn, cache, 1000L);
        destination.send(createMessage("key2"), _txn, cache, 1000L);
        verify(_exchange, org.mockito.Mockito.times(2)).authorisePublish(any(SecurityToken.class), any(Map.class));

        destination.send(createMessage("key1"), _txn, cache, 2000L);
        destination.send(createMessage("key2"), _txn, cache, 2000L);
        verify(_exchange, org.mockito.Mockito.times(2)).authorisePublish(any(SecurityToken.class), any(Map.class));
    }

    @Test
    void authorisePublishAccessControlExceptionPropagated()
    {
        final NodeReceivingDestination destination = createDestination(_exchange);
        final PublishAuthorisationCache cache = new PublishAuthorisationCache(_token, 5000L, 20);

        doThrow(new AccessDeniedException("denied")).when(_exchange)
                .authorisePublish(any(SecurityToken.class), any(Map.class));

        assertThrows(AccessDeniedException.class,
                     () -> destination.send(createMessage("myKey"), _txn, cache, 1000L));

        verify(_exchange, never()).route(any(), anyString(), any());
    }

    private NodeReceivingDestination createDestination(final Exchange<?> exchange)
    {
        final NamedAddressSpace addressSpace = mock(NamedAddressSpace.class);
        doReturn("testExchange").when(addressSpace).getLocalAddress("testExchange");
        doReturn(exchange).when(addressSpace).getAttainedMessageDestination("testExchange", false);

        final DestinationAddress destinationAddress = new DestinationAddress(addressSpace, "testExchange");
        return new NodeReceivingDestination(destinationAddress, null, null, null, mock(EventLogger.class));
    }

    private ServerMessage<?> createMessage(final String routingKey)
    {
        final ServerMessage<?> message = mock(ServerMessage.class);
        doReturn(routingKey).when(message).getInitialRoutingAddress();
        return message;
    }
}
