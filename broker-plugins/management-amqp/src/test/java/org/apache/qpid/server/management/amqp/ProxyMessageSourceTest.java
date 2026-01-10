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
package org.apache.qpid.server.management.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.connection.SessionPrincipal;
import org.apache.qpid.server.consumer.ConsumerOption;
import org.apache.qpid.server.consumer.ConsumerTarget;
import org.apache.qpid.server.message.MessageInstanceConsumer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.session.AMQPSession;
@SuppressWarnings({"rawtypes", "unchecked"})
public class ProxyMessageSourceTest
{
    @Test
    public void testAddConsumerWithoutSubjectReturnsNull() throws Exception
    {
        final String sourceName = "testSource";
        final ManagementAddressSpace addressSpace = mock(ManagementAddressSpace.class);
        final ManagementNode managementNode = mock(ManagementNode.class);
        when(addressSpace.getManagementNode()).thenReturn(managementNode);

        final ProxyMessageSource source =
                new ProxyMessageSource(addressSpace, Map.of(ConfiguredObject.NAME, sourceName));
        final ConsumerTarget target = mock(ConsumerTarget.class);

        final MessageInstanceConsumer consumer = SubjectExecutionContext.withSubject(null, () ->
                source.addConsumer(target, null, ServerMessage.class, "c", EnumSet.noneOf(ConsumerOption.class), null));

        assertNull(consumer, "Unexpected consumer");
        verifyNoInteractions(managementNode);
    }

    @Test
    public void testAddConsumerWithSessionPrincipalUsesSubject() throws Exception
    {
        final String sourceName = "testSource";
        final ManagementAddressSpace addressSpace = mock(ManagementAddressSpace.class);
        final ManagementNode managementNode = mock(ManagementNode.class);
        when(addressSpace.getManagementNode()).thenReturn(managementNode);

        final ProxyMessageSource source =
                new ProxyMessageSource(addressSpace, Map.of(ConfiguredObject.NAME, sourceName));
        final ConsumerTarget target = mock(ConsumerTarget.class);

        doAnswer(invocation ->
        {
            final ConsumerTarget wrapper = invocation.getArgument(0);
            final MessageInstanceConsumer consumer = mock(MessageInstanceConsumer.class);
            wrapper.consumerAdded(consumer);
            return null;
        }).when(managementNode).addConsumer(any(), any(), any(), anyString(), any(), any());

        final AMQPSession session = mock(AMQPSession.class);
        final Object connectionReference = new Object();
        when(session.getConnectionReference()).thenReturn(connectionReference);
        when(session.getId()).thenReturn(java.util.UUID.randomUUID());

        final SessionPrincipal sessionPrincipal = new SessionPrincipal(session);
        final Subject subject = new Subject(false, Set.of(sessionPrincipal), Set.of(), Set.of());

        final MessageInstanceConsumer consumer = SubjectExecutionContext.withSubject(subject, () ->
                source.addConsumer(target, null, ServerMessage.class, "c", EnumSet.noneOf(ConsumerOption.class), null));

        assertNotNull(consumer, "Consumer was not created");
        assertEquals(true, source.verifySessionAccess(session), "Unexpected session access");
        verify(managementNode).addConsumer(any(), any(), any(), anyString(), any(), any());
    }
}
