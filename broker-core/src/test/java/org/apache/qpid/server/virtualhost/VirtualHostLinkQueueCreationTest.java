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
package org.apache.qpid.server.virtualhost;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutorImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.Outcome;
import org.apache.qpid.server.logging.messages.QueueMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.queue.CreatingLinkInfo;
import org.apache.qpid.server.queue.CreatingLinkInfoImpl;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.AccessDeniedException;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.test.utils.UnitTestBase;

class VirtualHostLinkQueueCreationTest extends UnitTestBase
{
    private final Subject _caller = new Subject(true, Set.of((Principal) () -> "queue-creator"), Set.of(), Set.of());
    private final List<LogMessage> _creationEvents = new CopyOnWriteArrayList<>();
    private final List<Subject> _eventSubjects = new CopyOnWriteArrayList<>();
    private final AtomicInteger _authorizations = new AtomicInteger();
    private AbstractVirtualHost<?> _virtualHost;
    private AccessControl<SecurityToken> _accessControl;
    private DurableConfigurationStore _store;
    private TaskExecutor _executor;

    @BeforeEach
    void setUp()
    {
        _creationEvents.clear();
        _eventSubjects.clear();
        _authorizations.set(0);
        _executor = new TaskExecutorImpl(getTestName(), null);
        _executor.start();
        _accessControl = BrokerTestHelper.createAccessControlMock();
        final Broker<?> broker = BrokerTestHelper.createNewBrokerMock();
        final EventLogger eventLogger = mock(EventLogger.class);
        when(broker.getEventLogger()).thenReturn(eventLogger);
        when(((SystemConfig<?>) broker.getParent()).getEventLogger()).thenReturn(eventLogger);
        doAnswer(invocation ->
        {
            final LogMessage message = invocation.getArgument(0);
            recordCreationEvent(message);
            return null;
        }).when(eventLogger).message(any(LogMessage.class));
        doAnswer(invocation ->
        {
            recordCreationEvent(invocation.getArgument(1));
            return null;
        }).when(eventLogger).message(any(LogSubject.class), any(LogMessage.class));
        final VirtualHostNode<?> parent = BrokerTestHelper.createVirtualHostNodeMock(getTestName(), false,
                _accessControl, broker);
        when(parent.getEventLogger()).thenReturn(eventLogger);
        when(parent.getTaskExecutor()).thenReturn(_executor);
        when(parent.getChildExecutor()).thenReturn(_executor);
        _store = parent.getConfigurationStore();
        _virtualHost = (AbstractVirtualHost<?>) broker.getObjectFactory().create(VirtualHost.class,
                Map.of(VirtualHost.NAME, getTestName(), VirtualHost.TYPE, TestMemoryVirtualHost.VIRTUAL_HOST_TYPE),
                parent);
        _virtualHost.start();
        clearInvocations(_store);
    }

    @AfterEach
    void tearDown()
    {
        try
        {
            if (_virtualHost != null)
            {
                _virtualHost.close();
            }
        }
        finally
        {
            if (_executor != null)
            {
                _executor.stop();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeniedCreationIsAuditedBeforeResourceCreation(final boolean source)
    {
        expectAuthorization(Result.DENIED);
        final Subject originalSubject = SubjectExecutionContext.currentSubject();
        SubjectExecutionContext.withSubject(_caller, () ->
        {
            assertThrows(AccessDeniedException.class, () -> createLinkQueue(source, attributes()));
            assertSame(_caller, SubjectExecutionContext.currentSubject());
        });

        assertSame(originalSubject, SubjectExecutionContext.currentSubject());
        assertEquals(1, _authorizations.get());
        assertTrue(_virtualHost.getChildren(Queue.class).isEmpty());
        verifyNoInteractions(_store);
        assertCreationOutcome(Outcome.FAILURE);
        assertSame(_caller, _eventSubjects.get(0), "The refusal must be logged before adding system rights");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAllowedCreationRetainsMetadataAndRestoresCaller(final boolean source)
    {
        expectAuthorization(Result.ALLOWED);
        final Subject originalSubject = SubjectExecutionContext.currentSubject();
        final Queue<?> queue = SubjectExecutionContext.withSubjectUnchecked(_caller, () ->
        {
            final Queue<?> created = createLinkQueue(source, attributes());
            assertSame(_caller, SubjectExecutionContext.currentSubject());
            return created;
        });

        assertSame(originalSubject, SubjectExecutionContext.currentSubject());
        assertEquals(1, _authorizations.get());
        assertEquals(1, _virtualHost.getChildren(Queue.class).size());
        assertSame(queue, _virtualHost.getChildByName(Queue.class, getTestName()));
        assertFalse(queue.isDurable());
        assertEquals("container", queue.getCreatingLinkInfo().getRemoteContainerId());
        assertEquals("link", queue.getCreatingLinkInfo().getLinkName());
        assertEquals(source, queue.getCreatingLinkInfo().isSendingLink());
        assertCreationOutcome(Outcome.SUCCESS);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testValidationFailureIsLoggedOnce(final boolean source)
    {
        expectAuthorization(Result.ALLOWED);
        final Map<String, Object> attributes = new HashMap<>(attributes());
        attributes.put(Queue.CONTEXT, Map.of(Queue.QUEUE_FLOW_RESUME_LIMIT, 101.0));
        final Subject originalSubject = SubjectExecutionContext.currentSubject();
        SubjectExecutionContext.withSubject(_caller, () ->
        {
            assertThrows(IllegalConfigurationException.class, () -> createLinkQueue(source, attributes));
            assertSame(_caller, SubjectExecutionContext.currentSubject());
        });

        assertSame(originalSubject, SubjectExecutionContext.currentSubject());
        assertEquals(1, _authorizations.get());
        assertTrue(_virtualHost.getChildren(Queue.class).isEmpty());
        assertCreationOutcome(Outcome.FAILURE);
    }

    private void expectAuthorization(final Result result)
    {
        final Thread callerThread = Thread.currentThread();
        when(_accessControl.authorise(any(), eq(Operation.CREATE),
                argThat(object -> object != null && object.getCategoryClass() == Queue.class), any()))
                .thenAnswer(invocation ->
                {
                    assertNotSame(callerThread, Thread.currentThread(),
                                  "Authorization must cross the executor boundary");
                    assertSame(_caller, SubjectExecutionContext.currentSubject(),
                               "Authorize without added system rights");
                    assertTrue(_virtualHost.getChildren(Queue.class).isEmpty());
                    _authorizations.incrementAndGet();
                    return result;
                });
    }

    private Map<String, Object> attributes()
    {
        return Map.of(Queue.NAME, getTestName(), Queue.DURABLE, false,
                Queue.LIFETIME_POLICY, LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS);
    }

    private Queue<?> createLinkQueue(final boolean source, final Map<String, Object> attributes)
    {
        final CreatingLinkInfo info = new CreatingLinkInfoImpl(source, "container", "link");
        return (Queue<?>) (source
                ? _virtualHost.createLinkMessageSource(attributes, info)
                : _virtualHost.createLinkMessageDestination(attributes, info));
    }

    private void assertCreationOutcome(final Outcome outcome)
    {
        assertEquals(1, _creationEvents.size(), "The queue creation attempt must have one outcome event");
        assertTrue(_creationEvents.get(0).toString().contains(outcome.toString()));
        assertTrue(_creationEvents.get(0).toString().contains(getTestName()));
    }

    private void recordCreationEvent(final LogMessage message)
    {
        if (message.getLogHierarchy().endsWith(".queue.create") ||
                message.getLogHierarchy().equals(QueueMessages.CREATE_LOG_HIERARCHY))
        {
            _creationEvents.add(message);
            _eventSubjects.add(SubjectExecutionContext.currentSubject());
        }
    }
}
