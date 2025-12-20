/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.qpid.systests.jms_3_1.extensions.acl;

import static org.apache.qpid.systests.EntityTypes.QUEUE;
import static org.apache.qpid.systests.EntityTypes.RULE_BASED_VIRTUAL_HOST_ACCESS_CONTROL_PROVIDER;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Group;
import org.apache.qpid.server.model.GroupMember;
import org.apache.qpid.server.security.access.config.AclFileParser;
import org.apache.qpid.server.security.access.config.Rule;
import org.apache.qpid.server.security.access.config.RuleSet;
import org.apache.qpid.server.security.access.plugins.AclRule;
import org.apache.qpid.server.security.access.plugins.RuleBasedVirtualHostAccessControlProvider;
import org.apache.qpid.server.security.group.GroupProviderImpl;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("security")
class MessagingACLTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagingACLTest.class);

    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final String USER1 = "guest";
    private static final String USER1_PASSWORD = "guest";
    private static final String USER2 = "admin";
    private static final String USER2_PASSWORD = "admin";

    @Test
    void accessAuthorizedSuccess(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create())
        {
            assertConnection(connection);
        }
    }

    @Test
    void accessNoRightsFailure(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL DENY-LOG %s ACCESS VIRTUALHOST".formatted(USER1));

        final var e = assertThrows(JMSException.class,
                () ->
                {
                    try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create())
                    {
                        connection.start();
                    }
                },
                "Connection was created");

        assertAccessDeniedException(e);
    }

    @Test
    void accessVirtualHostWithName(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST name='%s'".formatted(USER1, jms.virtualHostName()),
                     "ACL DENY-LOG %s ACCESS VIRTUALHOST name='%s'".formatted(USER2, jms.virtualHostName()));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create())
        {
            assertConnection(connection);
        }

        final var e = assertThrows(JMSException.class,
                () ->
                {
                    try (final var connection = jms.builder().connection().username(USER2).password(USER2_PASSWORD).create())
                    {
                        connection.start();
                    }
                },
                "Access should be denied");

        assertAccessDeniedException(e);
    }

    @Test
    void accessVirtualHostWildCard(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST name='*'".formatted(USER1),
                "ACL DENY-LOG %s ACCESS VIRTUALHOST name='*'".formatted(USER2));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create())
        {
            assertConnection(connection);
        }

        final var e = assertThrows(JMSException.class,
                () ->
                {
                    try (final var connection = jms.builder().connection().username(USER2).password(USER2_PASSWORD).create())
                    {
                        connection.start();
                    }
                },
                "Access should be denied");

        assertAccessDeniedException(e);
    }

    @Test
    void consumeFromTempQueueSuccess(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL ALLOW-LOG %s CREATE QUEUE temporary=\"true\"".formatted(USER1),
                     "ACL ALLOW-LOG %s CONSUME QUEUE temporary=\"true\"".formatted(USER1),
                     "");

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create())
        {
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            connection.start();
            session.createConsumer(session.createTemporaryQueue()).close();
        }
    }

    @Test
    void consumeFromTempQueueFailure(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL ALLOW-LOG %s CREATE QUEUE temporary=\"true\"".formatted(USER1),
                     "ACL DENY-LOG %s CONSUME QUEUE temporary=\"true\"".formatted(USER1),
                     "");

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();

            final var temporaryQueue = session.createTemporaryQueue();

            assertThrows(JMSException.class,
                    () -> session.createConsumer(temporaryQueue),
                    "Exception is not thrown");
        }
        catch (Exception e)
        {
            LOGGER.error("Unexpected exception on connection close", e);
        }
    }

    @Test
    void consumeOwnQueueSuccess(final JmsSupport jms) throws Exception
    {
        assumeTrue(Objects.equals(jms.brokerAdmin().getValidUsername(), USER1));

        final String queueName = "user1Queue";
        jms.builder().queue(queueName).create();

        final Map<String, Object> queueAttributes = jms.management().read(queueName, QUEUE, true);
        assertThat("Test prerequisite not met, queue belongs to unexpected user", queueAttributes.get(ConfiguredObject.CREATED_BY), is(equalTo(USER1)));

        configureACL(jms, "ACL ALLOW-LOG ALL ACCESS VIRTUALHOST",
                     "ACL ALLOW-LOG OWNER CONSUME QUEUE",
                     "ACL DENY-LOG ALL CONSUME QUEUE");

        final String queueAddress = "%s".formatted(queueName);

        try (final var queueOwnerCon = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var queueOwnerSession = queueOwnerCon.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = queueOwnerSession.createQueue(queueAddress);
            queueOwnerSession.createConsumer(queue).close();
        }

        try (final var otherUserCon = jms.builder().connection().username(USER2).password(USER2_PASSWORD).create();
             final var otherUserSession = otherUserCon.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var e = assertThrows(JMSException.class,
                    () -> otherUserSession.createConsumer(otherUserSession.createQueue(queueAddress)).close(),
                    "Exception not thrown");

            assertJMSExceptionMessageContains(e, "Permission CREATE is denied for : Consumer");
        }
    }

    @Test
    void consumeFromTempTopicSuccess(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL ALLOW-LOG %s CREATE QUEUE temporary=\"true\"".formatted(USER1),
                     "ACL ALLOW-LOG %s CONSUME QUEUE temporary=\"true\"".formatted(USER1),
                     "ACL ALLOW-LOG %s BIND EXCHANGE temporary=\"true\"".formatted(USER1));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var temporaryTopic = session.createTemporaryTopic();
            session.createConsumer(temporaryTopic);
        }
    }

    @Test
    void consumeFromNamedQueueValid(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL ALLOW-LOG %s CONSUME QUEUE name=\"%s\"".formatted(USER1, queue.getQueueName()));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            session.createConsumer(queue).close();
        }
    }

    @Test
    void consumeFromNamedQueueFailure(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL DENY-LOG %s CONSUME QUEUE name=\"%s\"".formatted(USER1, queue.getQueueName()));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();

            assertThrows(JMSException.class,
                    () -> session.createConsumer(queue),
                    "Test failed as consumer was created");
        }
    }

    @Test
    void createTemporaryQueueSuccess(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL ALLOW-LOG %s CREATE QUEUE temporary=\"true\"".formatted(USER1));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var queue = session.createTemporaryQueue();
            assertNotNull(queue);
        }
    }

    // For AMQP 1.0 the server causes a temporary instance of the fanout exchange to come into being.
    // For early AMQP version, there are no server side objects created as amq.topic is used.
    @Test
    void createTempTopicSuccess(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var temporaryTopic = session.createTemporaryTopic();
            assertNotNull(temporaryTopic);
        }
    }

    @Test
    @Disabled("QPID-7919 - ACL constraints for temporary queues are not applied on AMQP 1.0 path")
    void createTemporaryQueueFailed(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL DENY-LOG %s CREATE QUEUE temporary=\"true\"".formatted(USER1));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();

            final var e = assertThrows(JMSException.class,
                    session::createTemporaryQueue,
                    "Test failed as creation succeeded");

            assertJMSExceptionMessageContains(e, "Permission CREATE is denied for : Queue");
        }
    }

    @Test
    void publishUsingTransactionSuccess(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"\" routingKey=\"%s\"".formatted(USER1, queue.getQueueName()));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var sender = session.createProducer(queue))
        {
            sender.send(session.createTextMessage("test"));
            session.commit();
        }
    }

    @Test
    void publishToExchangeUsingTransactionSuccess(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().bind().create();

        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"%s\"".formatted(USER1, queue.getQueueName()));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED))
        {
            final String address = "amq.direct/%s".formatted(queue.getQueueName());
            final var jmsQueue = session.createQueue(address);
            final var sender = session.createProducer(jmsQueue);
            sender.send(session.createTextMessage("test"));
            session.commit();
        }
    }

    @Test
    void requestResponseSuccess(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final String groupName = "messaging-users";
        createGroupProvider(jms, groupName, USER1, USER2);

        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(groupName),
                     "ACL ALLOW-LOG %s CONSUME QUEUE name=\"%s\"".formatted(USER1, queue.getQueueName()),
                     "ACL ALLOW-LOG %s CONSUME QUEUE temporary=true".formatted(USER2),
                     "ACL ALLOW-LOG %s CREATE QUEUE temporary=true".formatted(USER2),
                     "ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"\" routingKey=\"TempQueue*\"".formatted(USER1),
                     "ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"\" routingKey=\"%s\"".formatted(USER2, queue.getQueueName()));

        try (final var responderConnection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var responderSession = responderConnection.createSession(true, Session.SESSION_TRANSACTED);
             final var requestConsumer = responderSession.createConsumer(queue))
        {
            responderConnection.start();

            try (final var requesterConnection = jms.builder().connection().username(USER2).password(USER2_PASSWORD).create();
                 final var requesterSession = requesterConnection.createSession(true, Session.SESSION_TRANSACTED))
            {
                final var responseQueue = requesterSession.createTemporaryQueue();
                final var responseConsumer = requesterSession.createConsumer(responseQueue);
                requesterConnection.start();

                final var request = requesterSession.createTextMessage("Request");
                request.setJMSReplyTo(responseQueue);

                requesterSession.createProducer(queue).send(request);
                requesterSession.commit();

                final var receivedRequest = requestConsumer.receive(Timeouts.receiveMillis());
                assertNotNull(receivedRequest, "Request is not received");
                assertNotNull(receivedRequest.getJMSReplyTo(), "Request should have Reply-To");

                final var responder = responderSession.createProducer(receivedRequest.getJMSReplyTo());
                responder.send(responderSession.createTextMessage("Response"));
                responderSession.commit();

                final var receivedResponse = responseConsumer.receive(Timeouts.receiveMillis());
                requesterSession.commit();
                assertNotNull(receivedResponse, "Response is not received");
                final var textMessage = assertInstanceOf(TextMessage.class, receivedResponse);
                assertEquals("Response", textMessage.getText(), "Unexpected response is received");
            }
        }
    }

    @Test
    void publishToTempTopicSuccess(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL ALLOW-LOG %s PUBLISH EXCHANGE temporary=\"true\"".formatted(USER1));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED))
        {
            connection.start();

            final var temporaryTopic = session.createTemporaryTopic();
            final var producer = session.createProducer(temporaryTopic);
            producer.send(session.createMessage());
            session.commit();
        }
    }

    @Test
    void firewallAllow(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW %s ACCESS VIRTUALHOST from_network=\"127.0.0.1\"".formatted(USER1));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create())
        {
            assertConnection(connection);
        }
    }

    @Test
    void allAllowed(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL ALLOW ALL ALL");

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create())
        {
            assertConnection(connection);
        }
    }

    @Test
    void firewallDeny(final JmsSupport jms) throws Exception
    {
        configureACL(jms, "ACL DENY %s ACCESS VIRTUALHOST from_network=\"127.0.0.1\"".formatted(USER1));

        assertThrows(JMSException.class,
                () ->
                {
                    try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create())
                    {
                        connection.start();
                    }
                },
                "We expected the connection to fail");
    }

    @Test
    void anonymousProducerFailsToSendMessageIntoDeniedDestination(final JmsSupport jms) throws Exception
    {
        final String allowedDestinationName =  "example.RequestQueue";
        final String deniedDestinationName = "deniedQueue";
        jms.builder().queue(allowedDestinationName).create();
        jms.builder().queue(deniedDestinationName).create();

        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                "ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"\" routingKey=\"%s\"".formatted(USER1, allowedDestinationName),
                "ACL DENY-LOG %s PUBLISH EXCHANGE name=\"*\" routingKey=\"%s\"".formatted(USER1, deniedDestinationName));

        try (final var connection = jms.builder().connection().username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var producer = session.createProducer(null))
        {
            producer.send(session.createQueue(allowedDestinationName), session.createTextMessage("test1"));
            session.commit();
        }

        try (final var connection2 = jms.builder().connection().syncPublish(true).username(USER1).password(USER1_PASSWORD).create();
             final var session = connection2.createSession(true, Session.SESSION_TRANSACTED))
        {
            final var e = assertThrows(JMSException.class, () ->
            {
                final var producer = session.createProducer(null);
                producer.send(session.createQueue(deniedDestinationName), session.createTextMessage("test2"));
            }, "Sending should fail");

            assertJMSExceptionMessageContains(e,
                    "Permission PERFORM_ACTION(publish) is denied for : %s".formatted("Queue"));

            assertThrows(JMSException.class, session::commit, "Commit should fail");
        }
    }

    @Test
    void publishIntoDeniedDestinationFails(final JmsSupport jms) throws Exception
    {
        final String deniedDestinationName = "deniedQueue";
        jms.builder().queue(deniedDestinationName).create();

        configureACL(jms, "ACL ALLOW-LOG %s ACCESS VIRTUALHOST".formatted(USER1),
                     "ACL DENY-LOG %s PUBLISH EXCHANGE name=\"*\" routingKey=\"%s\"".formatted(USER1, deniedDestinationName));

        try (final var connection = jms.builder().connection().syncPublish(true).username(USER1).password(USER1_PASSWORD).create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED))
        {
            final var e = assertThrows(JMSException.class, () ->
            {
                final var producer = session.createProducer(session.createQueue(deniedDestinationName));
                producer.send(session.createTextMessage("test"));
            }, "Sending should fail");

            assertJMSExceptionMessageContains(e,
                    "Permission PERFORM_ACTION(publish) is denied for : %s".formatted("Queue"));
        }
    }

    private void assertJMSExceptionMessageContains(final JMSException e, final String expectedMessage)
    {
        final Set<Throwable> examined = new HashSet<>();
        Throwable current = e;
        do
        {
            if (current.getMessage().contains(expectedMessage))
            {
                return;
            }
            examined.add(current);
            current = current.getCause();
        }
        while (current != null && !examined.contains(current));
        LOGGER.error("Exception thrown", e);
        fail("Unexpected message. Root exception : %s. Expected root or underlying(s) to contain : %s".formatted(e.getMessage(), expectedMessage));
    }

    private void configureACL(final JmsSupport jms, final String... rules) throws Exception
    {
        final EventLoggerProvider eventLoggerProvider = mock(EventLoggerProvider.class);
        final EventLogger eventLogger = mock(EventLogger.class);
        when(eventLoggerProvider.getEventLogger()).thenReturn(eventLogger);

        final List<AclRule> aclRules = new ArrayList<>();
        try (final StringReader stringReader = new StringReader(String.join(LINE_SEPARATOR, rules)))
        {
            final RuleSet ruleSet = AclFileParser.parse(stringReader, eventLoggerProvider);
            for (final Rule rule: ruleSet)
            {
                aclRules.add(rule.asAclRule());
            }
        }

        configureACL(jms, aclRules.toArray(new AclRule[0]));
    }

    private void configureACL(final JmsSupport jms, final AclRule... rules) throws Exception
    {
        final String serializedRules = ConfiguredObjectJacksonModule.newObjectMapper(false).writeValueAsString(rules);
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(RuleBasedVirtualHostAccessControlProvider.RULES, serializedRules);
        attributes.put(RuleBasedVirtualHostAccessControlProvider.DEFAULT_RESULT, "DENIED");
        jms.management().create("acl", RULE_BASED_VIRTUAL_HOST_ACCESS_CONTROL_PROVIDER, attributes);
    }

    private void createGroupProvider(final JmsSupport jms, final String groupName, final String... groupMembers) throws Exception
    {
        final String groupProviderName = "groups";
        try (final var connection = jms.builder().connection().virtualHost("$management").create())
        {
            connection.start();

            jms.management(connection)
                    .create(groupProviderName, GroupProviderImpl.class.getName(), Map.of());
            jms.management(connection)
                    .create(groupName, Group.class.getName(), Map.of("object-path", groupProviderName));

            for (final String groupMember : groupMembers)
            {
                jms.management(connection)
                        .create(groupMember, GroupMember.class.getName(), Map.of("object-path", groupProviderName + "/" + groupName));
            }
        }
    }

    private void assertConnection(final Connection connection) throws JMSException
    {
        assertNotNull(connection.createSession(false, Session.AUTO_ACKNOWLEDGE),
                "create session should be successful");
    }

    private void assertAccessDeniedException(JMSException e)
    {
        assertTrue(e.getMessage().contains("Permission PERFORM_ACTION(connect) is denied"),
                "Unexpected exception message:" + e.getMessage());
        assertTrue(e.getMessage().contains("amqp:not-allowed"),
                "Unexpected error condition reported:" + e.getMessage());
    }
}
