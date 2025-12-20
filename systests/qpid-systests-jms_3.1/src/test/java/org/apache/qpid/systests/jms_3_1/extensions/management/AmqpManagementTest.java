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

package org.apache.qpid.systests.jms_3_1.extensions.management;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.server.model.Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES;
import static org.apache.qpid.systests.EntityTypes.BROKER;
import static org.apache.qpid.systests.EntityTypes.CONNECTION;
import static org.apache.qpid.systests.EntityTypes.DIRECT_EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.EXCHANGE;
import static org.apache.qpid.systests.EntityTypes.JSON_VIRTUALHOST_NODE;
import static org.apache.qpid.systests.EntityTypes.LAST_VALUE_QUEUE;
import static org.apache.qpid.systests.EntityTypes.PORT;
import static org.apache.qpid.systests.EntityTypes.PRIORITY_QUEUE;
import static org.apache.qpid.systests.EntityTypes.QUEUE;
import static org.apache.qpid.systests.EntityTypes.SORTED_QUEUE;
import static org.apache.qpid.systests.EntityTypes.STANDARD_QUEUE;
import static org.apache.qpid.systests.EntityTypes.VIRTUALHOST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Session;
import javax.naming.NamingException;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.queue.PriorityQueue;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.jms_3_1.extensions.BrokerManagementHelper;
import org.apache.qpid.systests.jms_3_1.extensions.TlsHelper;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.tests.utils.BrokerAdmin;

@JmsSystemTest
@Tag("management")
class AmqpManagementTest
{
    // test get types on $management
    @Test
    void getTypesOnBrokerManagement(final JmsSupport jms) throws Exception
    {
        try (final var connection = getBrokerManagementConnection(jms);
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createBytesMessage();

                message.setStringProperty("identity", "self");
                message.setStringProperty("type", "org.amqp.management");
                message.setStringProperty("operation", "GET-TYPES");

                message.setJMSReplyTo(replyAddress);

                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
                checkResponseIsMapType(responseMessage);
                assertNotNull(getValueFromMapResponse(responseMessage, "org.amqp.management"),
                        "The response did not include the org.amqp.Management type");
                assertNotNull(getValueFromMapResponse(responseMessage, PORT),
                        "The response did not include the org.apache.qpid.Port type");
            }
        }
    }

    // test get types on $management
    @Test
    void queryBrokerManagement(final JmsSupport jms) throws Exception
    {
        try (final var connection = getBrokerManagementConnection(jms);
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                // Basic QUERY against broker management
                final var basicQuery = session.createMapMessage();

                basicQuery.setStringProperty("identity", "self");
                basicQuery.setStringProperty("type", "org.amqp.management");
                basicQuery.setStringProperty("operation", "QUERY");
                basicQuery.setObject("attributeNames", "[]");
                basicQuery.setJMSReplyTo(replyAddress);

                producer.send(basicQuery);

                var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
                assertEquals(basicQuery.getJMSMessageID(), responseMessage.getJMSCorrelationID(),
                        "The correlation id does not match the sent message's messageId");
                checkResponseIsMapType(responseMessage);
                List<String> resultMessageKeys = new ArrayList<>(getMapResponseKeys(responseMessage));
                assertEquals(2, resultMessageKeys.size(), "The response map has two entries");
                assertTrue(resultMessageKeys.contains("attributeNames"),
                        "The response map does not contain attribute names");
                assertTrue(resultMessageKeys.contains("results"), "The response map does not contain results ");
                var attributeNames = getValueFromMapResponse(responseMessage, "attributeNames");
                assertInstanceOf(Collection.class, attributeNames, "The attribute names are not a list");
                Collection attributeNamesCollection = (Collection) attributeNames;
                assertTrue(attributeNamesCollection.contains("identity"),
                        "The attribute names do not contain identity");
                assertTrue(attributeNamesCollection.contains("name"), "The attribute names do not contain name");
                assertTrue(attributeNamesCollection.contains("qpid-type"),
                        "The attribute names do not contain qpid-type");

                // Create a unique direct exchange and then verify it appears in both:
                //  - QUERY entityType=org.apache.qpid.Exchange
                //  - QUERY entityType=org.apache.qpid.DirectExchange
                // This avoids comparing two potentially different snapshots of broker state when tests run in parallel.
                final String exchangeName = "direct-" + jms.testMethodName() + "-" + UUID.randomUUID();
                final String exchangePath = jms.virtualHostName() + "/" + jms.virtualHostName() + "/" + exchangeName;
                Object createdExchangeIdentity;

                try
                {
                    final var createExchange = session.createMapMessage();
                    createExchange.setStringProperty("type", EXCHANGE);
                    createExchange.setStringProperty("operation", "CREATE");
                    createExchange.setString("name", exchangeName);
                    createExchange.setString("qpid-type", "direct");
                    createExchange.setString("object-path", exchangePath);
                    createExchange.setJMSReplyTo(replyAddress);
                    producer.send(createExchange);

                    final var createResponse = consumer.receive(Timeouts.receiveMillis());
                    assertResponseCode(createResponse, 201);
                    checkResponseIsMapType(createResponse);
                    createdExchangeIdentity = getValueFromMapResponse(createResponse, "identity");
                    assertNotNull(createdExchangeIdentity, "The created exchange identity was not returned");

                    // Now test filtering by type: Exchange
                    final var exchangeQuery = session.createMapMessage();
                    exchangeQuery.setStringProperty("identity", "self");
                    exchangeQuery.setStringProperty("type", "org.amqp.management");
                    exchangeQuery.setStringProperty("operation", "QUERY");
                    exchangeQuery.setStringProperty("entityType", EXCHANGE);
                    exchangeQuery.setObject("attributeNames", "[\"name\", \"identity\", \"type\"]");
                    exchangeQuery.setJMSReplyTo(replyAddress);
                    producer.send(exchangeQuery);

                    responseMessage = consumer.receive(Timeouts.receiveMillis());
                    assertResponseCode(responseMessage, 200);
                    checkResponseIsMapType(responseMessage);
                    assertEquals(exchangeQuery.getJMSMessageID(), responseMessage.getJMSCorrelationID(),
                            "The correlation id does not match the sent message's messageId");
                    resultMessageKeys = new ArrayList<>(getMapResponseKeys(responseMessage));
                    assertEquals(2, resultMessageKeys.size(), "The response map has two entries");
                    assertTrue(resultMessageKeys.contains("attributeNames"),
                            "The response map does not contain attribute names");
                    assertTrue(resultMessageKeys.contains("results"), "The response map does not contain results ");
                    attributeNames = getValueFromMapResponse(responseMessage, "attributeNames");
                    assertInstanceOf(Collection.class, attributeNames, "The attribute names are not a list");
                    attributeNamesCollection = (Collection) attributeNames;
                    assertEquals(Arrays.asList("name", "identity", "type"), attributeNamesCollection,
                            "The attributeNames are no as expected");
                    final var resultsObject = getValueFromMapResponse(responseMessage, "results");
                    assertInstanceOf(Collection.class, resultsObject, "results is not a collection");
                    final Collection results = (Collection) resultsObject;
                    assertTrue(results.size() >= 4, "results should have at least 4 elements");

                    final String createdIdentityAsString = String.valueOf(createdExchangeIdentity);
                    boolean foundInAllExchanges = false;
                    for (final Object rowObject : results)
                    {
                        if (rowObject instanceof List)
                        {
                            final List row = (List) rowObject;
                            if (row.size() > 1)
                            {
                                final Object rowIdentity = row.get(1);
                                if (createdExchangeIdentity.equals(rowIdentity) ||
                                        createdIdentityAsString.equals(String.valueOf(rowIdentity)))
                                {
                                    foundInAllExchanges = true;
                                    break;
                                }
                            }
                        }
                        else if (rowObject instanceof Map)
                        {
                            final Object rowIdentity = ((Map) rowObject).get("identity");
                            if (createdExchangeIdentity.equals(rowIdentity) ||
                                    createdIdentityAsString.equals(String.valueOf(rowIdentity)))
                            {
                                foundInAllExchanges = true;
                                break;
                            }
                        }
                    }
                    assertTrue(foundInAllExchanges,
                            "The created direct exchange was not present in the results when querying for all exchanges");

                    // Now test filtering by type: DirectExchange
                    final var directExchangeQuery = session.createMapMessage();
                    directExchangeQuery.setStringProperty("identity", "self");
                    directExchangeQuery.setStringProperty("type", "org.amqp.management");
                    directExchangeQuery.setStringProperty("operation", "QUERY");
                    directExchangeQuery.setStringProperty("entityType", DIRECT_EXCHANGE);
                    directExchangeQuery.setObject("attributeNames", "[\"name\", \"identity\", \"type\"]");
                    directExchangeQuery.setJMSReplyTo(replyAddress);
                    producer.send(directExchangeQuery);

                    responseMessage = consumer.receive(Timeouts.receiveMillis());
                    assertResponseCode(responseMessage, 200);
                    checkResponseIsMapType(responseMessage);
                    assertEquals(directExchangeQuery.getJMSMessageID(), responseMessage.getJMSCorrelationID(),
                            "The correlation id does not match the sent message's messageId");

                    final Object directAttributeNames = getValueFromMapResponse(responseMessage, "attributeNames");
                    assertInstanceOf(Collection.class, directAttributeNames, "The attribute names are not a list");
                    final Collection directAttributeNamesCollection = (Collection) directAttributeNames;
                    assertEquals(Arrays.asList("name", "identity", "type"), directAttributeNamesCollection,
                            "The attributeNames are no as expected");

                    final Object directResultsObject = getValueFromMapResponse(responseMessage, "results");
                    assertInstanceOf(Collection.class, directResultsObject, "results is not a collection");
                    final Collection directResults = (Collection) directResultsObject;

                    boolean foundInDirectExchanges = false;
                    for (final Object rowObject : directResults)
                    {
                        if (rowObject instanceof List)
                        {
                            final List row = (List) rowObject;
                            if (row.size() > 1)
                            {
                                final Object rowIdentity = row.get(1);
                                if (createdExchangeIdentity.equals(rowIdentity) ||
                                        createdIdentityAsString.equals(String.valueOf(rowIdentity)))
                                {
                                    foundInDirectExchanges = true;
                                    break;
                                }
                            }
                        }
                        else if (rowObject instanceof Map)
                        {
                            final Object rowIdentity = ((Map) rowObject).get("identity");
                            if (createdExchangeIdentity.equals(rowIdentity) ||
                                    createdIdentityAsString.equals(String.valueOf(rowIdentity)))
                            {
                                foundInDirectExchanges = true;
                                break;
                            }
                        }
                    }

                    assertTrue(foundInDirectExchanges,
                            "The created direct exchange was not present in the results when querying for direct exchanges");
                }
                finally
                {
                    // Best-effort cleanup to reduce inter-test interference when running in parallel.
                    // Do not assert on cleanup to avoid masking the real test failure.
                    try
                    {
                        final var deleteExchange = session.createMapMessage();
                        deleteExchange.setStringProperty("type", EXCHANGE);
                        deleteExchange.setStringProperty("operation", "DELETE");
                        deleteExchange.setObjectProperty("index", "object-path");
                        deleteExchange.setObjectProperty("key", exchangePath);
                        deleteExchange.setJMSReplyTo(replyAddress);
                        producer.send(deleteExchange);
                        consumer.receive(Timeouts.receiveMillis());
                    }
                    catch (final Exception e)
                    {
                        // ignore
                    }
                }
            }
        }
    }

    // test get types on a virtual host
    @Test
    void getTypesOnVirtualHostManagement(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createBytesMessage();

                message.setStringProperty("identity", "self");
                message.setStringProperty("type", "org.amqp.management");
                message.setStringProperty("operation", "GET-TYPES");
                final String correlationID = "some correlation id";
                message.setJMSCorrelationIDAsBytes(correlationID.getBytes(UTF_8));

                message.setJMSReplyTo(replyAddress);

                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(responseMessage, "A response message was not sent");
                assertEquals(correlationID, responseMessage.getJMSCorrelationID(),
                        "The correlation id does not match the sent message's correlationId");

                assertResponseCode(responseMessage, 200);
                assertNotNull(getValueFromMapResponse(responseMessage, "org.amqp.management"),
                        "The response did not include the org.amqp.Management type");
                assertNull(getValueFromMapResponse(responseMessage, PORT),
                        "The response included the org.apache.qpid.Port type");
            }
        }
    }

    // create / update / read / delete a queue via $management
    @Test
    void createQueueOnBrokerManagement(final JmsSupport jms) throws Exception
    {
        try (final var connection = getBrokerManagementConnection(jms);
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                var message = session.createMapMessage();

                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "CREATE");
                message.setString("name", jms.testMethodName());
                message.setLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 100L);
                final String path = jms.virtualHostName() + "/" + jms.virtualHostName() + "/" + jms.testMethodName();
                message.setString("object-path", path);
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 201);
                checkResponseIsMapType(responseMessage);
                assertEquals(STANDARD_QUEUE, getValueFromMapResponse(responseMessage, "type"),
                        "The created queue was not a standard queue");
                assertEquals("standard", getValueFromMapResponse(responseMessage, "qpid-type"),
                        "The created queue was not a standard queue");
                assertEquals(100L, getValueFromMapResponse(responseMessage, ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES),
                        "the created queue did not have the correct alerting threshold");
                final var identity = getValueFromMapResponse(responseMessage, "identity");

                message = session.createMapMessage();

                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "UPDATE");
                message.setObjectProperty("identity", identity);
                message.setLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 250L);

                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
                checkResponseIsMapType(responseMessage);
                assertEquals(250L, getValueFromMapResponse(responseMessage, ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES),
                        "the created queue did not have the correct alerting threshold");

                message = session.createMapMessage();

                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "DELETE");
                message.setObjectProperty("index", "object-path");
                message.setObjectProperty("key", path);

                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 204);

                message = session.createMapMessage();

                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "READ");
                message.setObjectProperty("identity", identity);

                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 404);
            }
        }
    }

    // create / update / read / delete a queue via vhost
    @Test
    void createQueueOnVirtualHostManagement(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                var message = session.createMapMessage();

                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "CREATE");
                message.setString("name", jms.testMethodName());
                message.setInt(PriorityQueue.PRIORITIES, 13);
                final String path = jms.testMethodName();
                message.setString("object-path", path);
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 201);
                checkResponseIsMapType(responseMessage);
                assertEquals(PRIORITY_QUEUE, getValueFromMapResponse(responseMessage, "type"),
                        "The created queue was not a priority queue");
                assertEquals("priority", getValueFromMapResponse(responseMessage, "qpid-type"),
                        "The created queue was not a standard queue");
                assertEquals(13, Integer.valueOf(getValueFromMapResponse(responseMessage, PriorityQueue.PRIORITIES).toString())
                                .intValue(),
                        "the created queue did not have the correct number of priorities");
                final var identity = getValueFromMapResponse(responseMessage, "identity");

                // Trying to create a second queue with the same name should cause a conflict
                message = session.createMapMessage();

                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "CREATE");
                message.setString("name", jms.testMethodName());
                message.setInt(PriorityQueue.PRIORITIES, 7);
                message.setString("object-path", jms.testMethodName());
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 409);

                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "READ");
                message.setObjectProperty("identity", identity);

                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
                assertEquals(13, Integer.valueOf(getValueFromMapResponse(responseMessage, PriorityQueue.PRIORITIES).toString())
                                .intValue(),
                        "the queue did not have the correct number of priorities");
                assertEquals(jms.testMethodName(), getValueFromMapResponse(responseMessage, "object-path"),
                        "the queue did not have the expected path");

                message = session.createMapMessage();

                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "UPDATE");
                message.setObjectProperty("identity", identity);
                message.setLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 250L);

                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
                checkResponseIsMapType(responseMessage);
                assertEquals(250L, Long.valueOf(getValueFromMapResponse(responseMessage,
                                ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES).toString()).longValue(),
                        "The updated queue did not have the correct alerting threshold");

                message = session.createMapMessage();
                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "DELETE");
                message.setObjectProperty("index", "object-path");
                message.setObjectProperty("key", path);

                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 204);

                message = session.createMapMessage();
                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "DELETE");
                message.setObjectProperty("index", "object-path");
                message.setObjectProperty("key", path);

                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 404);
            }
        }
    }

    // read virtual host from virtual host management
    @Test
    void readVirtualHost(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", VIRTUALHOST);
                message.setStringProperty("operation", "READ");
                message.setStringProperty("index", "object-path");
                message.setStringProperty("key", "");
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
                checkResponseIsMapType(responseMessage);
                assertEquals(jms.virtualHostName(), getValueFromMapResponse(responseMessage, "name"),
                        "The name of the virtual host is not as expected");

                message.setBooleanProperty("actuals", false);
                producer.send(message);
                responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
                checkResponseIsMapType(responseMessage);
                assertNotNull(getValueFromMapResponse(responseMessage, "productVersion"),
                        "Derived attribute (productVersion) should be available");
            }
        }
    }

    @Test
    void readObject_ObjectNotFound(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", EXCHANGE);
                message.setStringProperty("operation", "READ");
                message.setStringProperty("index", "object-path");
                message.setStringProperty("key", "not-found-exchange");
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 404);
            }
        }
    }

    @Test
    void invokeOperation_ObjectNotFound(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", EXCHANGE);
                message.setStringProperty("operation", "getStatistics");
                message.setStringProperty("index", "object-path");
                message.setStringProperty("key", "not-found-exchange");
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 404);
            }
        }
    }

    @Test
    void invokeOperationReturningMap(final JmsSupport jms) throws Exception
    {
        try (final var connection = getBrokerManagementConnection(jms);
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", BROKER);
                message.setStringProperty("operation", "getStatistics");
                message.setStringProperty("index", "object-path");
                message.setStringProperty("key", "");
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
                checkResponseIsMapType(responseMessage);
                assertNotNull(getValueFromMapResponse(responseMessage, "numberOfLiveThreads"));
            }
        }
    }

    @Test
    void invokeOperationReturningManagedAttributeValue(final JmsSupport jms) throws Exception
    {
        try (final var connection = getBrokerManagementConnection(jms);
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", BROKER);
                message.setStringProperty("operation", "getConnectionMetaData");
                message.setStringProperty("index", "object-path");
                message.setStringProperty("key", "");
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
                checkResponseIsMapType(responseMessage);
                assertNotNull(getValueFromMapResponse(responseMessage, "port"));
            }
        }
    }

    @Test
    void invokeSecureOperation(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final TlsHelper tlsHelper = new TlsHelper(tls);

        final String secureOperation = "publishMessage";  // // a secure operation
        final Map<String, String> operationArg = new HashMap<>();
        operationArg.put("address", ExchangeDefaults.FANOUT_EXCHANGE_NAME);
        operationArg.put("content", "Hello, world!");

        try (final var unsecuredConnection = jms.builder().connection().create();
             final var session = unsecuredConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            unsecuredConnection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var plainRequest = session.createMapMessage();

                plainRequest.setStringProperty("type", VIRTUALHOST);
                plainRequest.setStringProperty("operation", secureOperation);
                plainRequest.setStringProperty("index", "object-path");
                plainRequest.setStringProperty("key", "");
                plainRequest.setStringProperty("message", new ObjectMapper().writeValueAsString(operationArg));
                plainRequest.setJMSReplyTo(replyAddress);
                producer.send(plainRequest);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 403);
            }
        }

        int tlsPort = 0;
        final String portName = jms.testMethodName() + "TlsPort";
        final String keyStoreName = portName + "KeyStore";
        final String trustStoreName = portName + "TrustStore";
        try (final BrokerManagementHelper helper = new BrokerManagementHelper(jms))
        {
            helper.openManagementConnection();

            final String authenticationManager =
                    helper.getAuthenticationProviderNameForAmqpPort(jms.brokerAdmin().getBrokerAddress(
                            BrokerAdmin.PortType.AMQP)
                                                                                    .getPort());
            tlsPort = helper.createKeyStore(keyStoreName, tlsHelper.getBrokerKeyStore(), tls.getSecret())
                    .createTrustStore(trustStoreName, tlsHelper.getBrokerTrustStore(), tls.getSecret())
                    .createAmqpTlsPort(portName, authenticationManager, keyStoreName, false,
                            false, false, trustStoreName).getAmqpBoundPort(portName);
        }

        try (final var connection = jms.builder().connection().tls(true)
                .port(tlsPort)
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var secureRequest = session.createMapMessage();

                secureRequest.setStringProperty("type", VIRTUALHOST);
                secureRequest.setStringProperty("operation", secureOperation);
                secureRequest.setStringProperty("index", "object-path");
                secureRequest.setStringProperty("key", "");
                secureRequest.setStringProperty("message", new ObjectMapper().writeValueAsString(operationArg));
                secureRequest.setJMSReplyTo(replyAddress);
                producer.send(secureRequest);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
            }
        }
    }

    // create a virtual host from $management
    @Test
    void createVirtualHost(final JmsSupport jms) throws Exception
    {
        final String virtualHostName = "newMemoryVirtualHost";
        try (final var connection = getBrokerManagementConnection(jms);
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", JSON_VIRTUALHOST_NODE);
                message.setStringProperty("operation", "CREATE");
                message.setString("name", virtualHostName);
                message.setString(VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION, "{ \"type\" : \"Memory\" }");
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 201);
            }
        }

        try (final var virtualHostConnection = jms.builder().connection().virtualHost(virtualHostName).create();
             final var session = virtualHostConnection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            virtualHostConnection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", VIRTUALHOST);
                message.setStringProperty("operation", "READ");
                message.setStringProperty("index", "object-path");
                message.setStringProperty("key", "");
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 200);
                checkResponseIsMapType(responseMessage);
                assertEquals(virtualHostName, getValueFromMapResponse(responseMessage, "name"),
                        "The name of the virtual host is not as expected");
                assertEquals("Memory", getValueFromMapResponse(responseMessage, "qpid-type"),
                        "The type of the virtual host is not as expected");
            }
        }
    }

    // attempt to delete the virtual host via the virtual host
    @Test
    void deleteVirtualHost(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", VIRTUALHOST);
                message.setStringProperty("operation", "DELETE");
                message.setStringProperty("index", "object-path");
                message.setStringProperty("key", "");
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 501);
            }
        }
    }

    // create a queue with the qpid type
    @Test
    void createQueueWithQpidType(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", QUEUE);
                message.setStringProperty("operation", "CREATE");
                message.setString("name", jms.testMethodName());
                message.setString("qpid-type", "lvq");
                String path = jms.testMethodName();
                message.setString("object-path", path);
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 201);
                checkResponseIsMapType(responseMessage);
                assertEquals(LAST_VALUE_QUEUE, getValueFromMapResponse(responseMessage, "type"),
                        "The created queue did not have the correct type");
            }
        }
    }

    // create a queue using the AMQP type
    @Test
    void createQueueWithAmqpType(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", SORTED_QUEUE);
                message.setStringProperty("operation", "CREATE");
                message.setString("name", jms.testMethodName());
                final String path = jms.testMethodName();
                message.setString("object-path", path);
                message.setString("sortKey", "foo");
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 201);
                checkResponseIsMapType(responseMessage);
                assertEquals("sorted", getValueFromMapResponse(responseMessage, "qpid-type"),
                        "The created queue did not have the correct type");
            }
        }
    }

    // attempt to create an exchange without a type
    @Test
    void createExchangeWithoutType(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", EXCHANGE);
                message.setStringProperty("operation", "CREATE");
                message.setString("name", jms.testMethodName());
                final String path = jms.testMethodName();
                message.setString("object-path", path);
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 400);
            }
        }
    }

    // attempt to create a connection
    @Test
    void createConnectionOnVirtualHostManagement(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", CONNECTION);
                message.setStringProperty("operation", "CREATE");
                message.setString("name", jms.testMethodName());
                final String path = jms.testMethodName();
                message.setString("object-path", path);
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 501);
            }
        }
    }

    @Test
    void createConnectionOnBrokerManagement(final JmsSupport jms) throws Exception
    {
        try (final var connection = getBrokerManagementConnection(jms);
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue("$management");
            final var replyAddress = session.createTemporaryQueue();

            try (final var consumer = session.createConsumer(replyAddress);
                 final var producer = session.createProducer(queue))
            {
                final var message = session.createMapMessage();

                message.setStringProperty("type", CONNECTION);
                message.setStringProperty("operation", "CREATE");
                message.setString("name", jms.testMethodName());
                final String path = jms.testMethodName();
                message.setString("object-path", path);
                message.setJMSReplyTo(replyAddress);
                producer.send(message);

                final var responseMessage = consumer.receive(Timeouts.receiveMillis());
                assertResponseCode(responseMessage, 501);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void assertResponseCode(final Message responseMessage, final int expectedResponseCode) throws JMSException
    {
        assertNotNull(responseMessage, "A response message was not sent");
        assertTrue(Collections.list(responseMessage.getPropertyNames()).contains("statusCode"),
                "The response message does not have a status code");
        assertEquals(expectedResponseCode, responseMessage.getIntProperty("statusCode"),
                "The response code did not indicate success");
    }


    private Connection getBrokerManagementConnection(final JmsSupport jms) throws NamingException, JMSException
    {
        return jms.builder().connection().virtualHost("$management")
                .clientId(UUID.randomUUID().toString())
                .create();
    }

    private void checkResponseIsMapType(final Message responseMessage) throws JMSException
    {
        if (!(responseMessage instanceof MapMessage)
                && !(responseMessage instanceof ObjectMessage
                && ((ObjectMessage) responseMessage).getObject() instanceof Map))
        {
            fail("The response was neither a Map Message nor an Object Message containing a Map. It was a : %s ".formatted(responseMessage.getClass()));
        }
    }

    private Object getValueFromMapResponse(final Message responseMessage, String name) throws JMSException
    {
        if (responseMessage instanceof ObjectMessage)
        {
            return ((Map)((ObjectMessage)responseMessage).getObject()).get(name);
        }
        else
        {
            return ((MapMessage) responseMessage).getObject(name);
        }
    }

    @SuppressWarnings("unchecked")
    private Collection<String> getMapResponseKeys(final Message responseMessage) throws JMSException
    {
        if (responseMessage instanceof ObjectMessage)
        {
            return ((Map)((ObjectMessage)responseMessage).getObject()).keySet();
        }
        else
        {
            return Collections.list(((MapMessage) responseMessage).getMapNames());
        }
    }
}
