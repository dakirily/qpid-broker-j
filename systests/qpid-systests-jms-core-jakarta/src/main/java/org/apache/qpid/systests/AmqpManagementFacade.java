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

package org.apache.qpid.systests;

import jakarta.jms.BytesMessage;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Helper for interacting with the broker's AMQP management node (typically {@code $management}) using JMS messages.
 *
 * <p>This facade is designed for use in system/integration tests. It can create, update, delete and read configured
 * objects via management operations, as well as perform arbitrary management operations and queries.
 *
 * <p>For operations that expect a reply, the facade creates a temporary queue, sets it as {@code JMSReplyTo},
 * waits for a response and parses the response message payload.
 */
public class AmqpManagementFacade
{
    private static final ObjectMapper OBJECT_MAPPER = ConfiguredObjectJacksonModule.newObjectMapper(false);

    private static final String MANAGEMENT_ADDRESS = "$management";

    /**
     * Sends a management {@code CREATE} request for an entity of the given type with no extra attributes.
     *
     * @param name entity object-path/name
     * @param session JMS session to use
     * @param type fully qualified configured object type (e.g. {@code org.apache.qpid.Queue})
     */
    public void createEntityUsingAmqpManagement(final String name, final Session session, final String type)
            throws JMSException
    {
        createEntityUsingAmqpManagement(name, session, type, Collections.emptyMap());
    }

    /**
     * Sends a management {@code CREATE} request for an entity of the given type with the provided attributes.
     *
     * <p>This method does not wait for or validate a response.
     *
     * @param name entity object-path/name
     * @param session JMS session to use
     * @param type fully qualified configured object type (e.g. {@code org.apache.qpid.Queue})
     * @param attributes entity attributes to set
     */
    public void createEntityUsingAmqpManagement(final String name,
                                                final Session session,
                                                final String type,
                                                final Map<String, Object> attributes)
            throws JMSException
    {
        try (final var producer = session.createProducer(session.createQueue(MANAGEMENT_ADDRESS)))
        {
            final MapMessage createMessage = session.createMapMessage();
            createMessage.setStringProperty("type", type);
            createMessage.setStringProperty("operation", "CREATE");
            createMessage.setString("name", name);
            createMessage.setString("object-path", name);
            for (final Map.Entry<String, Object> entry : attributes.entrySet())
            {
                createMessage.setObject(entry.getKey(), entry.getValue());
            }
            producer.send(createMessage);
            commitIfTransacted(session);
        }
    }

    /**
     * Sends a management {@code CREATE} request and returns the decoded response body as a map.
     *
     * <p>The request is expected to succeed with HTTP-like status code {@code 201}.
     *
     * @param name entity object-path/name
     * @param type fully qualified configured object type
     * @param attributes entity attributes to set
     * @param session JMS session to use
     * @return response body as a map (empty if the response has no map-like payload)
     */
    public Map<String, Object> createEntityAndAssertResponse(final String name,
                                                             final String type,
                                                             final Map<String, Object> attributes,
                                                             final Session session) throws JMSException
    {
        final long timeoutMillis = Timeouts.receiveMillis();
        try (final TemporaryQueueResource replyQueue = TemporaryQueueResource.create(session);
             final MessageConsumer consumer = session.createConsumer(replyQueue.queue());
             final MessageProducer producer = session.createProducer(session.createQueue(MANAGEMENT_ADDRESS)))
        {
            final MapMessage createMessage = session.createMapMessage();
            createMessage.setStringProperty("type", type);
            createMessage.setStringProperty("operation", "CREATE");
            createMessage.setString("name", name);
            createMessage.setString("object-path", name);
            createMessage.setJMSReplyTo(replyQueue.queue());

            for (final Map.Entry<String, Object> entry : attributes.entrySet())
            {
                createMessage.setObject(entry.getKey(), entry.getValue());
            }

            producer.send(createMessage);
            commitIfTransacted(session);

            try
            {
                return receiveManagementResponseAsMap(consumer, 201, timeoutMillis,
                        "CREATE " + type + " '" + name + "'");
            }
            finally
            {
                // In a transacted session, consuming the reply is part of the transaction.
                commitIfTransacted(session);
            }
        }
    }

    /**
     * Sends a management {@code UPDATE} request and returns the decoded response body as a map.
     *
     * <p>The request is expected to succeed with HTTP-like status code {@code 200}.
     *
     * @param name entity object-path/name
     * @param type fully qualified configured object type
     * @param attributes entity attributes to update
     * @param session JMS session to use
     * @return response body as a map (empty if the response has no map-like payload)
     */
    public Map<String, Object> updateEntityUsingAmqpManagementAndReceiveResponse(final String name,
                                                                                 final String type,
                                                                                 final Map<String, Object> attributes,
                                                                                 final Session session)
            throws JMSException
    {
        final long timeoutMillis = Timeouts.receiveMillis();
        try (final TemporaryQueueResource replyQueue = TemporaryQueueResource.create(session);
             final MessageConsumer consumer = session.createConsumer(replyQueue.queue()))
        {
            updateEntityUsingAmqpManagement(name, type, attributes, replyQueue.queue(), session);

            try
            {
                return receiveManagementResponseAsMap(consumer, 200, timeoutMillis,
                        "UPDATE " + type + " '" + name + "'");
            }
            finally
            {
                commitIfTransacted(session);
            }
        }
    }

    /**
     * Sends a management {@code UPDATE} request for an entity.
     *
     * <p>This method does not wait for or validate a response.
     *
     * @param name entity object-path/name
     * @param session JMS session to use
     * @param type fully qualified configured object type
     * @param attributes entity attributes to update
     */
    public void updateEntityUsingAmqpManagement(final String name,
                                                final Session session,
                                                final String type,
                                                final Map<String, Object> attributes) throws JMSException
    {
        updateEntityUsingAmqpManagement(name, type, attributes, null, session);
    }

    /**
     * Sends a management {@code DELETE} request for an entity.
     *
     * <p>This method does not wait for or validate a response.
     *
     * @param name entity object-path/name
     * @param session JMS session to use
     * @param type fully qualified configured object type
     */
    public void deleteEntityUsingAmqpManagement(final String name, final Session session, final String type)
            throws JMSException
    {
        try (final MessageProducer producer = session.createProducer(session.createQueue(MANAGEMENT_ADDRESS)))
        {
            final MapMessage createMessage = session.createMapMessage();
            createMessage.setStringProperty("type", type);
            createMessage.setStringProperty("operation", "DELETE");
            createMessage.setStringProperty("index", "object-path");

            createMessage.setStringProperty("key", name);
            producer.send(createMessage);
            commitIfTransacted(session);
        }
    }

    /**
     * Performs an arbitrary management operation on a configured object and returns the decoded response body.
     *
     * <p>The method validates that the response status code is in the {@code 2xx} range.
     * Depending on the response message type, the return value can be one of:
     * <ul>
     *     <li>{@link java.util.List} (for {@link StreamMessage})</li>
     *     <li>{@link java.util.Map} (for {@link MapMessage})</li>
     *     <li>Any {@link java.lang.Object} (for {@link ObjectMessage})</li>
     *     <li>{@code byte[]} (for {@link BytesMessage})</li>
     *     <li>{@code null} (for an empty {@link BytesMessage})</li>
     * </ul>
     *
     * @param name entity object-path/name
     * @param operation management operation name (e.g. {@code getStatistics})
     * @param session JMS session to use
     * @param type fully qualified configured object type
     * @param arguments operation arguments
     * @return decoded response payload
     */
    public Object performOperationUsingAmqpManagement(final String name,
                                                      final String operation,
                                                      final Session session,
                                                      final String type,
                                                      final Map<String, Object> arguments)
            throws JMSException
    {
        final long timeoutMillis = Timeouts.receiveMillis();
        try (final TemporaryQueueResource replyQueue = TemporaryQueueResource.create(session);
             final MessageConsumer consumer = session.createConsumer(replyQueue.queue());
             final MessageProducer producer = session.createProducer(session.createQueue(MANAGEMENT_ADDRESS)))
        {
            final MapMessage opMessage = session.createMapMessage();
            opMessage.setStringProperty("type", type);
            opMessage.setStringProperty("operation", operation);
            opMessage.setStringProperty("index", "object-path");
            opMessage.setJMSReplyTo(replyQueue.queue());

            opMessage.setStringProperty("key", name);
            for (final Map.Entry<String, Object> argument : arguments.entrySet())
            {
                final Object value = argument.getValue();
                if (isJmsPropertyValue(value))
                {
                    opMessage.setObjectProperty(argument.getKey(), value);
                }
                else
                {
                    opMessage.setObjectProperty(argument.getKey(), toJson(value, argument.getKey()));
                }
            }

            producer.send(opMessage);
            commitIfTransacted(session);

            final Message response = receiveRequiredMessage(consumer, timeoutMillis,
                    "OPERATION " + operation + " on " + type + " '" + name + "'");

            try
            {
                validateStatusCodeIs2xx(response, "OPERATION " + operation + " on " + type + " '" + name + "'");

                if (response instanceof StreamMessage bodyStream)
                {
                    final List<Object> result = new ArrayList<>();
                    boolean done = false;
                    do
                    {
                        try
                        {
                            result.add(bodyStream.readObject());
                        }
                        catch (MessageEOFException mfe)
                        {
                            // Expected - end of stream
                            done = true;
                        }
                    }
                    while (!done);
                    return result;
                }
                else if (response instanceof MapMessage bodyMap)
                {
                    final Map<String, Object> result = new TreeMap<>();
                    final Enumeration<?> mapNames = bodyMap.getMapNames();
                    while (mapNames.hasMoreElements())
                    {
                        final String key = String.valueOf(mapNames.nextElement());
                        result.put(key, bodyMap.getObject(key));
                    }
                    return result;
                }
                else if (response instanceof ObjectMessage)
                {
                    return ((ObjectMessage) response).getObject();
                }
                else if (response instanceof BytesMessage bytesMessage)
                {
                    if (bytesMessage.getBodyLength() == 0)
                    {
                        return null;
                    }
                    final byte[] buf = new byte[(int) bytesMessage.getBodyLength()];
                    bytesMessage.readBytes(buf);
                    return buf;
                }

                throw new IllegalArgumentException(
                        "Cannot parse the results from a management operation. JMS response message: " + response);
            }
            finally
            {
                commitIfTransacted(session);
            }
        }
    }

    /**
     * Executes a management {@code QUERY} operation for a configured object type.
     *
     * @param session JMS session to use
     * @param type configured object type to query (e.g. {@code org.apache.qpid.Queue})
     * @return query results where each row is mapped as attribute-name to value
     */
    public List<Map<String, Object>> managementQueryObjects(final Session session, final String type) throws JMSException
    {
        final long timeoutMillis = Timeouts.receiveMillis();

        try (final TemporaryQueueResource replyQueue = TemporaryQueueResource.create(session);
             final MessageConsumer consumer = session.createConsumer(replyQueue.queue()))
        {
            final MapMessage message = session.createMapMessage();
            message.setStringProperty("identity", "self");
            message.setStringProperty("type", "org.amqp.management");
            message.setStringProperty("operation", "QUERY");
            message.setStringProperty("entityType", type);
            message.setString("attributeNames", "[]");
            message.setJMSReplyTo(replyQueue.queue());

            try (final MessageProducer producer = session.createProducer(session.createQueue(MANAGEMENT_ADDRESS)))
            {
                producer.send(message);
            }
            commitIfTransacted(session);

            final Message response = receiveRequiredMessage(consumer, timeoutMillis, "QUERY " + type);
            commitIfTransacted(session);

            // Not all brokers/providers include a status code for QUERY responses; validate if present.
            validateStatusCodeIs2xxIfPresent(response, "QUERY " + type);

            if (response instanceof MapMessage bodyMap)
            {
                @SuppressWarnings("unchecked")
                final List<String> attributeNames = (List<String>) bodyMap.getObject("attributeNames");
                @SuppressWarnings("unchecked")
                final List<List<Object>> attributeValues = (List<List<Object>>) bodyMap.getObject("results");
                return getResultsAsMaps(attributeNames, attributeValues);
            }
            else if (response instanceof ObjectMessage)
            {
                final Object body = ((ObjectMessage) response).getObject();
                if (body instanceof Map)
                {
                    @SuppressWarnings("unchecked")
                    final Map<String, ?> bodyMap = (Map<String, ?>) body;
                    @SuppressWarnings("unchecked")
                    final List<String> attributeNames = (List<String>) bodyMap.get("attributeNames");
                    @SuppressWarnings("unchecked")
                    final List<List<Object>> attributeValues = (List<List<Object>>) bodyMap.get("results");
                    return getResultsAsMaps(attributeNames, attributeValues);
                }
            }

            throw new IllegalArgumentException("Cannot parse the results from a management query");
        }
    }

    /**
     * Sends a management {@code READ} request for an entity and returns the decoded entity state as a map.
     *
     * @param session JMS session to use
     * @param type fully qualified configured object type
     * @param name entity object-path/name
     * @param actuals if {@code true}, request the broker's actual values rather than configured values
     * @return the entity attributes as a map
     */
    public Map<String, Object> readEntityUsingAmqpManagement(final Session session,
                                                             final String type,
                                                             final String name,
                                                             final boolean actuals) throws JMSException
    {
        final long timeoutMillis = Timeouts.receiveMillis();

        try (final TemporaryQueueResource replyQueue = TemporaryQueueResource.create(session);
             final MessageConsumer consumer = session.createConsumer(replyQueue.queue());
             final MessageProducer producer = session.createProducer(session.createQueue(MANAGEMENT_ADDRESS)))
        {
            final MapMessage request = session.createMapMessage();
            request.setStringProperty("type", type);
            request.setStringProperty("operation", "READ");
            request.setString("name", name);
            request.setString("object-path", name);
            request.setStringProperty("index", "object-path");
            request.setStringProperty("key", name);
            request.setBooleanProperty("actuals", actuals);
            request.setJMSReplyTo(replyQueue.queue());

            producer.send(request);
            commitIfTransacted(session);

            final Message response = receiveRequiredMessage(consumer, timeoutMillis, "READ " + type + " '" + name + "'");
            commitIfTransacted(session);

            final int statusCode = getStatusCode(response,
                    "READ " + type + " '" + name + "'");
            if (statusCode == 200)
            {
                return extractBodyAsMap(response);
            }

            throw new OperationUnsuccessfulException(
                    response.getStringProperty("statusDescription"),
                    statusCode);
        }
    }

    /**
     * Reads the depth (queueDepthMessages) for the given queue using the management operation {@code getStatistics}.
     *
     * @param destination queue destination
     * @param session JMS session to use
     * @return queue depth in messages
     */
    public long getQueueDepth(final Queue destination, final Session session) throws Exception
    {
        final String escapedName = getEscapedName(destination);
        final Map<String, Object> arguments = Map.of("statistics", List.of("queueDepthMessages"));
        final Object statistics = performOperationUsingAmqpManagement(escapedName,
                "getStatistics",
                session,
                "org.apache.qpid.Queue",
                arguments);
        @SuppressWarnings("unchecked")
        final Map<String, Object> statisticsMap = (Map<String, Object>) statistics;
        return ((Number) statisticsMap.get("queueDepthMessages")).longValue();
    }

    /**
     * Returns {@code true} if the given queue exists according to the management {@code READ} operation.
     *
     * @param destination queue destination
     * @param session JMS session to use
     * @return {@code true} if the queue exists, {@code false} if the broker reports 404
     */
    public boolean isQueueExist(final Queue destination, final Session session) throws Exception
    {
        final String escapedName = getEscapedName(destination);
        try
        {
            performOperationUsingAmqpManagement(escapedName,
                    "READ",
                    session,
                    "org.apache.qpid.Queue",
                    Collections.emptyMap());
            return true;
        }
        catch (OperationUnsuccessfulException e)
        {
            if (e.getStatusCode() == 404)
            {
                return false;
            }
            throw e;
        }
    }

    /**
     * Waits for a management response and converts the response body to a {@link Map}.
     *
     * <p>The method validates the {@code statusCode} property equals {@code expectedStatusCode}.
     *
     * @param consumer consumer bound to the reply-to destination
     * @param expectedStatusCode expected HTTP-like status code (e.g. 200, 201)
     * @param timeoutMillis receive timeout in milliseconds
     * @param context human readable context used in error messages
     * @return response body as a map (empty if the response does not contain a map-like payload)
     */
    private Map<String, Object> receiveManagementResponseAsMap(final MessageConsumer consumer,
                                                               final int expectedStatusCode,
                                                               final long timeoutMillis,
                                                               final String context) throws JMSException
    {
        final Message response = receiveRequiredMessage(consumer, timeoutMillis, context);

        final int statusCode = getStatusCode(response, context);
        if (statusCode != expectedStatusCode)
        {
            throw new OperationUnsuccessfulException(response.getStringProperty("statusDescription"), statusCode);
        }

        return extractBodyAsMap(response);
    }

    /**
     * Reads a message from the consumer and fails fast if no message arrives within the timeout.
     */
    private Message receiveRequiredMessage(final MessageConsumer consumer,
                                           final long timeoutMillis,
                                           final String context) throws JMSException
    {
        final Message response = consumer.receive(timeoutMillis);
        if (response == null)
        {
            throw new JMSException("Timed out waiting for management response (" + context + ") after "
                    + timeoutMillis + " ms");
        }
        return response;
    }

    /**
     * Extracts a response message body as a map.
     *
     * <p>Supports {@link MapMessage} and {@link ObjectMessage} containing a {@link Map}. Any other message type results
     * in an empty map.
     */
    private Map<String, Object> extractBodyAsMap(final Message response) throws JMSException
    {
        if (response instanceof MapMessage bodyMap)
        {
            final Map<String, Object> result = new HashMap<>();
            final Enumeration<?> keys = bodyMap.getMapNames();
            while (keys.hasMoreElements())
            {
                final String key = String.valueOf(keys.nextElement());
                result.put(key, bodyMap.getObject(key));
            }
            return result;
        }
        else if (response instanceof ObjectMessage)
        {
            final Object body = ((ObjectMessage) response).getObject();
            if (body instanceof Map)
            {
                @SuppressWarnings("unchecked")
                final Map<String, Object> bodyMap = (Map<String, Object>) body;
                return new HashMap<>(bodyMap);
            }
        }

        return Collections.emptyMap();
    }

    /**
     * Updates an entity using the management {@code UPDATE} operation.
     *
     * @param name entity object-path/name
     * @param type fully qualified configured object type
     * @param attributes entity attributes to update
     * @param replyToDestination destination for the management response (may be {@code null} if the caller does not expect a response)
     * @param session JMS session to use
     */
    private void updateEntityUsingAmqpManagement(final String name,
                                                 final String type,
                                                 final Map<String, Object> attributes,
                                                 final Destination replyToDestination,
                                                 final Session session)
            throws JMSException
    {
        try (final MessageProducer producer = session.createProducer(session.createQueue(MANAGEMENT_ADDRESS)))
        {
            final MapMessage createMessage = session.createMapMessage();
            createMessage.setStringProperty("type", type);
            createMessage.setStringProperty("operation", "UPDATE");
            createMessage.setStringProperty("index", "object-path");
            createMessage.setStringProperty("key", name);
            createMessage.setJMSReplyTo(replyToDestination);
            for (final Map.Entry<String, Object> entry : attributes.entrySet())
            {
                createMessage.setObject(entry.getKey(), entry.getValue());
            }
            producer.send(createMessage);
            commitIfTransacted(session);
        }
    }

    /**
     * Escapes queue names for use as management object-path keys.
     */
    private String getEscapedName(final Queue destination) throws JMSException
    {
        return destination.getQueueName().replaceAll("([/\\\\])", "\\\\$1");
    }

    /**
     * Converts query results of the form (attributeNames, results) into a list of maps.
     */
    private List<Map<String, Object>> getResultsAsMaps(final List<String> attributeNames,
                                                       final List<List<Object>> attributeValues)
    {
        final List<Map<String, Object>> results = new ArrayList<>();
        for (final List<Object> resultObject : attributeValues)
        {
            final Map<String, Object> result = new HashMap<>();
            for (int i = 0; i < attributeNames.size(); ++i)
            {
                result.put(attributeNames.get(i), resultObject.get(i));
            }
            results.add(result);
        }
        return results;
    }

    /**
     * Commits the session if it is transacted.
     */
    private static void commitIfTransacted(final Session session) throws JMSException
    {
        if (session.getTransacted())
        {
            session.commit();
        }
    }

    /**
     * Returns {@code true} if the value can be stored as a JMS property without conversion.
     */
    private static boolean isJmsPropertyValue(final Object value)
    {
        return value == null
                || value instanceof String
                || value instanceof Boolean
                || value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Float
                || value instanceof Double;
    }

    /**
     * Converts an arbitrary value to JSON for transport in JMS properties where only primitive types and strings are allowed.
     */
    private static String toJson(final Object value, final String argumentName)
    {
        try
        {
            return OBJECT_MAPPER.writeValueAsString(value);
        }
        catch (JacksonException e)
        {
            throw new IllegalArgumentException(String.format(
                    "Cannot convert the argument '%s' to JSON to meet JMS type restrictions",
                    argumentName), e);
        }
    }

    /**
     * Reads status code and throws a descriptive exception if the property is missing.
     */
    private static int getStatusCode(final Message message, final String context)
            throws JMSException
    {
        if (!message.propertyExists("statusCode"))
        {
            throw new JMSException("Management response is missing required property 'statusCode' (" + context + ")");
        }
        return message.getIntProperty("statusCode");
    }

    /**
     * Validates a status code is within the {@code 2xx} range.
     */
    private static void validateStatusCodeIs2xx(final Message message, final String context) throws JMSException
    {
        final int statusCode = getStatusCode(message, context);
        if (statusCode < 200 || statusCode > 299)
        {
            throw new OperationUnsuccessfulException(message.getStringProperty("statusDescription"), statusCode);
        }
    }

    /**
     * Validates a status code is within the {@code 2xx} range if the {@code statusCode} property is present.
     */
    private static void validateStatusCodeIs2xxIfPresent(final Message message, final String context) throws JMSException
    {
        if (message.propertyExists("statusCode"))
        {
            validateStatusCodeIs2xx(message, context);
        }
    }

    /**
     * Small {@link AutoCloseable} wrapper that deletes a temporary queue on close.
     */
    private record TemporaryQueueResource(TemporaryQueue _queue) implements AutoCloseable
    {
        static TemporaryQueueResource create(final Session session) throws JMSException
        {
            return new TemporaryQueueResource(session.createTemporaryQueue());
        }

        TemporaryQueue queue()
        {
            return _queue;
        }

        @Override
        public void close() throws JMSException
        {
            _queue.delete();
        }
    }

    /**
     * Runtime exception used to surface a non-success management status code in test code.
     */
    public static class OperationUnsuccessfulException extends RuntimeException
    {
        private final int _statusCode;

        private OperationUnsuccessfulException(final String message, final int statusCode)
        {
            super(message == null ? String.format("Unexpected status code %d", statusCode) : message);
            _statusCode = statusCode;
        }

        public int getStatusCode()
        {
            return _statusCode;
        }
    }
}
