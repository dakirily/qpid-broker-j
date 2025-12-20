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

package org.apache.qpid.systests.builder;

import jakarta.jms.Queue;
import jakarta.jms.Session;
import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.server.queue.MessageGroupType;
import org.apache.qpid.systests.EntityTypes;
import org.apache.qpid.systests.support.JmsSupport;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class QueueBuilder
{
    private static final ObjectMapper OBJECT_MAPPER = ConfiguredObjectJacksonModule.newObjectMapper(false);

    private final Map<String, Map<String, List<String>>> _defaultFilters = new LinkedHashMap<>();

    private final JmsSupport _jmsSupport;

    private boolean _bind;

    private String _alternateBinding;
    private Boolean _durable;
    private Boolean _ensureNondestructiveConsumers;
    private Boolean _holdOnPublishEnabled;
    private String _lvqKey;
    private Integer _maximumDeliveryAttempts;
    private Integer _maximumLiveConsumers;
    private Long _maximumQueueDepthBytes;
    private Integer _maximumQueueDepthMessages;
    private String _messageGroupKeyOverride;
    private String _messageGroupType;
    private String _name;
    private String _nameSuffix;
    private String _overflowPolicy;
    private Integer _priorities;
    private Long _resumeCapacity;
    private String _sortKey;
    private String _virtualhost;

    public QueueBuilder(final JmsSupport jmsSupport)
    {
        _jmsSupport = jmsSupport;
    }

    public QueueBuilder(final JmsSupport jmsSupport, final String name)
    {
        _jmsSupport = jmsSupport;
        _name = name;
    }

    public QueueBuilder alternateBinding(String alternateBinding)
    {
        this._alternateBinding = OBJECT_MAPPER.writeValueAsString(Map.of("destination", alternateBinding));
        return this;
    }

    public QueueBuilder bind()
    {
        this._bind = true;
        return this;
    }

    public QueueBuilder jmsSelector(String selector)
    {
        selector = selector.replace("\\", "\\\\");
        selector = selector.replace("\"", "\\\"");
        this._defaultFilters.put("x-filter-jms-selector", Map.of("x-filter-jms-selector", List.of(selector)));
        return this;
    }

    public QueueBuilder durable(boolean durable)
    {
        this._durable = durable;
        return this;
    }

    public QueueBuilder ensureNondestructiveConsumers(boolean ensureNondestructiveConsumers)
    {
        this._ensureNondestructiveConsumers = ensureNondestructiveConsumers;
        return this;
    }

    public QueueBuilder holdOnPublishEnabled(boolean holdOnPublishEnabled)
    {
        this._holdOnPublishEnabled = holdOnPublishEnabled;
        return this;
    }

    public QueueBuilder lvqKey(String lvqKey)
    {
        this._lvqKey = lvqKey;
        return this;
    }

    public QueueBuilder maximumDeliveryAttempts(int maximumDeliveryAttempts)
    {
        this._maximumDeliveryAttempts = maximumDeliveryAttempts;
        return this;
    }

    public QueueBuilder maximumLiveConsumers(int maximumLiveConsumers)
    {
        this._maximumLiveConsumers = maximumLiveConsumers;
        return this;
    }

    public QueueBuilder maximumQueueDepthBytes(long maximumQueueDepthBytes)
    {
        this._maximumQueueDepthBytes = maximumQueueDepthBytes;
        return this;
    }

    public QueueBuilder maximumQueueDepthMessages(int maximumQueueDepthMessages)
    {
        this._maximumQueueDepthMessages = maximumQueueDepthMessages;
        return this;
    }

    public QueueBuilder messageGroupKeyOverride(String messageGroupKeyOverride)
    {
        this._messageGroupKeyOverride = messageGroupKeyOverride;
        return this;
    }

    public QueueBuilder messageGroupType(MessageGroupType messageGroupType)
    {
        this._messageGroupType = messageGroupType.toString();
        return this;
    }

    public QueueBuilder overflowPolicy(OverflowPolicy overflowPolicy)
    {
        this._overflowPolicy = overflowPolicy.toString();
        return this;
    }

    public QueueBuilder name(String name)
    {
        this._name = name;
        return this;
    }

    public QueueBuilder nameSuffix(String nameSuffix)
    {
        this._nameSuffix = nameSuffix;
        return this;
    }

    public QueueBuilder priorities(int priorities)
    {
        this._priorities = priorities;
        return this;
    }

    public QueueBuilder replayPeriod(long replayPeriod)
    {
        this._defaultFilters.put("x-qpid-replay-period", Map.of("x-qpid-replay-period", List.of(Long.toString(replayPeriod))));
        return this;
    }

    public QueueBuilder resumeCapacity(long resumeCapacity)
    {
        this._resumeCapacity = resumeCapacity;
        return this;
    }

    public QueueBuilder sortKey(String sortKey)
    {
        this._sortKey = sortKey;
        return this;
    }

    public QueueBuilder virtualhost(String virtualhost)
    {
        this._virtualhost = virtualhost;
        return this;
    }

    public Queue get() throws Exception
    {
        String queueName = _name == null ? _jmsSupport.testMethodName() : _name;
        if (_nameSuffix != null)
        {
            queueName += _nameSuffix;
        }
        try (final var connection = _jmsSupport.builder().connection().create();
        final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            return session.createQueue(queueName);
        }
    }

    public Queue create() throws Exception
    {
        String queueName = _name == null ? _jmsSupport.testMethodName() : _name;
        if (_nameSuffix != null)
        {
            queueName += _nameSuffix;
        }
        String queueType = EntityTypes.QUEUE;
        final String virtualHostName = _virtualhost == null ? _jmsSupport.virtualHostName() : _virtualhost;
        final Map<String, Object> attributes = new HashMap<>();

        if (_alternateBinding != null)
        {
            attributes.put("alternateBinding", _alternateBinding);
        }

        if (_durable != null)
        {
            attributes.put("durable", _durable);
        }

        if (_ensureNondestructiveConsumers != null)
        {
            attributes.put("ensureNondestructiveConsumers", _ensureNondestructiveConsumers);
        }

        if (_holdOnPublishEnabled != null)
        {
            attributes.put("holdOnPublishEnabled", _holdOnPublishEnabled);
        }

        if (_lvqKey != null)
        {
            attributes.put("lvqKey", _lvqKey);
            queueType = "org.apache.qpid.LastValueQueue";
        }

        if (_maximumDeliveryAttempts != null)
        {
            attributes.put("maximumDeliveryAttempts", _maximumDeliveryAttempts);
        }

        if (_maximumLiveConsumers != null)
        {
            attributes.put("maximumLiveConsumers", _maximumLiveConsumers);
        }

        if (_maximumQueueDepthBytes != null)
        {
            attributes.put("maximumQueueDepthBytes", _maximumQueueDepthBytes);
        }

        if (_maximumQueueDepthMessages != null)
        {
            attributes.put("maximumQueueDepthMessages", _maximumQueueDepthMessages);
        }

        if (_messageGroupKeyOverride != null)
        {
            attributes.put("messageGroupKeyOverride", _messageGroupKeyOverride);
        }

        if (_messageGroupType != null)
        {
            attributes.put("messageGroupType", _messageGroupType);
        }

        if (_overflowPolicy != null)
        {
            attributes.put("overflowPolicy", _overflowPolicy);
        }

        if (_priorities != null)
        {
            attributes.put("priorities", _priorities);
            queueType = "org.apache.qpid.PriorityQueue";
        }

        if (_resumeCapacity != null && _maximumQueueDepthBytes != null && _resumeCapacity > 0 && _maximumQueueDepthBytes > 0)
        {
            final double ratio = (double) _resumeCapacity / _maximumQueueDepthBytes;
            final String flowResumeLimit = String.format(Locale.ROOT, "%.2f",ratio * 100.0);
            attributes.put("context", "{\"queue.queueFlowResumeLimit\": %s}".formatted(flowResumeLimit));
        }

        if (_sortKey != null)
        {
            attributes.put("sortKey", _sortKey);
            queueType = "org.apache.qpid.SortedQueue";
        }

        if (!_defaultFilters.isEmpty())
        {
            attributes.put("defaultFilters", OBJECT_MAPPER.writeValueAsString(_defaultFilters));
        }

        attributes.put("name", queueName);

        try (final var connection = _jmsSupport.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            _jmsSupport.amqpManagementFacade()
                    .createEntityAndAssertResponse(queueName, queueType, attributes, session);
        }

        try (final var connection = _jmsSupport.builder().connection().virtualHost(virtualHostName).create())
        {
            connection.start();
            try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
            {
                final var queue = session.createQueue(queueName);

                if (_bind)
                {
                    final Map<String, Object> arguments = new HashMap<>();
                    arguments.put("destination", queue.getQueueName());
                    arguments.put("bindingKey", queue.getQueueName());
                    _jmsSupport.management().perform("amq.direct", "bind", "org.apache.qpid.Exchange", arguments);
                }

                return queue;
            }
        }
    }
}
