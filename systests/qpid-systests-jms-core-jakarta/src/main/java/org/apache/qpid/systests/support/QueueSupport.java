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

package org.apache.qpid.systests.support;

import jakarta.jms.Session;
import org.apache.qpid.systests.EntityTypes;
import org.apache.qpid.systests.Timeouts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class QueueSupport
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueSupport.class);

    private final JmsSupport _jmsSupport;
    private final String _name;

    public QueueSupport(final JmsSupport jmsSupport, final String name)
    {
        _jmsSupport = jmsSupport;
        _name = name;
    }

    public Map<String, Object> statistics() throws Exception
    {
        final String queueName = _name == null ? _jmsSupport.testMethodName() : _name;
        final Map<String, Object> arguments = Map.of("statistics", List.of("queueDepthMessages", "queueDepthBytes"));
        final Object statistics = _jmsSupport
                .management().perform(queueName, "getStatistics", EntityTypes.QUEUE, arguments);
        assertNotNull(statistics, "Statistics is null");
        assertInstanceOf(Map.class, statistics, "Statistics is not map");
        return (Map<String, Object>) statistics;
    }

    public long depthBytes() throws Exception
    {
        final Map<String, Object> statisticsMap = statistics();
        assertInstanceOf(Number.class, statisticsMap.get("queueDepthBytes"), "queueDepthBytes is not present");
        return ((Number) statisticsMap.get("queueDepthBytes")).longValue();
    }

    public int depthMessages() throws Exception
    {
        final Map<String, Object> statisticsMap = statistics();
        assertInstanceOf(Number.class, statisticsMap.get("queueDepthMessages"), "queueDepthMessages is not present");
        return ((Number) statisticsMap.get("queueDepthMessages")).intValue();
    }

    public void drain() throws Exception
    {
        try (final var connection = _jmsSupport.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            connection.start();
            final var queue = session.createQueue(_name);
            try (final var consumer = session.createConsumer(queue))
            {
                int cnt = 0;
                while (consumer.receive(Timeouts.noMessagesMillis()) != null)
                {
                    cnt ++;
                }
                LOGGER.info("{} messages deleted from queue '{}'", cnt, queue.getQueueName());
            }
        }
    }
}
