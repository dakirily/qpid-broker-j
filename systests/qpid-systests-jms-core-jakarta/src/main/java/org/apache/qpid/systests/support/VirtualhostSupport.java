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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class VirtualhostSupport
{
    private final JmsSupport _jmsSupport;
    private final String _name;

    public VirtualhostSupport(final JmsSupport jmsSupport, final String name)
    {
        _jmsSupport = jmsSupport;
        _name = name;
    }

    public Map<String, Object> statistics(final String... statisticsName) throws Exception
    {
        final Map<String, Object> arguments = Collections.singletonMap("statistics", Arrays.asList(statisticsName));
        Object statistics = _jmsSupport.management().perform(_name, "getStatistics", "org.apache.qpid.VirtualHost", arguments);

        assertNotNull(statistics, "Statistics is null");
        assertInstanceOf(Map.class, statistics, "Statistics is not map");

        return (Map<String, Object>) statistics;
    }

    public int totalDepthOfQueuesMessages() throws Exception
    {
        final Map<String, Object> statisticsMap = statistics("totalDepthOfQueuesMessages");
        return ((Number) statisticsMap.get("totalDepthOfQueuesMessages")).intValue();
    }

    public int queueCount() throws Exception
    {
        final Map<String, Object> statisticsMap = statistics("queueCount");
        return ((Number) statisticsMap.get("queueCount")).intValue();
    }

    public int exchangeCount() throws Exception
    {
        final Map<String, Object> statisticsMap = statistics("exchangeCount");
        return ((Number) statisticsMap.get("exchangeCount")).intValue();
    }
}
