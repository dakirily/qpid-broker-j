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

package org.apache.qpid.systests;

public final class EntityTypes
{
    private EntityTypes()
    {
        // utility class, constructor is private
    }

    public static final String AMQP_PORT = "org.apache.qpid.AmqpPort";
    public static final String BROKER = "org.apache.qpid.Broker";
    public static final String CONNECTION = "org.apache.qpid.Connection";
    public static final String DIRECT_EXCHANGE = "org.apache.qpid.DirectExchange";
    public static final String EXCHANGE = "org.apache.qpid.Exchange";
    public static final String FANOUT_EXCHANGE = "org.apache.qpid.FanoutExchange";
    public static final String JSON_VIRTUALHOST_NODE = "org.apache.qpid.JsonVirtualHostNode";
    public static final String LAST_VALUE_QUEUE = "org.apache.qpid.LastValueQueue";
    public static final String PORT = "org.apache.qpid.Port";
    public static final String PRIORITY_QUEUE = "org.apache.qpid.PriorityQueue";
    public static final String QUEUE = "org.apache.qpid.Queue";
    public static final String RULE_BASED_VIRTUAL_HOST_ACCESS_CONTROL_PROVIDER =
            "org.apache.qpid.RuleBaseVirtualHostAccessControlProvider";
    public static final String RULE_BASED_VIRTUAL_HOST_USER_CONNECTION_LIMIT_PROVIDER =
            "org.apache.qpid.RuleBasedVirtualHostConnectionLimitProvider";
    public static final String SORTED_QUEUE = "org.apache.qpid.SortedQueue";
    public static final String STANDARD_QUEUE = "org.apache.qpid.StandardQueue";
    public static final String TOPIC_EXCHANGE = "org.apache.qpid.TopicExchange";
    public static final String VIRTUALHOST = "org.apache.qpid.VirtualHost";
}
