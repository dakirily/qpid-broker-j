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
package org.apache.qpid.server.embedded;

import org.apache.qpid.server.exchange.ExchangeDefaults;

/**
 * Exchange types supported by the embedded broker topology builder.
 */
public enum ExchangeType
{
    /** Direct exchange. */
    DIRECT(ExchangeDefaults.DIRECT_EXCHANGE_CLASS),
    /** Topic exchange. */
    TOPIC(ExchangeDefaults.TOPIC_EXCHANGE_CLASS),
    /** Fanout exchange. */
    FANOUT(ExchangeDefaults.FANOUT_EXCHANGE_CLASS),
    /** Headers exchange. */
    HEADERS(ExchangeDefaults.HEADERS_EXCHANGE_CLASS);

    private final String _type;

    ExchangeType(final String type)
    {
        _type = type;
    }

    /**
     * Returns the Broker-J configured-object type name.
     *
     * @return configured-object type name
     */
    public String getType()
    {
        return _type;
    }
}
