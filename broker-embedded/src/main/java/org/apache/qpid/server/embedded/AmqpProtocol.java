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

import org.apache.qpid.server.model.Protocol;

/**
 * AMQP protocol versions that can be enabled on an embedded broker port.
 *
 * <p>AMQP 1.0 support is included by this module. Selecting an AMQP 0-x version requires the corresponding
 * Broker-J protocol plug-in on the test runtime class path.</p>
 */
public enum AmqpProtocol
{
    /** AMQP 0-8. */
    AMQP_0_8(Protocol.AMQP_0_8),
    /** AMQP 0-9. */
    AMQP_0_9(Protocol.AMQP_0_9),
    /** AMQP 0-9-1. */
    AMQP_0_9_1(Protocol.AMQP_0_9_1),
    /** AMQP 0-10. */
    AMQP_0_10(Protocol.AMQP_0_10),
    /** AMQP 1.0. */
    AMQP_1_0(Protocol.AMQP_1_0);

    private final Protocol _protocol;

    AmqpProtocol(final Protocol protocol)
    {
        _protocol = protocol;
    }

    Protocol getProtocol()
    {
        return _protocol;
    }
}
