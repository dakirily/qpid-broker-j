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

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

import java.time.Duration;

import org.awaitility.core.ConditionFactory;

public class JmsAwait
{
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(50);

    private JmsAwait()
    {

    }

    public static ConditionFactory jms()
    {
        return await().pollInSameThread()
                .pollDelay(Duration.ZERO)
                .pollInterval(DEFAULT_POLL_INTERVAL);
    }

    public static ConditionFactory jms(final Duration atMost)
    {
        return jms().atMost(atMost);
    }

    public static ConditionFactory jmsGiven()
    {
        return given().pollInSameThread()
                .pollDelay(Duration.ZERO)
                .pollInterval(DEFAULT_POLL_INTERVAL)
                .await();
    }

    public static void pause(final Duration delay)
    {
        await().pollDelay(delay)
                .atMost(delay.plusSeconds(1))
                .until(() -> true);
    }
}