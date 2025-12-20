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

package org.apache.qpid.systests.jms_3_1.topic;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on subscription name character and length requirements.
 *
 * The tests in this class are aligned with:
 * - 4.2.4 "Subscription name characters and length"
 */
@JmsSystemTest
@Tag("topic")
class SubscriptionNameValidationTest
{
    private static final int REQUIRED_MAX_LENGTH = 128;

    /**
     * Verifies that durable subscription names accept the required characters
     * and support a length of at least 128 characters (4.2.4).
     */
    @Test
    void durableSubscriptionAllowsRequiredCharactersAndLength(final JmsSupport jms) throws Exception
    {
        final Topic topic = jms.builder().topic().create();
        final String baseName = "sub_A-1.b_" + jms.testMethodName();
        final String longName = nameWithLength(baseName, REQUIRED_MAX_LENGTH);

        try (final var connection = jms.builder().connection().clientId(jms.testMethodName()).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            try (final TopicSubscriber subscriber = session.createDurableSubscriber(topic, baseName))
            {
                assertNotNull(subscriber, "Durable subscriber should be created with required characters");
            }
            session.unsubscribe(baseName);

            try (final TopicSubscriber subscriber = session.createDurableSubscriber(topic, longName))
            {
                assertNotNull(subscriber, "Durable subscriber should be created with length 128");
            }
            session.unsubscribe(longName);
        }
    }

    /**
     * Verifies that shared (non-durable) subscription names accept the required
     * characters and support a length of at least 128 characters (4.2.4).
     */
    @Test
    void sharedSubscriptionAllowsRequiredCharactersAndLength(final JmsSupport jms) throws Exception
    {
        final Topic topic = jms.builder().topic().create();
        final String baseName = "shared_A-1.b_" + jms.testMethodName();
        final String longName = nameWithLength(baseName, REQUIRED_MAX_LENGTH);

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            try (final var consumer = session.createSharedConsumer(topic, baseName))
            {
                assertNotNull(consumer, "Shared consumer should be created with required characters");
            }

            try (final var consumer = session.createSharedConsumer(topic, longName))
            {
                assertNotNull(consumer, "Shared consumer should be created with length 128");
            }
        }
    }

    private String nameWithLength(final String base, final int length)
    {
        if (base.length() >= length)
        {
            return base.substring(0, length);
        }
        return base + "a".repeat(length - base.length());
    }
}
