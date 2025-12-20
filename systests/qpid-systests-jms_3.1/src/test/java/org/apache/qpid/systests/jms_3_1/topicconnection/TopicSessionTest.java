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

package org.apache.qpid.systests.jms_3_1.topicconnection;

import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.Session;

import jakarta.jms.TopicConnection;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("legacy")
@Tag("topic")
class TopicSessionTest
{
    @Test
    void topicSessionCannotCreateCreateBrowser(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var topicConnection = jms.builder().connection().create(TopicConnection.class);
             final var topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            assertThrows(jakarta.jms.IllegalStateException.class,
                    () -> topicSession.createBrowser(queue),
                    "Expected exception was not thrown");
        }
    }

    @Test
    void topicSessionCannotCreateQueues(final JmsSupport jms) throws Exception
    {
        try (final var topicConnection = jms.builder().connection().create(TopicConnection.class);
             final var topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            assertThrows(jakarta.jms.IllegalStateException.class,
                    () -> topicSession.createQueue("abc"),
                    "Expected exception was not thrown");
        }
    }

    @Test
    void topicSessionCannotCreateTemporaryQueues(final JmsSupport jms) throws Exception
    {
        try (final var topicConnection = jms.builder().connection().create(TopicConnection.class);
             final var topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            assertThrows(jakarta.jms.IllegalStateException.class,
                    topicSession::createTemporaryQueue,
                    "Expected exception was not thrown");
        }
    }

    @Test
    void publisherGetDeliveryModeAfterConnectionClose(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();
        try (final var connection = jms.builder().connection().create(TopicConnection.class);
             final var session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
             final var publisher = session.createPublisher(topic))
        {
            connection.close();

            assertThrows(jakarta.jms.IllegalStateException.class,
                    publisher::getDeliveryMode,
                    "Expected exception was not thrown");
        }
    }
}
