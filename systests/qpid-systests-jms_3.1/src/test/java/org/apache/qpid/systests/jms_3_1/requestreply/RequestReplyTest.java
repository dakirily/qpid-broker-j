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

package org.apache.qpid.systests.jms_3_1.requestreply;

import static org.apache.qpid.systests.JmsAwait.jms;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import jakarta.jms.Message;
import jakarta.jms.QueueRequestor;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.TopicRequestor;

import jakarta.jms.TopicSession;
import org.apache.qpid.systests.Timeouts;
import org.apache.qpid.systests.support.AsyncSupport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on request/reply helper classes.
 * <br>
 * The tests in this class are aligned with:
 * - 2.16   "Request/reply"
 * - 4.1.7  "QueueRequestor"
 * - 4.2.10 "TopicRequestor"
 */
@JmsSystemTest
@Tag("queue")
@Tag("topic")
class RequestReplyTest
{
    /**
     * Verifies that QueueRequestor performs a request/reply round trip (4.1.7, 2.16).
     */
    @Test
    void queueRequestorRoundTrip(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var requestSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var responseSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final var requestConsumer = responseSession.createConsumer(queue);
            final var replyProducer = responseSession.createProducer(null);

            connection.start();

            final var responder = async.run("responder", () ->
            {
                final Message request = requestConsumer.receive(Timeouts.receiveMillis());
                if (request == null)
                {
                    throw new AssertionError("Request message not received");
                }
                final TextMessage reply = responseSession.createTextMessage("reply");
                replyProducer.send(request.getJMSReplyTo(), reply);
            });

            final QueueRequestor requestor = new QueueRequestor((QueueSession) requestSession, queue);
            try
            {
                final TextMessage reply = async
                        .call("requestor.request", Duration.ofMillis(Timeouts.receiveMillis()),
                                () -> (TextMessage) requestor.request(requestSession.createTextMessage("request")));
                assertNotNull(reply, "Reply should not be null");
                assertEquals("reply", reply.getText(), "Unexpected reply payload");
            }
            finally
            {
                requestor.close();
            }

            responder.await(Duration.ofMillis(Timeouts.receiveMillis()), "Responder didn't finish");
        }
    }

    /**
     * Verifies that TopicRequestor performs a request/reply round trip (4.2.10, 2.16).
     */
    @Test
    void topicRequestorRoundTrip(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final var topic = jms.builder().topic().create();
        try (final var connection = jms.builder().connection().create();
             final var requestSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var responseSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final int queueCount = jms.virtualhost().queueCount();
            final var requestConsumer = responseSession.createConsumer(topic);
            final var replyProducer = responseSession.createProducer(null);

            connection.start();

            // wait until the topic subscription is fully established on the broker.
            // without this the request can be published before the subscription is active and will be lost
            jms(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertTrue(jms.virtualhost().queueCount() > queueCount,
                            "Topic subscription not established yet (no subscription queue created)"));

            final var responder = async.run("responder", () ->
            {
                final Message request = requestConsumer.receive(Timeouts.receiveMillis());
                if (request == null)
                {
                    throw new AssertionError("Request message not received");
                }
                final TextMessage reply = responseSession.createTextMessage("reply");
                replyProducer.send(request.getJMSReplyTo(), reply);
            });

            final TopicRequestor requestor = new TopicRequestor((TopicSession) requestSession, topic);
            try
            {
                final TextMessage reply = async.call("requestor.request",
                        Duration.ofMillis(Timeouts.receiveMillis()),
                        () -> (TextMessage) requestor.request(requestSession.createTextMessage("request")));
                assertNotNull(reply, "Reply should not be null");
                assertEquals("reply", reply.getText(), "Unexpected reply payload");
            }
            finally
            {
                requestor.close();
            }

            responder.await(Duration.ofMillis(Timeouts.receiveMillis()), "Responder didn't finish");
        }
    }
}
