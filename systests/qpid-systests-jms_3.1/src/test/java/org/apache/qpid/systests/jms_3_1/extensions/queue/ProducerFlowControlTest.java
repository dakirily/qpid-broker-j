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

package org.apache.qpid.systests.jms_3_1.extensions.queue;

import static org.apache.qpid.systests.EntityTypes.QUEUE;
import static org.apache.qpid.systests.JmsAwait.jms;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.qpid.systests.support.AsyncSupport;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.opentest4j.AssertionFailedError;

import org.apache.qpid.server.model.OverflowPolicy;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("policy")
@Tag("queue")
@Tag("timing")
class ProducerFlowControlTest extends OverflowPolicyTestBase
{
    @Test
    void capacityExceededCausesBlock(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final String queueName = jms.testMethodName();
        final long messageSize = evaluateMessageSize(jms);
        final long capacity = messageSize * 3 + messageSize / 2;
        final long resumeCapacity = messageSize * 2;
        final var queue = jms.builder().queue()
            .maximumQueueDepthBytes(capacity)
            .maximumQueueDepthMessages(-1)
            .overflowPolicy(OverflowPolicy.PRODUCER_FLOW_CONTROL)
            .resumeCapacity(resumeCapacity)
            .create();

        try (final var producerConnection = jms.builder().connection().syncPublish(true).create();
             final var producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = producerSession.createProducer(queue))
        {
            // try to send 5 messages (should block after 4)
            final var messageSender = sendMessagesAsync(async, producer, producerSession, 5);

            awaitQueueFlowStopped(Duration.ofMillis(5000), jms, queueName, true, "Flow is not stopped");
            assertEquals(4, messageSender.sentCount(),
                    "Incorrect number of message sent before blocking");

            try (final var consumerConnection = jms.builder().connection().create();
                 final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 final var consumer = consumerSession.createConsumer(queue))
            {
                consumerConnection.start();

                final var message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Message is not received");

                assertQueueFlowStoppedMaintained(Duration.ofMillis(1000), jms, queueName);

                assertEquals(4, messageSender.sentCount(),
                        "Message incorrectly sent after one message received");

                final var message2 = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message2, "Message is not received");
                messageSender.awaitCompleted(Duration.ofMillis(1000));
                assertEquals(5, messageSender.sentCount(),
                        "Message not sent after two messages received");
            }
        }
    }

    @Test
    void flowControlOnCapacityResumeEqual(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final String queueName = jms.testMethodName();
        final long messageSize = evaluateMessageSize(jms);
        final long capacity = messageSize * 3 + messageSize / 2;
        final var queue = jms.builder().queue()
                .maximumQueueDepthBytes(capacity)
                .maximumQueueDepthMessages(-1)
                .overflowPolicy(OverflowPolicy.PRODUCER_FLOW_CONTROL)
                .resumeCapacity(capacity)
                .create();

        try (final var producerConnection = jms.builder().connection().syncPublish(true).create();
             final var producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = producerSession.createProducer(queue))
        {
            // try to send 5 messages (should block after 4)
            final var messageSender = sendMessagesAsync(async, producer, producerSession, 5);

            awaitQueueFlowStopped(Duration.ofMillis(5000), jms, queueName, true, "Flow is not stopped");

            assertEquals(4, messageSender.sentCount(),
                    "Incorrect number of message sent before blocking");

            try (final var consumerConnection = jms.builder().connection().create();
                 final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 final var consumer = consumerSession.createConsumer(queue))
            {
                consumerConnection.start();

                final var message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Message is not received");

                messageSender.awaitCompleted(Duration.ofMillis(1000));

                assertEquals(5, messageSender.sentCount(),
                        "Message incorrectly sent after one message received");
            }
        }
    }

    @Test
    void flowControlAttributeModificationViaManagement(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final String queueName = jms.testMethodName();
        final long messageSize = evaluateMessageSize(jms);
        final var queue = jms.builder().queue()
                .maximumQueueDepthBytes(0)
                .maximumQueueDepthMessages(-1)
                .overflowPolicy(OverflowPolicy.PRODUCER_FLOW_CONTROL)
                .resumeCapacity(0)
                .create();

        // set new values that will cause flow control to be active, and the queue to become overfull after 1 message is sent
        setFlowLimits(jms, queueName, messageSize / 2, messageSize / 2);
        assertFalse(isFlowStopped(jms, queueName), "Queue should not be overfull");

        try (final var producerConnection = jms.builder().connection().syncPublish(true).create();
             final var producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = producerSession.createProducer(queue))
        {
            // try to send 2 messages (should block after 1)
            final var sender = sendMessagesAsync(async, producer, producerSession, 2);

            awaitQueueFlowStopped(Duration.ofMillis(2000), jms, queueName, true,"Flow is not stopped");

            assertEquals(1, sender.sentCount(),
                    "Incorrect number of message sent before blocking");

            final long queueDepthBytes = jms.queue(queueName).depthBytes();
            //raise the attribute values, causing the queue to become underfull and allow the second message to be sent.
            setFlowLimits(jms, queueName, queueDepthBytes * 2 + queueDepthBytes / 2, queueDepthBytes + queueDepthBytes / 2);

            awaitQueueFlowStopped(Duration.ofMillis(2000), jms, queueName, false, "Flow is stopped");
            awaitSecondMessage(Duration.ofMillis(2000), jms, queueName);
            assertFalse(isFlowStopped(jms, queueName), "Queue should not be overfull");

            // try to send another message to block flow
            final var sender2 = sendMessagesAsync(async, producer, producerSession, 1);

            awaitQueueFlowStopped(Duration.ofMillis(2000), jms, queueName, true, "Flow is stopped");
            assertEquals(1, sender2.sentCount(),
                    "Incorrect number of message sent before blocking");
            assertTrue(isFlowStopped(jms, queueName), "Queue should be overfull");

            try (final var consumerConnection = jms.builder().connection().create();
                 final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 final var consumer = consumerSession.createConsumer(queue))
            {
                consumerConnection.start();

                var message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Message is not received");

                message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Second message is not received");

                awaitQueueFlowStopped(Duration.ofMillis(2000), jms, queueName, false,"Flow is stopped");

                assertNotNull(consumer.receive(Timeouts.receiveMillis()), "Should have received second message");
            }
        }
    }

    @Test
    void producerFlowControlIsTriggeredOnEnqueue(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final String queueName = jms.testMethodName();
        final var queue = jms.builder().queue()
                .maximumQueueDepthBytes(1)
                .maximumQueueDepthMessages(-1)
                .overflowPolicy(OverflowPolicy.PRODUCER_FLOW_CONTROL)
                .resumeCapacity(0)
                .create();

        try (final var producerConnection = jms.builder().connection().syncPublish(true).create();
             final var producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = producerSession.createProducer(queue))
        {
            producer.send(nextMessage(0, producerSession));

            // try to send 2 messages (should block after 1)
            sendMessagesAsync(async, producer, producerSession, 2);

            awaitQueueFlowStopped(Duration.ofMillis(2000), jms, queueName, true, "Flow is not stopped");

            try (final var consumerConnection = jms.builder().connection().create();
                 final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 final var consumer = consumerSession.createConsumer(queue))
            {
                consumerConnection.start();

                var message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Message is not received");

                message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Second message is not received");
            }
        }
    }

    @Test
    void queueDeleteWithBlockedFlow(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final String queueName = jms.testMethodName();
        final long messageSize = evaluateMessageSize(jms);
        final long capacity = messageSize * 3 + messageSize / 2;
        final long resumeCapacity = messageSize * 2;
        final var queue = jms.builder().queue()
                .maximumQueueDepthBytes(capacity)
                .maximumQueueDepthMessages(-1)
                .overflowPolicy(OverflowPolicy.PRODUCER_FLOW_CONTROL)
                .resumeCapacity(resumeCapacity)
                .create();
        try (final var producerConnection = jms.builder().connection().syncPublish(true).create();
             final var producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = producerSession.createProducer(queue))
        {
            // try to send 5 messages (should block after 4)
            final var sender = sendMessagesAsync(async, producer, producerSession, 5);

            awaitQueueFlowStopped(Duration.ofMillis(5000), jms, queueName, true, "Flow is not stopped");

            assertEquals(4, sender.sentCount(), "Incorrect number of message sent before blocking");

            jms.management().delete(queueName, QUEUE);

            jms.builder().queue(queueName).create();

            assertEquals(0, jms.queue(queueName).depthMessages(), "Unexpected queue depth");
        }
    }

    @Test
    void enforceFlowControlOnNewConnection(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final var testQueue = jms.builder().queue()
                .maximumQueueDepthMessages(1)
                .overflowPolicy(OverflowPolicy.PRODUCER_FLOW_CONTROL)
                .create();
        try (final var producerConnection1 = jms.builder().connection().syncPublish(true).create())
        {
            jms.messages().send(producerConnection1, testQueue, 2);
            awaitQueueFlowStopped(Duration.ofMillis(5000), jms, testQueue.getQueueName(), true,  "Flow is not stopped");
        }

        try (final var producerConnection2 = jms.builder().connection().syncPublish(true).create();
             final var producerSession = producerConnection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = producerSession.createProducer(testQueue))
        {
            final var messageSender = sendMessagesAsync(async, producer, producerSession, 1);
            awaitQueueFlowStopped(Duration.ofMillis(5000), jms, testQueue.getQueueName(), true, "Flow is not stopped");
            assertEquals(0, messageSender.sentCount(),
                    "Incorrect number of message sent before blocking");

            try (final var consumerConnection = jms.builder().connection().create();
                 final var consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                 final var consumer = consumerSession.createConsumer(testQueue))
            {
                consumerConnection.start();

                final var message = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message, "Message is not received");

                final var message2 = consumer.receive(Timeouts.receiveMillis());
                assertNotNull(message2, "Message is not received");
                messageSender.awaitCompleted(Duration.ofMillis(1000));
            }
        }
    }

    private void setFlowLimits(final JmsSupport jms, final String queueName, final long blockValue, final long resumeValue) throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.Queue.MAXIMUM_QUEUE_DEPTH_BYTES, blockValue);
        attributes.put(org.apache.qpid.server.model.Queue.OVERFLOW_POLICY, OverflowPolicy.PRODUCER_FLOW_CONTROL.name());
        final String resumeLimit = getFlowResumeLimit(blockValue, resumeValue);
        final String context = "{\"%s\": %s}".formatted(org.apache.qpid.server.model.Queue.QUEUE_FLOW_RESUME_LIMIT, resumeLimit);
        attributes.put(org.apache.qpid.server.model.Queue.CONTEXT, context);
        jms.management().update(queueName, QUEUE, attributes);
    }

    private boolean isFlowStopped(final JmsSupport jms, final String queueName) throws Exception
    {
        final Map<String, Object> attributes = jms.management().read(queueName, QUEUE, false);
        return Boolean.TRUE.equals(attributes.get("queueFlowStopped"));
    }

    private SendTask sendMessagesAsync(final AsyncSupport async,
                                       final MessageProducer producer,
                                       final Session producerSession,
                                       final int numMessages)
    {
        final var sent = new AtomicInteger();
        final var task = async.run("send %s messages".formatted(numMessages), () ->
        {
            for (int msg = 0; msg < numMessages; msg ++)
            {
                producer.send(nextMessage(msg, producerSession));
                sent.incrementAndGet();
                producerSession.createTemporaryQueue().delete();
            }
        });
        return new SendTask(sent, task);
    }

    private void awaitSecondMessage(final Duration timeout,
                                    final JmsSupport jms,
                                    final String queueName)
    {
        jms(timeout).pollInterval(Duration.ofMillis(50)).untilAsserted(() ->
        {
            final var actualValue = jms.queue(queueName).depthMessages();
            assertEquals(2, actualValue, () ->
                    "%s: expected = '%s', actual = '%s'".formatted("Second message was not sent after changing limits", 2, actualValue));
        });
    }

    private void assertQueueFlowStoppedMaintained(final Duration timeout,
                                                  final JmsSupport jms,
                                                  final String queueName)
    {
        final long deadline = System.nanoTime() + timeout.toNanos();
        jms(timeout.plusMillis(250))
                .pollInterval(Duration.ofMillis(50))
                .await("Flow should remain stopped after one message received")
                .until(() ->
                {
                    final Map<String, Object> attributes =
                            jms.management().read(queueName, QUEUE, false);
                    final var actualValue = attributes.get("queueFlowStopped");

                    if (!equalsLoosely(true, actualValue))
                    {
                        throw new AssertionFailedError("Attribute value changed, expected = '%s', actual = '%s'"
                                .formatted(true, actualValue));
                    }

                    return System.nanoTime() >= deadline;
                });
    }

    private void awaitQueueFlowStopped(final Duration timeout,
                                       final JmsSupport jms,
                                       final String queueName,
                                       final Object expectedValue,
                                       final String message)
    {
        jms(timeout)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() ->
                {
                    final Map<String, Object> attributes = jms.management().read(queueName, QUEUE, false);
                    final var actualValue = attributes.get("queueFlowStopped");
                    assertTrue(equalsLoosely(expectedValue, actualValue), () ->
                            "%s: expected = '%s', actual = '%s'".formatted(message, expectedValue, actualValue));
                });
    }

    private static Boolean equalsLoosely(final Object expected, final Object actual)
    {
        if (expected == null)
        {
            return actual == null;
        }
        if (actual == null)
        {
            return false;
        }
        if (actual.getClass() == expected.getClass())
        {
            return expected.equals(actual);
        }
        return String.valueOf(expected).equals(String.valueOf(actual));
    }

    private record SendTask(AtomicInteger _sent, AsyncSupport.Task<Void> _task)
    {
        int sentCount()
        {
            return _sent.get();
        }

        void awaitCompleted(final Duration timeout)
        {
            _task.await(timeout, "Message sending is not finished");
        }
    }
}
