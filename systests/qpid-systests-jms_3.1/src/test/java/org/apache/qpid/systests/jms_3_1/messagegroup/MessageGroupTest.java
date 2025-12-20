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

package org.apache.qpid.systests.jms_3_1.messagegroup;

import static jakarta.jms.Session.AUTO_ACKNOWLEDGE;
import static jakarta.jms.Session.CLIENT_ACKNOWLEDGE;
import static jakarta.jms.Session.SESSION_TRANSACTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.queue.MessageGroupType;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("queue")
class MessageGroupTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageGroupTest.class);

    @Test
    void simpleGroupAssignment(final JmsSupport jms) throws Exception
    {
        simpleGroupAssignment(jms, false, false);
    }

    @Test
    void sharedGroupSimpleGroupAssignment(final JmsSupport jms) throws Exception
    {
        simpleGroupAssignment(jms, true, false);
    }

    @Test
    void simpleGroupAssignmentWithJMSXGroupID(final JmsSupport jms) throws Exception
    {
        simpleGroupAssignment(jms, false, true);
    }

    @Test
    void sharedGroupSimpleGroupAssignmentWithJMSXGroupID(final JmsSupport jms) throws Exception
    {
        simpleGroupAssignment(jms, true, true);
    }

    /**
     * Pre-populate the queue with messages with groups as follows
     * <br>
     *  ONE
     *  TWO
     *  ONE
     *  TWO
     * <br>
     *  Create two consumers with prefetch of 1, the first consumer should then be assigned group ONE, the second
     *  consumer assigned group TWO if they are started in sequence.
     * <br>
     *  Thus doing
     * <br>
     *  c1 <--- (ONE)
     *  c2 <--- (TWO)
     *  c2 ack --->
     * <br>
     *  c2 should now be able to receive a second message from group TWO (skipping over the message from group ONE)
     * <br>
     *  i.e.
     * <br>
     *  c2 <--- (TWO)
     *  c2 ack --->
     *  c1 <--- (ONE)
     *  c1 ack --->
     */
    private void simpleGroupAssignment(final JmsSupport jms, boolean sharedGroups, final boolean useDefaultGroup) throws Exception
    {
        final var groupKey = getGroupKey(useDefaultGroup);
        final var queue = jms.builder().queue()
                .durable(false)
                .messageGroupKeyOverride(useDefaultGroup ? null : "group")
                .messageGroupType(sharedGroups ? MessageGroupType.SHARED_GROUPS : MessageGroupType.STANDARD)
                .create();

        try (final var producerConnection = jms.builder().connection().create())
        {
            producerConnection.start();
            final var producerSession = producerConnection.createSession(true, SESSION_TRANSACTED);
            final var producer = producerSession.createProducer(queue);

            String[] groups = { "ONE", "TWO"};

            for (int msg = 0; msg < 4; msg++)
            {
                producer.send(createMessage(producerSession, msg, groups[msg % groups.length], useDefaultGroup));
            }
            producerSession.commit();
        }

        try (final var consumerConnection = jms.builder().connection().prefetch(0).create();
             final var cs1 = consumerConnection.createSession(false, CLIENT_ACKNOWLEDGE);
             final var cs2 = consumerConnection.createSession(false, CLIENT_ACKNOWLEDGE))
        {
            final var consumer1 = cs1.createConsumer(queue);
            final var consumer2 = cs2.createConsumer(queue);

            consumerConnection.start();
            final var cs1Received = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(cs1Received, "Consumer 1 should have received first message");

            final var cs2Received = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(cs2Received, "Consumer 2 should have received first message");

            cs2Received.acknowledge();

            final var cs2Received2 = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(cs2Received2, "Consumer 2 should have received second message");
            assertEquals(cs2Received2.getStringProperty(groupKey), cs2Received.getStringProperty(groupKey),
                    "Differing groups");

            cs1Received.acknowledge();
            final var cs1Received2 = consumer1.receive(Timeouts.receiveMillis());

            assertNotNull(cs1Received2, "Consumer 1 should have received second message");
            assertEquals(cs1Received2.getStringProperty(groupKey), cs1Received.getStringProperty(groupKey),
                    "Differing groups");

            cs1Received2.acknowledge();
            cs2Received2.acknowledge();

            assertNull(consumer1.receive(Timeouts.noMessagesMillis()));
            assertNull(consumer2.receive(Timeouts.noMessagesMillis()));
        }
    }

    @Test
    void consumerCloseGroupAssignment(final JmsSupport jms) throws Exception
    {
        consumerCloseGroupAssignment(jms, false);
    }

    @Test
    void sharedGroupConsumerCloseGroupAssignment(final JmsSupport jms) throws Exception
    {
        consumerCloseGroupAssignment(jms, true);
    }

    /**
     * Tests that upon closing a consumer, groups previously assigned to that consumer are reassigned to a different
     * consumer.
     * <br>
     * Pre-populate the queue as ONE, ONE, TWO, ONE
     * <br>
     * create in sequence two consumers
     * <br>
     * receive first from c1 then c2 (thus ONE is assigned to c1, TWO to c2)
     * <br>
     * Then close c1 before acking.
     * <br>
     * If we now attempt to receive from c2, then the remaining messages in group ONE should be available (which
     * requires c2 to go "backwards" in the queue).
     **/
    private void consumerCloseGroupAssignment(final JmsSupport jms, boolean sharedGroups) throws Exception
    {
        final var groupKey = getGroupKey(false);
        final var queue = jms.builder().queue()
                .durable(false)
                .messageGroupKeyOverride("group")
                .messageGroupType(sharedGroups ? MessageGroupType.SHARED_GROUPS : MessageGroupType.STANDARD)
                .create();

        try (final var producerConnection = jms.builder().connection().create())
        {
            final var producerSession = producerConnection.createSession(true, SESSION_TRANSACTED);
            final var producer = producerSession.createProducer(queue);

            producer.send(createMessage(producerSession, 1, "ONE", false));
            producer.send(createMessage(producerSession, 2, "ONE", false));
            producer.send(createMessage(producerSession, 3, "TWO", false));
            producer.send(createMessage(producerSession, 4, "ONE", false));
            producerSession.commit();
        }

        try (final var consumerConnection = jms.builder().connection().prefetch(0).create())
        {
            consumerConnection.start();
            final var cs1 = consumerConnection.createSession(true, SESSION_TRANSACTED);
            final var cs2 = consumerConnection.createSession(true, SESSION_TRANSACTED);

            final var consumer1 = cs1.createConsumer(queue);
            final var consumer2 = cs2.createConsumer(queue);

            final var cs1Received = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(cs1Received, "Consumer 1 should have received first message");
            assertEquals(1, cs1Received.getIntProperty("msg"), "incorrect message received");

            final var cs2Received = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(cs2Received, "Consumer 2 should have received first message");
            assertEquals(3, cs2Received.getIntProperty("msg"), "incorrect message received");
            cs2.commit();

            final var cs2Received2 = consumer2.receive(Timeouts.noMessagesMillis());

            assertNull(cs2Received2, "Consumer 2 should not yet have received a second message");

            consumer1.close();

            cs1.commit();
            final var cs2Received3 = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(cs2Received3, "Consumer 2 should have received second message");
            assertEquals("ONE", cs2Received3.getStringProperty(groupKey), "Unexpected group");
            assertEquals(2, cs2Received3.getIntProperty("msg"), "incorrect message received");

            cs2.commit();

            final var cs2Received4 = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(cs2Received4, "Consumer 2 should have received third message");
            assertEquals("ONE", cs2Received4.getStringProperty(groupKey), "Unexpected group");
            assertEquals(4, cs2Received4.getIntProperty("msg"), "incorrect message received");
            cs2.commit();

            assertNull(consumer2.receive(Timeouts.noMessagesMillis()));
        }
    }

    @Test
    void consumerCloseWithRelease(final JmsSupport jms) throws Exception
    {
        consumerCloseWithRelease(jms, false);
    }

    @Test
    void sharedGroupConsumerCloseWithRelease(final JmsSupport jms) throws Exception
    {
        consumerCloseWithRelease(jms, true);
    }

    /**
     * Tests that upon closing a consumer and its session, groups previously assigned to that consumer are reassigned
     * to a different consumer, including messages which were previously delivered but have now been released.
     * <br>
     * Pre-populate the queue as ONE, ONE, TWO, ONE
     * <br>
     * create in sequence two consumers
     * <br>
     * receive first from c1 then c2 (thus ONE is assigned to c1, TWO to c2)
     * <br>
     * Then close c1 and its session without acking.
     * <br>
     * If we now attempt to receive from c2, then the all messages in group ONE should be available (which
     * requires c2 to go "backwards" in the queue). The first such message should be marked as redelivered
     */
    private void consumerCloseWithRelease(final JmsSupport jms, boolean sharedGroups) throws Exception
    {
        final var groupKey = getGroupKey(false);
        final var queue = jms.builder().queue()
                .durable(false)
                .messageGroupKeyOverride("group")
                .messageGroupType(sharedGroups ? MessageGroupType.SHARED_GROUPS : MessageGroupType.STANDARD)
                .create();

        try (final var producerConnection = jms.builder().connection().create())
        {
            producerConnection.start();
            final var producerSession = producerConnection.createSession(true, SESSION_TRANSACTED);
            final var producer = producerSession.createProducer(queue);

            producer.send(createMessage(producerSession, 1, "ONE", false));
            producer.send(createMessage(producerSession, 2, "ONE", false));
            producer.send(createMessage(producerSession, 3, "TWO", false));
            producer.send(createMessage(producerSession, 4, "ONE", false));
            producerSession.commit();
        }

        try (final var consumerConnection = jms.builder().connection().prefetch(0).create())
        {
            consumerConnection.start();

            final var cs1 = consumerConnection.createSession(true, SESSION_TRANSACTED);
            final var cs2 = consumerConnection.createSession(true, SESSION_TRANSACTED);

            final var consumer1 = cs1.createConsumer(queue);
            final var consumer2 = cs2.createConsumer(queue);

            final var cs1Received = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(cs1Received, "Consumer 1 should have received its first message");
            assertEquals(1, cs1Received.getIntProperty("msg"), "incorrect message received");

            var received = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(received, "Consumer 2 should have received its first message");
            assertEquals(3, received.getIntProperty("msg"), "incorrect message received");

            final var received2 = consumer2.receive(Timeouts.noMessagesMillis());

            assertNull(received2, "Consumer 2 should not yet have received second message");

            consumer1.close();
            cs1.close();
            cs2.commit();

            received = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(received, "Consumer 2 should now have received second message");
            assertEquals("ONE", received.getStringProperty(groupKey), "Unexpected group");
            assertEquals(1, received.getIntProperty("msg"), "incorrect message received");
            assertTrue(received.getJMSRedelivered(),
                    "Expected second message to be marked as redelivered " + received.getIntProperty("msg"));

            cs2.commit();

            received = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(received, "Consumer 2 should have received a third message");
            assertEquals("ONE", received.getStringProperty(groupKey), "Unexpected group");
            assertEquals(2, received.getIntProperty("msg"), "incorrect message received");

            cs2.commit();

            received = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(received, "Consumer 2 should have received a fourth message");
            assertEquals("ONE", received.getStringProperty(groupKey), "Unexpected group");
            assertEquals(4, received.getIntProperty("msg"), "incorrect message received");

            cs2.commit();

            assertNull(consumer2.receive(Timeouts.noMessagesMillis()));
        }
    }

    @Test
    void groupAssignmentSurvivesEmpty(final JmsSupport jms) throws Exception
    {
        groupAssignmentOnEmpty(jms, false);
    }

    @Test
    void sharedGroupAssignmentDoesNotSurviveEmpty(final JmsSupport jms) throws Exception
    {
        groupAssignmentOnEmpty(jms, true);
    }

    private void groupAssignmentOnEmpty(final JmsSupport jms, boolean sharedGroups) throws Exception
    {
        final var queue = jms.builder().queue()
                .durable(false)
                .messageGroupKeyOverride("group")
                .messageGroupType(sharedGroups ? MessageGroupType.SHARED_GROUPS : MessageGroupType.STANDARD)
                .create();

        try (final var producerConnection = jms.builder().connection().create())
        {
            producerConnection.start();
            final var producerSession = producerConnection.createSession(true, SESSION_TRANSACTED);
            final var producer = producerSession.createProducer(queue);

            producer.send(createMessage(producerSession, 1, "ONE", false));
            producer.send(createMessage(producerSession, 2, "TWO", false));
            producer.send(createMessage(producerSession, 3, "THREE", false));
            producer.send(createMessage(producerSession, 4, "ONE", false));
            producerSession.commit();
        }

        try (final var consumerConnection = jms.builder().connection().prefetch(0).create())
        {
            consumerConnection.start();

            final var cs1 = consumerConnection.createSession(true, SESSION_TRANSACTED);
            final var cs2 = consumerConnection.createSession(true, SESSION_TRANSACTED);

            final var consumer1 = cs1.createConsumer(queue);
            final var consumer2 = cs2.createConsumer(queue);

            var cs1Received = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(cs1Received, "Consumer 1 should have received its first message");
            assertEquals(1, cs1Received.getIntProperty("msg"), "incorrect message received");

            var cs2Received = consumer2.receive(Timeouts.receiveMillis());

            assertNotNull(cs2Received, "Consumer 2 should have received its first message");
            assertEquals(2, cs2Received.getIntProperty("msg"), "incorrect message received");

            cs1.commit();

            cs1Received = consumer1.receive(Timeouts.receiveMillis());
            assertNotNull(cs1Received, "Consumer 1 should have received its second message");
            assertEquals(3, cs1Received.getIntProperty("msg"), "incorrect message received");

            // We expect different behaviors from "shared groups": here the assignment of a subscription to a group
            // is terminated when there are no outstanding delivered but unacknowledged messages.  In contrast, with a
            // standard message grouping queue the assignment will be retained until the subscription is no longer
            // registered
            if (sharedGroups)
            {
                cs2.commit();
                cs2Received = consumer2.receive(Timeouts.receiveMillis());

                assertNotNull(cs2Received, "Consumer 2 should have received its second message");
                assertEquals(4, cs2Received.getIntProperty("msg"), "incorrect message received");

                cs2.commit();
            }
            else
            {
                cs2.commit();
                cs2Received = consumer2.receive(Timeouts.noMessagesMillis());

                assertNull(cs2Received, "Consumer 2 should not have received a second message");

                cs1.commit();

                cs1Received = consumer1.receive(Timeouts.receiveMillis());
                assertNotNull(cs1Received, "Consumer 1 should have received its third message");
                assertEquals(4, cs1Received.getIntProperty("msg"), "incorrect message received");
            }
        }
    }

    /**
     * Tests that when a number of new messages for a given groupid are arriving while the delivery group
     * state is also in the process of being emptied (due to acking a message while using prefetch=1), that only
     * 1 of a number of existing consumers is ever receiving messages for the shared group at a time.
     */
    @Test
    void singleSharedGroupWithMultipleConsumers(final JmsSupport jms) throws Exception
    {
        final boolean useDefaultGroup = false;
        final var queue = jms.builder().queue()
                .durable(false)
                .messageGroupKeyOverride("group")
                .messageGroupType(MessageGroupType.SHARED_GROUPS)
                .create();

        try (final var consumerConnection = jms.builder().connection().prefetch(1).create())
        {
            final var numMessages = 100;
            final var groupingTestMessageListener = new SharedGroupTestMessageListener(numMessages)
                    .withArtificialDelay(25, 10);

            final var cs1 = consumerConnection.createSession(false, AUTO_ACKNOWLEDGE);
            final var cs2 = consumerConnection.createSession(false, AUTO_ACKNOWLEDGE);
            final var cs3 = consumerConnection.createSession(false, AUTO_ACKNOWLEDGE);
            final var cs4 = consumerConnection.createSession(false, AUTO_ACKNOWLEDGE);

            final var consumer1 = cs1.createConsumer(queue);
            consumer1.setMessageListener(groupingTestMessageListener);
            final var consumer2 = cs2.createConsumer(queue);
            consumer2.setMessageListener(groupingTestMessageListener);
            final var consumer3 = cs3.createConsumer(queue);
            consumer3.setMessageListener(groupingTestMessageListener);
            final var consumer4 = cs4.createConsumer(queue);
            consumer4.setMessageListener(groupingTestMessageListener);

            consumerConnection.start();

            try (final var producerConnection = jms.builder().connection().create())
            {
                final var producerSession = producerConnection.createSession(true, SESSION_TRANSACTED);
                final var producer = producerSession.createProducer(queue);

                for (int i = 1; i <= numMessages; i++)
                {
                    producer.send(createMessage(producerSession, i, "GROUP", useDefaultGroup));
                }

                producerSession.commit();
            }
            finally
            {
                groupingTestMessageListener.disableDelay();
            }

            assertTrue(groupingTestMessageListener.waitForLatch(30),
                    "Mesages not all received in the allowed timeframe");
            assertEquals(0, groupingTestMessageListener.getConcurrentProcessingCases(),
                    "Unexpected concurrent processing of messages for the group");
            assertNull(groupingTestMessageListener.getThrowable(), "Unexpected throwable in message listeners");
        }
    }

    private Message createMessage(final Session session, int msg, String group, final boolean useDefaultGroup) throws JMSException
    {
        final var send = session.createTextMessage("Message: " + msg);
        send.setIntProperty("msg", msg);
        send.setStringProperty(getGroupKey(useDefaultGroup), group);
        return send;
    }

    private String getGroupKey(final boolean useDefaultGroup)
    {
        return useDefaultGroup ? "JMSXGroupID" : "group";
    }

    public static class SharedGroupTestMessageListener implements MessageListener
    {
        private final CountDownLatch count;
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger concurrent = new AtomicInteger();
        private final AtomicInteger delayed = new AtomicInteger();

        private final java.util.concurrent.Semaphore delayGate = new java.util.concurrent.Semaphore(0);
        private volatile boolean delayEnabled = false;
        private volatile int delayFirstN = 0;
        private volatile long delayMs = 0;

        private Throwable throwable;

        SharedGroupTestMessageListener(int numMessages)
        {
            this.count = new CountDownLatch(numMessages);
        }

        SharedGroupTestMessageListener withArtificialDelay(long delayMs, int firstNMessages)
        {
            this.delayMs = delayMs;
            this.delayFirstN = firstNMessages;
            this.delayEnabled = true;
            return this;
        }

        void disableDelay()
        {
            delayEnabled = false;
            delayGate.release(Integer.MAX_VALUE / 2);
        }

        @Override
        public void onMessage(Message message)
        {
            int currentActive = active.incrementAndGet();
            try
            {
                if (currentActive > 1)
                {
                    concurrent.incrementAndGet();
                    LOGGER.error("Concurrent processing when handling message: {}", message.getIntProperty("msg"));
                    disableDelay(); // fail-fast
                }

                if (delayEnabled && delayed.incrementAndGet() <= delayFirstN)
                {
                    try
                    {
                        delayGate.tryAcquire(1, delayMs, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            catch (Throwable t)
            {
                LOGGER.error("Unexpected throwable received by listener", t);
                throwable = t;
            }
            finally
            {
                active.decrementAndGet();
                count.countDown();
            }
        }

        boolean waitForLatch(int seconds) throws InterruptedException
        {
            return count.await(seconds, TimeUnit.SECONDS);
        }

        int getConcurrentProcessingCases()
        {
            return concurrent.get();
        }

        Throwable getThrowable()
        {
            return throwable;
        }
    }
}
