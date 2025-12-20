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

package org.apache.qpid.systests.jms_3_1.context;

import static jakarta.jms.JMSContext.AUTO_ACKNOWLEDGE;
import static jakarta.jms.JMSContext.CLIENT_ACKNOWLEDGE;
import static jakarta.jms.JMSContext.DUPS_OK_ACKNOWLEDGE;
import static jakarta.jms.JMSContext.SESSION_TRANSACTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * JMSContext tests according to 2.8 and 6.x specification
 */
@JmsSystemTest
class JMSContextSessionModeTest
{
    /**
     * Create JMSContext for each standard mode, send and read message via simplified API.
     * For SESSION_TRANSACTED commit() should be called to end the local transaction
     */
    @ParameterizedTest
    @ValueSource(ints = { AUTO_ACKNOWLEDGE, CLIENT_ACKNOWLEDGE, DUPS_OK_ACKNOWLEDGE, SESSION_TRANSACTED })
    @Tag("queue")
    @Tag("transactions")
    void createContextWithAllSessionModes(final int sessionMode, final JmsSupport jms) throws Exception
    {
        final ConnectionFactory cf = jms.builder().connection().connectionFactory();

        final var queue = jms.builder().queue().nameSuffix("_" + sessionMode).create();

        try (JMSContext context = cf.createContext(sessionMode))
        {
            final JMSProducer producer = context.createProducer();
            final JMSConsumer consumer = context.createConsumer(queue);

            final String expectedBody = "body-" + sessionMode;
            producer.send(queue, expectedBody);

            // when using transacted mode, context should call commit() to make message "visible"
            if (sessionMode == SESSION_TRANSACTED)
            {
                context.commit();
            }

            final String actualBody = consumer.receiveBody(String.class, Timeouts.receiveMillis());

            assertEquals(expectedBody, actualBody, "Unexpected body for session mode " + sessionMode);
        }
    }

    /**
     * Check local transaction using mode SESSION_TRANSACTED:
     *  - read message, then rollback;
     *  - same message is redelivered with the flag JMSRedelivered;
     *  - after commit() message is not delivered again
     */
    @Test
    void rollbackAndCommitInTransactedContext(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();

        final var cf = jms.builder().connection().connectionFactory();

        // send message using non-transacted context
        try (final var sendingContext = cf.createContext(AUTO_ACKNOWLEDGE))
        {
            sendingContext.createProducer().send(queue, "payload");
        }

        try (final var txContext = cf.createContext(SESSION_TRANSACTED))
        {
            assertTrue(txContext.getTransacted());
            assertEquals(SESSION_TRANSACTED, txContext.getSessionMode());

            final JMSConsumer consumer = txContext.createConsumer(queue);

            // first delivery
            final Message firstDelivery = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(firstDelivery, "Message was not received before rollback");

            final String messageId = firstDelivery.getJMSMessageID();

            assertEquals(1, firstDelivery.getIntProperty("JMSXDeliveryCount"));

            // rollback local transaction => message should be delivered again
            txContext.rollback();

            final Message redelivered = consumer.receive(Timeouts.receiveMillis());
            assertNotNull(redelivered, "Message was not redelivered after rollback");
            assertEquals(messageId, redelivered.getJMSMessageID(), "Redelivered message must have the same JMSMessageID");
            // after the rollback broker must set JMSRedelivered = true.
            assertTrue(redelivered.getJMSRedelivered(), "JMSRedelivered should be true after rollback");
            // after the rollback broker must increment JMSXDeliveryCount
            assertEquals(2, redelivered.getIntProperty("JMSXDeliveryCount"));

            // commit transaction => message should not be delivered anymore
            txContext.commit();

            final Message afterCommit = consumer.receive(Timeouts.noMessagesMillis());
            assertNull(afterCommit, "No further deliveries expected after commit");
        }
    }

    /**
     * Negative scenario:
     * non-transacted modes (AUTO_ACKNOWLEDGE, CLIENT_ACKNOWLEDGE, DUPS_OK_ACKNOWLEDGE) must throw JMSRuntimeException
     * on JMSContext#commit() / JMSContext#rollback()
     */
    @ParameterizedTest
    @ValueSource(ints = { AUTO_ACKNOWLEDGE, CLIENT_ACKNOWLEDGE, DUPS_OK_ACKNOWLEDGE })
    void commitRollbackOnNonTransactedContextThrowsJMSRuntimeException(final int sessionMode, final JmsSupport jms) throws Exception
    {
        final var connectionFactory = jms.builder().connection().connectionFactory();

        try (final var context = connectionFactory.createContext(sessionMode))
        {
            assertThrows(JMSRuntimeException.class, context::commit,
                    "Expected JMSRuntimeException from commit() on non-transacted context with mode " + sessionMode);

            assertThrows(JMSRuntimeException.class, context::rollback,
                    "Expected JMSRuntimeException from rollback() on non-transacted context with mode " + sessionMode);
        }
    }
}
