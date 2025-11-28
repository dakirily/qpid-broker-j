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

package org.apache.qpid.systests.jms_2_0.context;

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
import jakarta.jms.Queue;

import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.JmsTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * JMSContext tests according to 2.8 and 6.x specification
 */
public class JMSContextSessionModeTest extends JmsTestBase
{
    /**
     * Create JMSContext for each standard mode, send and read message via simplified API.
     * For SESSION_TRANSACTED commit() should be called to end the local transaction
     */
    @ParameterizedTest
    @ValueSource(ints = { AUTO_ACKNOWLEDGE, CLIENT_ACKNOWLEDGE, DUPS_OK_ACKNOWLEDGE, SESSION_TRANSACTED })
    public void createContextWithAllSessionModes(final int sessionMode) throws Exception
    {
        final ConnectionFactory cf = getConnectionBuilder().buildConnectionFactory();

        final String queueName = getTestName() + "_" + sessionMode;
        final Queue queue = createQueue(queueName);

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

            final String actualBody = consumer.receiveBody(String.class, getReceiveTimeout());

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
    public void rollbackAndCommitInTransactedContext() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createQueue(queueName);

        final var cf = getConnectionBuilder().buildConnectionFactory();

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
            final Message firstDelivery = consumer.receive(getReceiveTimeout());
            assertNotNull(firstDelivery, "Message was not received before rollback");

            final String messageId = firstDelivery.getJMSMessageID();

            assertEquals(1, firstDelivery.getIntProperty("JMSXDeliveryCount"));

            // rollback local transaction => message should be delivered again
            txContext.rollback();

            final Message redelivered = consumer.receive(getReceiveTimeout());
            assertNotNull(redelivered, "Message was not redelivered after rollback");
            assertEquals(messageId, redelivered.getJMSMessageID(), "Redelivered message must have the same JMSMessageID");
            // after the rollback broker must set JMSRedelivered = true.
            assertTrue(redelivered.getJMSRedelivered(), "JMSRedelivered should be true after rollback");
            // after the rollback broker must increment JMSXDeliveryCount
            assertEquals(2, redelivered.getIntProperty("JMSXDeliveryCount"));

            // commit transaction => message should not be delivered anymore
            txContext.commit();

            final Message afterCommit = consumer.receive(getReceiveTimeout());
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
    public void commitRollbackOnNonTransactedContextThrowsJMSRuntimeException(final int sessionMode) throws Exception
    {
        final var connectionFactory = getConnectionBuilder().buildConnectionFactory();

        try (final var context = connectionFactory.createContext(sessionMode))
        {
            assertThrows(JMSRuntimeException.class, context::commit,
                    "Expected JMSRuntimeException from commit() on non-transacted context with mode " + sessionMode);

            assertThrows(JMSRuntimeException.class, context::rollback,
                    "Expected JMSRuntimeException from rollback() on non-transacted context with mode " + sessionMode);
        }
    }
}
