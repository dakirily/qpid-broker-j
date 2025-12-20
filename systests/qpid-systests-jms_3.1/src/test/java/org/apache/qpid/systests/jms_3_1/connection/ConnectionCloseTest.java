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

package org.apache.qpid.systests.jms_3_1.connection;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;

import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;

import org.apache.qpid.systests.Timeouts;
import org.apache.qpid.systests.support.AsyncSupport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on close semantics for connections and sessions.
 * <br>
 * The tests in this class are aligned with:
 * - 6.1.8 "Closing a connection"
 * - 6.2.15 "Closing a session"
 */
@JmsSystemTest
@Tag("connection")
@Tag("queue")
class ConnectionCloseTest
{
    /**
     * Verifies that Connection.close() unblocks a pending synchronous receive
     * as required by section 6.1.8.
     */
    @Test
    void connectionCloseUnblocksPendingReceive(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            final Object result = awaitReceiveAndClose(() -> consumer.receive(30000), connection::close, async);

            assertTrue(result == null || result instanceof Exception,
                    "Pending receive should be unblocked by Connection.close()");
        }
    }

    /**
     * Verifies that Session.close() unblocks a pending synchronous receive
     * as required by section 6.2.15.
     */
    @Test
    void sessionCloseUnblocksPendingReceive(final JmsSupport jms, final AsyncSupport async) throws Exception
    {
        final var queue = jms.builder().queue().create();
        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            final Object result = awaitReceiveAndClose(() -> consumer.receive(30000), session::close, async);

            assertTrue(result == null || result instanceof Exception,
                    "Pending receive should be unblocked by Session.close()");
        }
    }

    private Object awaitReceiveAndClose(final Callable<Message> receiveCall,
                                        final CheckedRunnable closeAction,
                                        final AsyncSupport async) throws Exception
    {
        final var task = async.start("blocking receive", () ->
        {
            try
            {
                return receiveCall.call();
            }
            catch (Exception e)
            {
                return e;
            }
        });

        task.awaitStarted(Timeouts.receive(), "Receive task didn't start");

        closeAction.run();

        return task.await(Timeouts.receive(), "Receive not unblocked after close()");
    }

    @FunctionalInterface
    private interface CheckedRunnable
    {
        void run() throws Exception;
    }
}
