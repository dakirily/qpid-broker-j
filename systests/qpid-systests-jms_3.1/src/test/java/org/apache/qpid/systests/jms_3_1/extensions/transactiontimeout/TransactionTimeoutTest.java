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

package org.apache.qpid.systests.jms_3_1.extensions.transactiontimeout;

import static org.apache.qpid.systests.JmsAwait.jms;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.tests.utils.VirtualhostContextVariable;

@JmsSystemTest
@Tag("transactions")
class TransactionTimeoutTest
{
    private static final Long CLOSE_TIME = 500L;
    private final ExceptionCatchingListener _listener = new ExceptionCatchingListener();

    @BeforeEach
    void setUp(final JmsSupport jms)
    {
        assumeTrue(jms.brokerAdmin().isManagementSupported());
    }

    @Test
    @VirtualhostContextVariable(name = "virtualhost.storeTransactionIdleTimeoutClose", value = "500")
    void producerTransactionIdle(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        // exception may be thrown on connection.close() therefore try-with-resources doesn't work here
        final var connection = jms.builder().connection().create();
        try
        {
            connection.setExceptionListener(_listener);
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var producer = session.createProducer(queue);

            _listener.assertNoException(CLOSE_TIME * 2);
            producer.send(session.createMessage());
            _listener.assertConnectionExceptionReported(CLOSE_TIME * 4);

            assertThrows(JMSException.class, session::commit, "Exception not thrown");
        }
        finally
        {
            try
            {
                connection.close();
            }
            catch (JMSException ignore)
            {
                // ignore
            }
        }
    }

    @Test
    @VirtualhostContextVariable(name = "virtualhost.storeTransactionOpenTimeoutClose", value = "500")
    void producerTransactionOpen(final JmsSupport jms) throws Exception
    {
        final var queue = jms.builder().queue().create();
        final var connection = jms.builder().connection().create();
        // exception may be thrown on connection.close() therefore try-with-resources doesn't work here
        try
        {
            connection.setExceptionListener(_listener);
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var producer = session.createProducer(queue);
            final long halfTime = System.currentTimeMillis() + CLOSE_TIME / 2;

            jms(Duration.ofMillis(CLOSE_TIME))
                    .pollInterval(Duration.ofMillis(50))
                    .until(() ->
                    {
                        producer.send(session.createMessage());
                        return System.currentTimeMillis() >= halfTime;
                    });

            final long fullTime = System.currentTimeMillis() + CLOSE_TIME;
            final AtomicBoolean exceptionReceived = new AtomicBoolean(false);

            jms(Duration.ofMillis(CLOSE_TIME).plusSeconds(5))
                    .pollInterval(Duration.ofMillis(50))
                    .until(() ->
                    {
                        try
                        {
                            producer.send(session.createMessage());
                        }
                        catch (JMSException e)
                        {
                            exceptionReceived.set(true);
                        }
                        return System.currentTimeMillis() >= fullTime && exceptionReceived.get();
                    });

            assertThat("Transaction open for an excessive length of time was not closed",
                    exceptionReceived.get(), is(equalTo(true)));

            _listener.assertConnectionExceptionReported(CLOSE_TIME * 4);

            assertThrows(JMSException.class, session::commit, "Exception not thrown");
        }
        finally
        {
            try
            {
                connection.close();
            }
            catch (JMSException ignore)
            {
                // ignore
            }
        }
    }

    private static class ExceptionCatchingListener implements ExceptionListener
    {
        private final CompletableFuture<JMSException> _future = new CompletableFuture<>();

        @Override
        public void onException(final JMSException exception)
        {
            _future.complete(exception);
        }

        void assertConnectionExceptionReported(final long time) throws Exception
        {
            final JMSException jmsException = _future.get(time, TimeUnit.MILLISECONDS);
            assertThat(jmsException.getMessage(), containsString("transaction timed out"));
        }

        void assertNoException(final long time) throws Exception
        {
            try
            {
                _future.get(time, TimeUnit.MILLISECONDS);
                assertThat("Exception unexpectedly received by listener", _future.isDone(), is(equalTo(true)));
            }
            catch (TimeoutException e)
            {
                // PASS
            }
        }
    }
}
