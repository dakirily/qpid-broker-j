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

package org.apache.qpid.systests.jms_3_1.connection;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.Timeouts;
import org.apache.qpid.test.utils.TCPTunneler;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the behavior of the Broker when the client's connection is unexpectedly
 * severed.  Test uses a TCP tunneler which is halted by the test in order to
 * simulate a sudden client failure.
 */
@JmsSystemTest
@Tag("connection")
class AbruptClientDisconnectTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbruptClientDisconnectTest.class);

    private TCPTunneler _tcpTunneler;
    private Connection _tunneledConnection;
    private ExecutorService _executorService;
    private Queue _testQueue;
    private Connection _utilityConnection;

    @BeforeEach
    void setUp(final JmsSupport jms) throws Exception
    {
        _executorService = Executors.newFixedThreadPool(3);

        _utilityConnection = jms.builder().connection().create();
        _utilityConnection.start();

        // create queue
        _testQueue = jms.builder().queue().create();

        final InetSocketAddress brokerAddress = jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        _tcpTunneler = new TCPTunneler(0, brokerAddress.getHostName(), brokerAddress.getPort(), 1);
        _tcpTunneler.start();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        try
        {
            try
            {
                if (_tunneledConnection != null)
                {
                    _tunneledConnection.close();
                }
            }
            finally
            {
                if (_utilityConnection != null)
                {
                    _utilityConnection.close();
                }
            }
        }
        finally
        {
            try
            {
                if (_tcpTunneler != null)
                {
                    _tcpTunneler.stop();
                }
            }
            finally
            {
                if (_executorService != null)
                {
                    _executorService.shutdown();
                    assertTrue(_executorService.awaitTermination(2000L, TimeUnit.MILLISECONDS),
                            "Executor didn't shutdown in time");
                }
            }
        }
    }

    @Test
    void messagingOnAbruptConnectivityLostWhilstPublishing(final JmsSupport jms) throws Exception
    {
        final ClientMonitor clientMonitor = new ClientMonitor();
        _tunneledConnection = createTunneledConnection(jms, clientMonitor);
        Producer producer =
                new Producer(_tunneledConnection, _testQueue, Session.SESSION_TRANSACTED, 10,
                             () -> _tcpTunneler.disconnect(clientMonitor.getClientAddress())
                );
        _executorService.submit(producer);
        boolean disconnected = clientMonitor.awaitDisconnect(10, TimeUnit.SECONDS);
        producer.stop();
        assertTrue(disconnected, "Client disconnect did not happen");
        assertTrue(producer.getNumberOfPublished() >= 10,
                "Unexpected number of published messages " + producer.getNumberOfPublished());

        consumeIgnoringLastSeenOmission(jms, _utilityConnection, _testQueue, 0, producer.getNumberOfPublished(), -1);
    }

    @Test
    void messagingOnAbruptConnectivityLostWhilstConsuming(final JmsSupport jms) throws Exception
    {
        int minimumNumberOfMessagesToProduce = 40;
        int minimumNumberOfMessagesToConsume = 20;

        // produce minimum required number of messages before starting consumption
        final CountDownLatch queueDataWaiter = new CountDownLatch(1);
        final Producer producer = new Producer(_utilityConnection,
                                               _testQueue,
                                               Session.SESSION_TRANSACTED,
                                               minimumNumberOfMessagesToProduce,
                                               queueDataWaiter::countDown);

        // create tunneled connection to consume messages
        final ClientMonitor clientMonitor = new ClientMonitor();
        _tunneledConnection = createTunneledConnection(jms, clientMonitor);
        _tunneledConnection.start();

        // consumer will consume minimum number of messages before abrupt disconnect
        Consumer consumer = new Consumer(jms, _tunneledConnection,
                                         _testQueue,
                                         Session.SESSION_TRANSACTED,
                                         minimumNumberOfMessagesToConsume,
                                         () -> {
                                             try
                                             {
                                                 _tcpTunneler.disconnect(clientMonitor.getClientAddress());
                                             }
                                             finally
                                             {
                                                 producer.stop();
                                             }
                                         }
        );

        LOGGER.debug("Waiting for producer to produce {} messages before consuming", minimumNumberOfMessagesToProduce);
        _executorService.submit(producer);

        assertTrue(queueDataWaiter.await(10, TimeUnit.SECONDS),
                "Latch waiting for produced messages was not count down");

        LOGGER.debug("Producer sent {} messages. Starting consumption...", producer.getNumberOfPublished());

        _executorService.submit(consumer);

        boolean disconnectOccurred = clientMonitor.awaitDisconnect(10, TimeUnit.SECONDS);

        LOGGER.debug("Stopping consumer and producer");
        consumer.stop();
        producer.stop();

        LOGGER.debug("Producer sent {} messages. Consumer received {} messages",
                     producer.getNumberOfPublished(),
                     consumer.getNumberOfConsumed());

        assertTrue(disconnectOccurred, "Client disconnect did not happen");
        assertTrue(producer.getNumberOfPublished() >= minimumNumberOfMessagesToProduce,
                "Unexpected number of published messages " + producer.getNumberOfPublished());
        assertTrue(consumer.getNumberOfConsumed() >= minimumNumberOfMessagesToConsume,
                "Unexpected number of consumed messages " + consumer.getNumberOfConsumed());

        LOGGER.debug("Remaining number to consume {}.",
                     (producer.getNumberOfPublished() - consumer.getNumberOfConsumed()));
        consumeIgnoringLastSeenOmission(jms, _utilityConnection,
                                        _testQueue,
                                        consumer.getNumberOfConsumed(),
                                        producer.getNumberOfPublished(),
                                        consumer.getLastSeenMessageIndex());
    }

    private Connection createTunneledConnection(final JmsSupport jms, final ClientMonitor clientMonitor) throws Exception
    {
        final int localPort = _tcpTunneler.getLocalPort();
        final var tunneledConnection = jms.builder().connection().port(localPort).create();
        _tcpTunneler.addClientListener(clientMonitor);
        final AtomicReference<JMSException> _exception = new AtomicReference<>();
        tunneledConnection.setExceptionListener(exception -> {
            _exception.set(exception);
            _tcpTunneler.disconnect(clientMonitor.getClientAddress());
        });
        return tunneledConnection;
    }

    private void consumeIgnoringLastSeenOmission(final JmsSupport jms,
                                                 final Connection connection,
                                                 final Queue testQueue,
                                                 final int fromIndex,
                                                 final int toIndex,
                                                 final int consumerLastSeenMessageIndex) throws JMSException
    {
        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var consumer = session.createConsumer(testQueue))
        {
            int expectedIndex = fromIndex;
            while (expectedIndex < toIndex)
            {
                final var message = consumer.receive(Timeouts.receiveMillis());
                if (message == null && consumerLastSeenMessageIndex + 1 == toIndex)
                {
                    // this is a corner case when one remaining message is expected
                    // but it was already received previously, Commit was sent
                    // and broker successfully committed and sent back CommitOk
                    // but CommitOk did not reach client due to abrupt disconnect
                    LOGGER.debug( "Broker transaction was completed for message {}"
                                    + " but there was no network to notify client about its completion.",
                            consumerLastSeenMessageIndex);
                }
                else
                {
                    assertNotNull(message, "Expected message with index " + expectedIndex + " but got null");
                    int messageIndex = message.getIntProperty(INDEX);
                    LOGGER.debug("Received message with index {}, expected index is {}", messageIndex, expectedIndex);
                    if (messageIndex != expectedIndex && expectedIndex == fromIndex &&
                            messageIndex == consumerLastSeenMessageIndex + 1)
                    {
                        LOGGER.debug("Broker transaction was completed for message {}"
                                        + " but there was no network to notify client about its completion.",
                                consumerLastSeenMessageIndex);
                        expectedIndex = messageIndex;
                    }
                    assertEquals(expectedIndex, messageIndex, "Unexpected message index");
                }
                expectedIndex++;
            }
        }
    }

    private void threadJoin(final Thread thread)
    {
        if (thread != null)
        {
            try
            {
                thread.join(2000);
            }
            catch (InterruptedException e)
            {
                thread.interrupt();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class ClientMonitor implements TCPTunneler.TunnelListener
    {
        private final CountDownLatch _closeLatch = new CountDownLatch(1);

        private final AtomicReference<InetSocketAddress> _clientAddress = new AtomicReference<>();

        @Override
        public void clientConnected(final InetSocketAddress clientAddress)
        {
            _clientAddress.set(clientAddress);
        }

        @Override
        public void clientDisconnected(final InetSocketAddress clientAddress)
        {
            if (clientAddress.equals(getClientAddress()))
            {
                _closeLatch.countDown();
            }
        }

        boolean awaitDisconnect(int period, TimeUnit timeUnit) throws InterruptedException
        {
            return _closeLatch.await(period, timeUnit);
        }

        InetSocketAddress getClientAddress()
        {
            return _clientAddress.get();
        }

        @Override
        public void notifyClientToServerBytesDelivered(final InetAddress inetAddress, final int numberOfBytesForwarded)
        {
        }

        @Override
        public void notifyServerToClientBytesDelivered(final InetAddress inetAddress, final int numberOfBytesForwarded)
        {
        }
    }

    private class Producer implements Runnable
    {
        private final Runnable _runnable;
        private final Session _session;
        private final MessageProducer _messageProducer;
        private final int _numberOfMessagesToInvokeRunnableAfter;
        private final AtomicBoolean _closed = new AtomicBoolean();

        private volatile int _publishedMessageCounter;
        private volatile Exception _exception;
        private volatile Thread _thread;

        Producer(Connection connection, Destination queue, int acknowledgeMode,
                 int numberOfMessagesToInvokeRunnableAfter, Runnable runnableToInvoke)
                throws JMSException
        {
            _session = connection.createSession(acknowledgeMode == Session.SESSION_TRANSACTED, acknowledgeMode);
            _messageProducer = _session.createProducer(queue);
            _runnable = runnableToInvoke;
            _numberOfMessagesToInvokeRunnableAfter = numberOfMessagesToInvokeRunnableAfter;
        }

        @Override
        public void run()
        {
            _thread = Thread.currentThread();
            try
            {
                final var message = _session.createMessage();
                while (!_closed.get())
                {
                    if (_publishedMessageCounter == _numberOfMessagesToInvokeRunnableAfter && _runnable != null)
                    {
                        _executorService.execute(_runnable);
                    }

                    message.setIntProperty(INDEX, _publishedMessageCounter);
                    _messageProducer.send(message);
                    if (_session.getTransacted())
                    {
                        _session.commit();
                    }
                    LOGGER.debug("Produced message with index {}", _publishedMessageCounter);
                    _publishedMessageCounter++;
                }
                LOGGER.debug("Stopping producer gracefully");
            }
            catch (Exception e)
            {
                LOGGER.debug("Stopping producer due to exception", e);
                _exception = e;
            }
        }

        void stop()
        {
            if (_closed.compareAndSet(false, true))
            {
                threadJoin(_thread);
            }
        }


        int getNumberOfPublished()
        {
            return _publishedMessageCounter;
        }

        public Exception getException()
        {
            return _exception;
        }

    }

    private class Consumer implements Runnable
    {
        private final JmsSupport _jms;
        private final Runnable _runnable;
        private final Session _session;
        private final MessageConsumer _messageConsumer;
        private final int _numberOfMessagesToInvokeRunnableAfter;
        private final AtomicBoolean _closed = new AtomicBoolean();

        private volatile int _consumedMessageCounter;
        private volatile Exception _exception;
        private volatile Thread _thread;
        private volatile int _lastSeenMessageIndex;

        Consumer(final JmsSupport jms,
                 Connection connection,
                 Destination queue,
                 int acknowledgeMode,
                 int numberOfMessagesToInvokeRunnableAfter,
                 Runnable runnableToInvoke) throws JMSException
        {
            _jms = jms;
            _session = connection.createSession(acknowledgeMode == Session.SESSION_TRANSACTED, acknowledgeMode);
            _messageConsumer = _session.createConsumer(queue);
            _runnable = runnableToInvoke;
            _numberOfMessagesToInvokeRunnableAfter = numberOfMessagesToInvokeRunnableAfter;
        }

        @Override
        public void run()
        {
            _thread = Thread.currentThread();
            try
            {
                while (!_closed.get())
                {
                    if (_consumedMessageCounter == _numberOfMessagesToInvokeRunnableAfter && _runnable != null)
                    {
                        _executorService.execute(_runnable);
                    }

                    final var message = _messageConsumer.receive(Timeouts.receiveMillis());
                    if (message != null)
                    {
                        int messageIndex = message.getIntProperty(INDEX);
                        _lastSeenMessageIndex = messageIndex;
                        LOGGER.debug("Received message with index {}, expected index {}",
                                     messageIndex,
                                     _consumedMessageCounter);
                        assertEquals(_consumedMessageCounter, messageIndex, "Unexpected message index");

                        if (_session.getTransacted())
                        {
                            _session.commit();
                            LOGGER.debug("Committed message with index {}", messageIndex);
                        }
                        _consumedMessageCounter++;
                    }
                }
                LOGGER.debug("Stopping consumer gracefully");
            }
            catch (Exception e)
            {
                LOGGER.debug("Stopping consumer due to exception, number of consumed {}", _consumedMessageCounter, e);
                _exception = e;
            }
        }

        void stop()
        {
            if (_closed.compareAndSet(false, true))
            {
                threadJoin(_thread);
            }
        }

        int getNumberOfConsumed()
        {
            return _consumedMessageCounter;
        }

        public Exception getException()
        {
            return _exception;
        }

        int getLastSeenMessageIndex()
        {
            return _lastSeenMessageIndex;
        }
    }
}
