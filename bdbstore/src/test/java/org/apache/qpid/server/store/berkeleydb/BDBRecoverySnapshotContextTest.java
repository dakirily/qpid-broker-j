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
 */
package org.apache.qpid.server.store.berkeleydb;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.MessageLogger;
import org.apache.qpid.server.logging.messages.TransactionLogMessages;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.internal.InternalMessage;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

class BDBRecoverySnapshotContextTest extends UnitTestBase
{
    private static final String NODE_NAME = "recovery";
    private static final String ALIAS = "snapshotBytes";
    private static final String CAP = QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES;
    private static final String VALID_CAP = "67108864";
    private static final String INVALID_CAP = "8191";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    void setUp()
    {
        QpidByteBuffer.deinitialisePool();
    }

    @AfterEach
    void tearDown()
    {
        QpidByteBuffer.deinitialisePool();
    }

    @ParameterizedTest
    @CsvSource({"BDB, ProvidedStore", "JSON, BDB"})
    void testRejectsInvalidContextBeforePersistingIt(final String nodeType, final String hostType,
                                                     @TempDir final Path work) throws Exception
    {
        MAPPER.writeValue(work.resolve("config.json").toFile(),
                          Map.of(Broker.NAME, getTestName(), Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION));
        try (final Launcher launcher = new Launcher())
        {
            launcher.start(work);
            final VirtualHostNode<?> node = launcher.createNode(nodeType, hostType);
            final VirtualHost<?> host = node.getVirtualHost();
            assertEquals(1024, BDBRecoverySnapshotLimit.getBlockLimit(host));
            host.setAttributes(Map.of(VirtualHost.CONTEXT, Map.of(CAP, "${" + ALIAS + "}", ALIAS, VALID_CAP)));
            assertRecoveryLimit(host);

            for (final String invalidCap : List.of(INVALID_CAP, "64MiB", Long.toString(Long.MAX_VALUE)))
            {
                final IllegalConfigurationException failure = assertThrows(IllegalConfigurationException.class,
                        () -> host.setAttributes(Map.of(VirtualHost.CONTEXT, Map.of(CAP, invalidCap))));
                assertTrue(failure.getMessage().contains(CAP));
                assertRecoveryLimit(host);
            }

            // The running host can expand a reference to the old alias during the update.
            // The persisted expression must also be valid when resolved from the updated context on restart.
            assertThrows(IllegalConfigurationException.class,
                    () -> host.setAttributes(Map.of(VirtualHost.CONTEXT,
                                                    Map.of(CAP, "${" + ALIAS + "}", ALIAS, INVALID_CAP))));
            assertRecoveryLimit(host);

            node.setContextVariable(CAP, INVALID_CAP);
            assertThrows(IllegalConfigurationException.class, () -> host.removeContextVariable(CAP));
            assertRecoveryLimit(host);
            node.removeContextVariable(CAP);
        }

        try (final Launcher launcher = new Launcher())
        {
            launcher.start(work);
            assertRecoveryLimit(launcher._broker.getChildByName(VirtualHostNode.class, NODE_NAME).getVirtualHost());
        }
    }

    @ParameterizedTest
    @CsvSource({"BDB, ProvidedStore", "JSON, BDB"})
    void testQueueCopyWaitsForSynchronousRecovery(final String nodeType, final String hostType,
                                                   @TempDir final Path work) throws Exception
    {
        MAPPER.writeValue(work.resolve("config.json").toFile(),
                          Map.of(Broker.NAME, getTestName(), Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION));
        try (final Launcher launcher = new Launcher())
        {
            launcher.start(work);
            final QueueManagingVirtualHost<?> host =
                    (QueueManagingVirtualHost<?>) launcher.createNode(nodeType, hostType).getVirtualHost();
            host.setContextVariable(CAP, "8192");
            final Queue<?> source = host.createChild(Queue.class, Map.of(Queue.NAME, "source", Queue.DURABLE, true));
            host.createChild(Queue.class, Map.of(Queue.NAME, "destination", Queue.DURABLE, true));

            // With one-byte message ID deltas this exceeds the capacity of one 8 KiB snapshot block.
            final int messageCount = 8300;
            storeMessages(host.getMessageStore(), source, messageCount);
            host.setAttributes(Map.of(VirtualHost.DESIRED_STATE, State.STOPPED));

            final EventLogger eventLogger = host.getEventLogger();
            final MessageLogger originalLogger = eventLogger.getMessageLogger();
            final MessageLogger recoveryLogger = mock(MessageLogger.class);
            final CountDownLatch recoveryStarted = new CountDownLatch(1);
            final CountDownLatch resumeRecovery = new CountDownLatch(1);
            doAnswer(invocation ->
            {
                recoveryStarted.countDown();
                assertTrue(resumeRecovery.await(30, SECONDS));
                return null;
            }).when(recoveryLogger).message(any(LogSubject.class), argThat(message ->
                    TransactionLogMessages.RECOVERY_START_LOG_HIERARCHY.equals(message.getLogHierarchy())));

            final ExecutorService writer = Executors.newSingleThreadExecutor();
            eventLogger.setMessageLogger(recoveryLogger);
            try
            {
                final CompletableFuture<Void> restart =
                        host.setAttributesAsync(Map.of(VirtualHost.DESIRED_STATE, State.ACTIVE));
                assertTrue(recoveryStarted.await(30, SECONDS));
                final Queue<?> recoveringSource = host.getChildByName(Queue.class, "source");
                final Queue<?> destination = host.getChildByName(Queue.class, "destination");
                final CountDownLatch copyStarted = new CountDownLatch(1);
                final CompletableFuture<List<Long>> copy = CompletableFuture.supplyAsync(() ->
                {
                    copyStarted.countDown();
                    return recoveringSource.copyMessages(destination, null, null, 1);
                }, writer);

                assertTrue(copyStarted.await(30, SECONDS));
                assertThrows(TimeoutException.class, () -> copy.get(200, MILLISECONDS));
                resumeRecovery.countDown();
                restart.get(30, SECONDS);
                assertEquals(1, copy.get(30, SECONDS).size());
                assertEquals(State.ACTIVE, host.getState());
                assertEquals(messageCount, recoveringSource.getQueueDepthMessages());
                assertEquals(1, destination.getQueueDepthMessages());
            }
            finally
            {
                resumeRecovery.countDown();
                writer.shutdownNow();
                eventLogger.setMessageLogger(originalLogger);
            }
        }
    }

    private void storeMessages(final MessageStore store, final Queue<?> queue, final int count)
    {
        final AMQMessageHeader header = mock(AMQMessageHeader.class);
        final Transaction transaction = store.newTransaction();
        for (int i = 0; i < count; i++)
        {
            transaction.enqueueMessage(queue, InternalMessage.createStringMessage(store, header, "message", true));
        }
        transaction.commitTran();
    }

    private void assertRecoveryLimit(final VirtualHost<?> host)
    {
        assertEquals(State.ACTIVE, host.getState());
        assertEquals(VALID_CAP, host.getContext().get(ALIAS));
        assertEquals(8192, BDBRecoverySnapshotLimit.getBlockLimit(host));
    }

    private static final class Launcher implements AutoCloseable
    {
        private final SystemLauncher _launcher = new SystemLauncher(new SystemLauncherListener.DefaultSystemLauncherListener()
        {
            @Override
            public void onContainerResolve(final SystemConfig<?> config)
            {
                _broker = config.getContainer(Broker.class);
            }
        });
        private Broker<?> _broker;

        private void start(final Path work) throws Exception
        {
            _launcher.startup(Map.of(SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE, SystemConfig.CONTEXT,
                                     Map.of(SystemConfig.QPID_WORK_DIR, work.toString(),
                                            "use_async_message_store_recovery", false)));
            assertEquals(State.ACTIVE, _broker.getState());
        }

        private VirtualHostNode<?> createNode(final String nodeType, final String hostType) throws Exception
        {
            return _broker.createChild(VirtualHostNode.class, Map.of(
                    VirtualHostNode.NAME, NODE_NAME, VirtualHostNode.TYPE, nodeType,
                    VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION,
                    MAPPER.writeValueAsString(Map.of(VirtualHost.TYPE, hostType))));
        }

        @Override
        public void close()
        {
            _launcher.shutdown();
        }
    }
}
