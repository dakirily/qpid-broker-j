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

import static org.apache.qpid.server.virtualhost.berkeleydb.BDBVirtualHost.QPID_BROKER_BDB_RECOVERY_SNAPSHOT_MAX_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

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
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
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
            final VirtualHostNode<?> node = launcher._broker.createChild(VirtualHostNode.class, Map.of(
                    VirtualHostNode.NAME, NODE_NAME, VirtualHostNode.TYPE, nodeType,
                    VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION,
                    MAPPER.writeValueAsString(Map.of(VirtualHost.TYPE, hostType))));
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

        @Override
        public void close()
        {
            _launcher.shutdown();
        }
    }
}
