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

package org.apache.qpid.tests.utils;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.logging.logback.LogbackLoggingSystemLauncherListener;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.ManageableMessage;
import org.apache.qpid.server.model.NotFoundException;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.auth.TaskPrincipal;
import org.apache.qpid.server.store.MemoryConfigurationStore;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhostnode.JsonVirtualHostNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PluggableService
public class EmbeddedBrokerPerRunAdminImpl extends AbstractEmbeddedBrokerAdmin implements BrokerAdmin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedBrokerPerRunAdminImpl.class);

    /** BrokerAdmin type which can be referred using -Dqpid.tests.brokerAdminType */
    public static final String TYPE = "EMBEDDED_BROKER_PER_RUN";

    /** Shared broker on JVM level */
    private static final Object SHARED_LOCK = new Object();
    private static SharedBroker SHARED;

    /** Current VHN for a given test class (is not supposed for method-level parallelization) */
    private VirtualHostNode<?> _currentVirtualHostNode;
    private boolean _isPersistentStore;
    private String _currentStoreDir; // for cleanup

    @Override
    public void beforeTestClass(final Class testClass)
    {
        final List<ConfigItem> configItems = Arrays.asList((ConfigItem[]) testClass.getAnnotationsByType(ConfigItem.class));

        final Map<String, String> requiredJvmProps = configItems.stream()
                .filter(ConfigItem::jvm)
                .collect(Collectors.toMap(ConfigItem::name, ConfigItem::value, (a, b) -> b));

        final Map<String, String> requiredBrokerContext = configItems.stream()
                .filter(ci -> !ci.jvm())
                .collect(Collectors.toMap(ConfigItem::name, ConfigItem::value, (a, b) -> b));

        acquireSharedBroker(requiredJvmProps, requiredBrokerContext);
    }

    @Override
    public void afterTestClass(final Class testClass)
    {
        releaseSharedBroker();
    }

    @Override
    public void beforeTestMethod(final Class testClass, final Method method)
    {
        final SharedBroker shared = shared();

        final String virtualHostNodeName = testClass.getCanonicalName() + "_" + method.getName();

        final String blueprint = System.getProperty("virtualhostnode.context.blueprint");
        final String storeType = System.getProperty("virtualhostnode.type");

        Objects.requireNonNull(blueprint, "System property 'virtualhostnode.context.blueprint' needs to be provided");
        Objects.requireNonNull(storeType, "System property 'virtualhostnode.type' needs to be provided");

        _isPersistentStore = !"Memory".equals(storeType);

        _currentStoreDir = null;
        if (System.getProperty("profile", "").startsWith("java-dby-mem"))
        {
            _currentStoreDir = ":memory:";
        }
        else if (!MemoryConfigurationStore.TYPE.equals(storeType))
        {
            _currentStoreDir = shared.workDir + File.separator + virtualHostNodeName;
        }

        final Map<String, String> context = new HashMap<>();
        context.put("virtualhostBlueprint", blueprint);
        context.put(QueueManagingVirtualHost.VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE, "2");
        context.put(QueueManagingVirtualHost.VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS, "1");
        context.put("virtualhost.housekeepingThreadCount", "1");
        context.put("virtualhost.flowToDiskCheckPeriod", "0");
        if ("derby".equalsIgnoreCase(storeType))
        {
            context.put("qpid.jdbcstore.executorThreads", "2");
        }

        context.putAll(virtualhostContextVariables(testClass, method));

        LOGGER.info("Using context: {}", context);

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, virtualHostNodeName);
        attributes.put(ConfiguredObject.TYPE, storeType);
        attributes.put(ConfiguredObject.CONTEXT, context);
        attributes.put(VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION, blueprint);

        if (_currentStoreDir != null)
        {
            attributes.put(JsonVirtualHostNode.STORE_PATH, _currentStoreDir);
        }

        // create VHN using system subject (to be independent of thread-local principal)
        _currentVirtualHostNode = shared.doAsSystem("beforeTestMethod:createVhn", () ->
                shared.broker.createChild(VirtualHostNode.class, attributes));
    }

    @Override
    public void afterTestMethod(final Class testClass, final Method method)
    {
        final SharedBroker shared = shared();

        if (_currentVirtualHostNode == null)
        {
            return;
        }

        final VirtualHostNode<?> toDelete = _currentVirtualHostNode;
        final String storeDir = _currentStoreDir;

        _currentVirtualHostNode = null;
        _currentStoreDir = null;

        // delete VHN after the test method
        shared.doAsSystem("afterTestMethod:deleteVhn", () ->
        {
            toDelete.delete();
            return null;
        });

        if (Boolean.getBoolean("broker.clean.between.tests") && storeDir != null && !":memory:".equals(storeDir))
        {
            FileUtils.delete(new File(storeDir), true);
        }
    }

    @Override
    public InetSocketAddress getBrokerAddress(final PortType portType)
    {
        final Integer port = shared().ports.get(portType.name());
        if (port == null)
        {
            throw new IllegalStateException("Could not find port '" + portType.name() + "' on the Broker");
        }
        return InetSocketAddress.createUnresolved("localhost", port);
    }

    @Override
    public void createQueue(final String queueName)
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, queueName);
        attributes.put(ConfiguredObject.TYPE, "standard");

        final Queue<?> queue = _currentVirtualHostNode.getVirtualHost().createChild(Queue.class, attributes);
        final Exchange<?> exchange = _currentVirtualHostNode.getVirtualHost()
                .getChildByName(Exchange.class, "amq.direct");

        exchange.bind(queueName, queueName, Collections.emptyMap(), false);
    }

    @Override
    public void deleteQueue(final String queueName)
    {
        getQueue(queueName).delete();
    }

    @Override
    public void putMessageOnQueue(final String queueName, final String... messages)
    {
        for (String message : messages)
        {
            ((QueueManagingVirtualHost<?>) _currentVirtualHostNode.getVirtualHost())
                    .publishMessage(new EmbeddedMessage(queueName, message));
        }
    }

    @Override
    public int getQueueDepthMessages(final String queueName)
    {
        Queue<?> queue = _currentVirtualHostNode.getVirtualHost().getChildByName(Queue.class, queueName);
        return queue.getQueueDepthMessages();
    }

    @Override
    public boolean supportsRestart()
    {
        return _isPersistentStore;
    }

    @Override
    public CompletableFuture<Void> restart()
    {
        try
        {
            _currentVirtualHostNode.stop();
            _currentVirtualHostNode.start();
            return CompletableFuture.completedFuture(null);
        }
        catch (Exception e)
        {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override public boolean isAnonymousSupported()
    {
        return true;
    }

    @Override public boolean isSASLSupported()
    {
        return true;
    }

    @Override public boolean isSASLMechanismSupported(final String mechanismName)
    {
        return true;
    }

    @Override public boolean isWebSocketSupported()
    {
        return true;
    }

    @Override public boolean isQueueDepthSupported()
    {
        return true;
    }

    @Override public boolean isManagementSupported()
    {
        return true;
    }

    @Override public boolean isPutMessageOnQueueSupported()
    {
        return true;
    }

    @Override public boolean isDeleteQueueSupported()
    {
        return true;
    }

    @Override public String getValidUsername()
    {
        return "guest";
    }

    @Override public String getValidPassword()
    {
        return "guest";
    }

    @Override public String getKind()
    {
        return KIND_BROKER_J;
    }

    @Override public String getType()
    {
        return TYPE;
    }

    private Queue<?> getQueue(final String queueName)
    {
        return _currentVirtualHostNode.getVirtualHost().getChildren(Queue.class).stream()
                .filter(q -> q.getName().equals(queueName))
                .findFirst()
                .orElseThrow(() -> new NotFoundException("Queue '" + queueName + "' not found"));
    }

    private static SharedBroker shared()
    {
        synchronized (SHARED_LOCK)
        {
            if (SHARED == null)
            {
                throw new IllegalStateException("Shared broker is not started");
            }
            return SHARED;
        }
    }

    private static void acquireSharedBroker(final Map<String, String> requiredJvmProps,
                                            final Map<String, String> requiredBrokerContext)
    {
        synchronized (SHARED_LOCK)
        {
            if (SHARED == null)
            {
                SHARED = new SharedBroker(requiredJvmProps, requiredBrokerContext);
                SHARED.start();
            }
            else
            {
                // all test classes must have same global settings
                SHARED.verifyCompatible(requiredJvmProps, requiredBrokerContext);
            }
            SHARED.refCount.incrementAndGet();
        }
    }

    private static void releaseSharedBroker()
    {
        synchronized (SHARED_LOCK)
        {
            if (SHARED == null)
            {
                return;
            }
            if (SHARED.refCount.decrementAndGet() == 0)
            {
                try
                {
                    SHARED.stop();
                }
                finally
                {
                    SHARED = null;
                }
            }
        }
    }

    /**
     * Embedded message for publishMessage
     */
    private static final class EmbeddedMessage implements ManageableMessage
    {
        private final String _address;
        private final String _body;

        private EmbeddedMessage(final String address, final String body)
        {
            _address = address;
            _body = body;
        }

        @Override public String getAddress()
        {
            return _address;
        }

        @Override public boolean isPersistent()
        {
            return false;
        }

        @Override public Date getExpiration()
        {
            return null;
        }

        @Override public String getCorrelationId()
        {
            return null;
        }

        @Override public String getAppId()
        {
            return null;
        }

        @Override public String getMessageId()
        {
            return null;
        }

        @Override public String getMimeType()
        {
            return "text/plain";
        }

        @Override public String getEncoding()
        {
            return null;
        }

        @Override public int getPriority()
        {
            return 0;
        }

        @Override public Date getNotValidBefore()
        {
            return null;
        }

        @Override public String getReplyTo()
        {
            return null;
        }

        @Override public Map<String, Object> getHeaders()
        {
            return Map.of();
        }

        @Override public Object getContent()
        {
            return _body;
        }

        @Override public String getContentTransferEncoding()
        {
            return null;
        }
    }

    /**
     * Shared broker holder.
     */
    private static final class SharedBroker
    {
        private final AtomicInteger refCount = new AtomicInteger(0);
        private final Map<String, Integer> ports = new ConcurrentHashMap<>();

        private final Map<String, String> appliedJvmProps;
        private final Map<String, String> appliedBrokerContext;

        private Map<String, String> preservedJvmProps;

        private SystemLauncher systemLauncher;
        private Broker<?> broker;
        private String workDir;

        private SharedBroker(final Map<String, String> requiredJvmProps,
                             final Map<String, String> requiredBrokerContext)
        {
            this.appliedJvmProps = new LinkedHashMap<>(requiredJvmProps);
            this.appliedBrokerContext = new LinkedHashMap<>(requiredBrokerContext);
        }

        private void verifyCompatible(final Map<String, String> requiredJvmProps,
                                      final Map<String, String> requiredBrokerContext)
        {
            // JVM properties: should be consistent (otherwise parallel run will become indeterministic)
            for (var entry : requiredJvmProps.entrySet())
            {
                final String key = entry.getKey();
                final String val = entry.getValue();
                final String already = appliedJvmProps.get(key);
                if (already == null)
                {
                    throw new BrokerAdminException("ConfigItem(jvm=true) '" + key + "' is requested by another test class, " +
                            "but shared broker is already started. Make it consistent across suite or serialize such tests.");
                }
                if (!Objects.equals(already, val))
                {
                    throw new BrokerAdminException("Conflicting ConfigItem(jvm=true) for '" + key + "': '" + already + "' vs '" + val + "'");
                }
            }

            for (var entry : requiredBrokerContext.entrySet())
            {
                final String key = entry.getKey();
                final String val = entry.getValue();
                final String already = appliedBrokerContext.get(key);
                if (already == null)
                {
                    throw new BrokerAdminException("ConfigItem(jvm=false) '" + key + "' is requested by another test class, " +
                            "but shared broker is already started. Make it consistent across suite or serialize such tests.");
                }
                if (!Objects.equals(already, val))
                {
                    throw new BrokerAdminException("Conflicting ConfigItem(jvm=false) for '" + key + "': '" + already + "' vs '" + val + "'");
                }
            }
        }

        private void start()
        {
            try
            {
                // apply jvm properties once during the run (and restore them on stop)
                preservedJvmProps = new HashMap<>();
                for (var entry : appliedJvmProps.entrySet())
                {
                    preservedJvmProps.put(entry.getKey(), System.getProperty(entry.getKey()));
                    System.setProperty(entry.getKey(), entry.getValue());
                }

                final String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
                workDir = Files.createTempDirectory("qpid-work-run-" + timestamp + "-").toString();

                final Map<String, String> context = new HashMap<>();
                context.put("qpid.work_dir", workDir);
                context.put("qpid.port.protocol_handshake_timeout", "1000000");
                context.putAll(appliedBrokerContext);

                final Map<String, Object> systemConfigAttributes = new HashMap<>();
                systemConfigAttributes.put(ConfiguredObject.CONTEXT, context);
                systemConfigAttributes.put(ConfiguredObject.TYPE, System.getProperty("broker.config-store-type", "JSON"));
                systemConfigAttributes.put(SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, Boolean.FALSE);

                if (Thread.getDefaultUncaughtExceptionHandler() == null)
                {
                    Thread.setDefaultUncaughtExceptionHandler((t, e) ->
                            LOGGER.error("Uncaught exception in thread {}", t.getName(), e));
                }

                LOGGER.info("Starting shared internal broker (same JVM, per-run)");

                final List<SystemLauncherListener> listeners = new ArrayList<>();
                listeners.add(new LogbackLoggingSystemLauncherListener());
                listeners.add(new PortExtractingLauncherListener(this));

                systemLauncher = new SystemLauncher(listeners.toArray(SystemLauncherListener[]::new));
                systemLauncher.startup(systemConfigAttributes);
            }
            catch (Exception e)
            {
                throw new BrokerAdminException("Failed to start shared broker", e);
            }
        }

        private void stop()
        {
            try
            {
                if (systemLauncher != null)
                {
                    systemLauncher.shutdown();
                }
            }
            finally
            {
                ports.clear();

                if (Boolean.getBoolean("broker.clean.between.tests") && workDir != null)
                {
                    FileUtils.delete(new File(workDir), true);
                }

                // restore jvm properties
                if (preservedJvmProps != null)
                {
                    preservedJvmProps.forEach((k, v) -> {
                        if (v == null) System.clearProperty(k);
                        else System.setProperty(k, v);
                    });
                }

                systemLauncher = null;
                broker = null;
                workDir = null;
            }
        }

        private <T> T doAsSystem(final String taskName, final PrivilegedAction<T> action)
        {
            final Subject subject = new Subject(true,
                    new HashSet<>(Arrays.asList(systemLauncher.getSystemPrincipal(), new TaskPrincipal(taskName))),
                    Collections.emptySet(),
                    Collections.emptySet());

            return Subject.doAs(subject, action);
        }
    }

    /**
     * Listener, retrieving Broker and bound ports after the  startup
     */
    private static final class PortExtractingLauncherListener implements SystemLauncherListener
    {
        private final SharedBroker shared;
        private SystemConfig<?> systemConfig;

        private PortExtractingLauncherListener(final SharedBroker shared)
        {
            this.shared = shared;
        }

        @Override public void beforeStartup()
        {
            // no op
        }

        @Override public void errorOnStartup(final RuntimeException e)
        {
            // no op
        }

        @Override
        public void onContainerResolve(final SystemConfig<?> systemConfig)
        {
            this.systemConfig = systemConfig;
        }

        @Override
        public void afterStartup()
        {
            if (systemConfig == null)
            {
                throw new IllegalStateException("System config is required");
            }
            shared.broker = (Broker<?>) systemConfig.getContainer();
            shared.broker.getChildren(Port.class).forEach(p -> shared.ports.put(p.getName(), p.getBoundPort()));
        }

        @Override public void onContainerClose(final SystemConfig<?> systemConfig)
        {
            // no op
        }

        @Override public void onShutdown(final int exitCode)
        {
            // no op
        }

        @Override public void exceptionOnShutdown(final Exception e)
        {
            // no op
        }
    }
}

