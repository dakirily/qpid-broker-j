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
package org.apache.qpid.server.embedded;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.LogManager;

import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.PluggableFactoryLoader;
import org.apache.qpid.server.plugin.ProtocolEngineCreator;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.plugin.SystemConfigFactory;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;

/**
 * Runs broker characterization in a new JVM before {@link SystemLauncher} is initialized.
 */
public final class EmbeddedBrokerIsolationProbe
{
    private static final String SEQUENTIAL_MODE = "sequential";
    private static final String CONCURRENT_MODE = "concurrent";
    private static final String PROTOCOL_HANDLER_PROPERTY = "java.protocol.handler.pkgs";
    private static final long ACTIVE_SETTLE_MILLIS = 250L;
    private static final long CLOSED_SETTLE_MILLIS = 500L;

    private EmbeddedBrokerIsolationProbe()
    {
    }

    public static void main(final String[] arguments) throws Exception
    {
        if (arguments.length != 2)
        {
            throw new IllegalArgumentException("Expected probe mode and result file");
        }

        final String mode = arguments[0];
        final Map<String, Object> result;
        if (SEQUENTIAL_MODE.equals(mode))
        {
            result = runSequentialProbe();
        }
        else if (CONCURRENT_MODE.equals(mode))
        {
            result = runConcurrentProbe();
        }
        else
        {
            throw new IllegalArgumentException("Unknown probe mode: " + mode);
        }

        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(arguments[1]).toFile(), result);
    }

    private static Map<String, Object> runSequentialProbe() throws Exception
    {
        primeHostInfrastructure();
        final long initialHeapUsed = usedHeapAfterGc();
        final GlobalState initialState = GlobalState.capture();
        final Map<String, Object> first = runBroker("baseline-first", "baseline-first-vhost");
        final GlobalState afterFirstState = GlobalState.capture();
        final Map<String, Object> second = runBroker("baseline-second", "baseline-second-vhost");
        final long finalHeapUsed = usedHeapAfterGc();
        final GlobalState finalState = GlobalState.capture();

        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", SEQUENTIAL_MODE);
        result.put("environment", environment());
        result.put("initialHeapUsedBytes", initialHeapUsed);
        result.put("firstBroker", first);
        result.put("afterFirstBrokerGlobalDifference", initialState.difference(afterFirstState));
        result.put("secondBroker", second);
        result.put("finalGlobalDifference", initialState.difference(finalState));
        result.put("finalHeapUsedBytes", finalHeapUsed);
        result.put("totalRetainedHeapDeltaBytes", finalHeapUsed - initialHeapUsed);
        result.put("standaloneCompatibility", characterizeStandaloneCompatibility());
        return result;
    }

    private static Map<String, Object> runConcurrentProbe() throws Exception
    {
        primeHostInfrastructure();
        usedHeapAfterGc();
        final GlobalState initialState = GlobalState.capture();
        final EmbeddedQpidBroker first = EmbeddedQpidBroker.builder()
                .brokerName("concurrent-first")
                .virtualHost("concurrent-first-vhost")
                .build();
        final EmbeddedQpidBroker second = EmbeddedQpidBroker.builder()
                .brokerName("concurrent-second")
                .virtualHost("concurrent-second-vhost")
                .build();
        final AtomicInteger threadNumber = new AtomicInteger();
        final ThreadFactory threadFactory = runnable ->
        {
            final Thread thread = new Thread(runnable, "probe-control-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        final ExecutorService executor = Executors.newFixedThreadPool(2, threadFactory);

        GlobalState activeState = null;
        GlobalState afterFirstCloseState = null;
        int firstPort = -1;
        int secondPort = -1;
        boolean secondRunningAfterFirstClose = false;
        boolean secondAcceptsConnectionAfterFirstClose = false;
        Path firstWorkDirectory = null;
        Path secondWorkDirectory = null;
        try
        {
            final Future<EmbeddedQpidBroker> firstStart = executor.submit(first::start);
            final Future<EmbeddedQpidBroker> secondStart = executor.submit(second::start);
            firstStart.get(2L, TimeUnit.MINUTES);
            secondStart.get(2L, TimeUnit.MINUTES);

            firstPort = first.getAmqpAddress().getPort();
            secondPort = second.getAmqpAddress().getPort();
            firstWorkDirectory = first.getWorkDirectory();
            secondWorkDirectory = second.getWorkDirectory();
            Thread.sleep(ACTIVE_SETTLE_MILLIS);
            activeState = GlobalState.capture();

            first.close();
            secondRunningAfterFirstClose = second.isRunning();
            secondAcceptsConnectionAfterFirstClose = canConnect(secondPort);
            Thread.sleep(ACTIVE_SETTLE_MILLIS);
            afterFirstCloseState = GlobalState.capture();
            second.close();
        }
        finally
        {
            first.close();
            second.close();
            executor.shutdownNow();
            executor.awaitTermination(30L, TimeUnit.SECONDS);
        }

        Thread.sleep(CLOSED_SETTLE_MILLIS);
        usedHeapAfterGc();
        final GlobalState finalState = GlobalState.capture();
        final List<String> runtimeMarkers =
                List.of("concurrent-first", "concurrent-first-vhost",
                        "concurrent-second", "concurrent-second-vhost");
        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", CONCURRENT_MODE);
        result.put("environment", environment());
        result.put("firstPort", firstPort);
        result.put("secondPort", secondPort);
        result.put("portsAreDistinct", firstPort > 0 && secondPort > 0 && firstPort != secondPort);
        result.put("secondBrokerRunningAfterFirstClose", secondRunningAfterFirstClose);
        result.put("secondBrokerAcceptsConnectionAfterFirstClose", secondAcceptsConnectionAfterFirstClose);
        result.put("firstWorkDirectoryRemoved",
                   firstWorkDirectory != null && !Files.exists(firstWorkDirectory));
        result.put("secondWorkDirectoryRemoved",
                   secondWorkDirectory != null && !Files.exists(secondWorkDirectory));
        result.put("activeGlobalDifference", initialState.difference(activeState));
        result.put("afterFirstCloseGlobalDifference", initialState.difference(afterFirstCloseState));
        result.put("finalGlobalDifference", initialState.difference(finalState));
        result.put("activeRuntimeAttributedThreads",
                   attributedThreads(initialState, activeState, runtimeMarkers, true));
        result.put("activeUnattributedThreads",
                   attributedThreads(initialState, activeState, runtimeMarkers, false));
        result.put("closedRuntimeAttributedThreads",
                   attributedThreads(initialState, finalState, runtimeMarkers, true));
        return result;
    }

    private static boolean canConnect(final int port)
    {
        try (Socket socket = new Socket())
        {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 5000);
            return true;
        }
        catch (final IOException ignore)
        {
            // Connection failure is reported through the probe result
            return false;
        }
    }

    private static Map<String, Object> runBroker(final String brokerName,
                                                  final String virtualHostName) throws Exception
    {
        final long heapBeforeStart = usedHeapAfterGc();
        final GlobalState beforeState = GlobalState.capture();
        final EmbeddedQpidBroker broker = EmbeddedQpidBroker.builder()
                .brokerName(brokerName)
                .virtualHost(virtualHostName)
                .build();

        final long startedAt = System.nanoTime();
        broker.start();
        final long startupMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
        final Path workDirectory = broker.getWorkDirectory();
        final int port = broker.getAmqpAddress().getPort();
        final boolean workDirectoryExistsWhileActive = Files.isDirectory(workDirectory);
        Thread.sleep(ACTIVE_SETTLE_MILLIS);
        final GlobalState activeState = GlobalState.capture();
        final long activeHeapUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();

        broker.close();
        Thread.sleep(CLOSED_SETTLE_MILLIS);
        final long heapAfterClose = usedHeapAfterGc();
        final GlobalState closedState = GlobalState.capture();
        final List<String> runtimeMarkers = List.of(brokerName, virtualHostName);

        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("brokerName", brokerName);
        result.put("virtualHostName", virtualHostName);
        result.put("startupMillis", startupMillis);
        result.put("port", port);
        result.put("workDirectoryExistsWhileActive", workDirectoryExistsWhileActive);
        result.put("workDirectoryRemovedAfterClose", !Files.exists(workDirectory));
        result.put("heapBeforeStartBytes", heapBeforeStart);
        result.put("activeHeapUsedBytes", activeHeapUsed);
        result.put("heapAfterCloseBytes", heapAfterClose);
        result.put("retainedHeapDeltaBytes", heapAfterClose - heapBeforeStart);
        result.put("activeGlobalDifference", beforeState.difference(activeState));
        result.put("closedGlobalDifference", beforeState.difference(closedState));
        result.put("activeRuntimeAttributedThreads",
                   attributedThreads(beforeState, activeState, runtimeMarkers, true));
        result.put("activeUnattributedThreads",
                   attributedThreads(beforeState, activeState, runtimeMarkers, false));
        result.put("closedRuntimeAttributedThreads",
                   attributedThreads(beforeState, closedState, runtimeMarkers, true));
        return result;
    }

    private static Map<String, Object> characterizeStandaloneCompatibility() throws Exception
    {
        final Map<String, Object> result = new LinkedHashMap<>();
        final Set<String> protocolVersions = new TreeSet<>();
        for (final ProtocolEngineCreator creator : new QpidServiceLoader().instancesOf(ProtocolEngineCreator.class))
        {
            protocolVersions.add(creator.getVersion().name());
        }
        result.put("protocolVersions", protocolVersions);
        result.put("systemConfigTypes",
                   new PluggableFactoryLoader<>(SystemConfigFactory.class).getSupportedTypes());

        final Map<String, Object> resourceDefaults = new LinkedHashMap<>();
        resourceDefaults.put("availableProcessors", Runtime.getRuntime().availableProcessors());
        resourceDefaults.put("portWorkerThreads", AmqpPort.DEFAULT_PORT_AMQP_THREAD_POOL_SIZE);
        resourceDefaults.put("portSelectorThreads", AmqpPort.DEFAULT_PORT_AMQP_NUMBER_OF_SELECTORS);
        resourceDefaults.put("brokerHousekeepingThreads", Broker.DEFAULT_HOUSEKEEPING_THREAD_COUNT);
        resourceDefaults.put("virtualHostWorkerThreads",
                             QueueManagingVirtualHost.DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE);
        resourceDefaults.put("virtualHostSelectorThreads",
                             QueueManagingVirtualHost.DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS);
        resourceDefaults.put("virtualHostHousekeepingThreads",
                             QueueManagingVirtualHost.DEFAULT_HOUSEKEEPING_THREAD_COUNT);
        result.put("resourceDefaults", resourceDefaults);
        result.put("propertyPrecedence", characterizePropertyPrecedence());

        final ExitCodeListener requestedExitListener = new ExitCodeListener();
        new SystemLauncher(requestedExitListener).shutdown(17);
        result.put("requestedShutdownExitCode", requestedExitListener.getExitCode());

        final ExitCodeListener failedStartupListener = new ExitCodeListener();
        final SystemLauncher failedLauncher = new SystemLauncher(failedStartupListener);
        failedLauncher.startup(Map.of(SystemConfig.TYPE, "missing-baseline-system-config-type"));
        result.put("failedStartupReported", failedStartupListener.isStartupFailureReported());
        result.put("failedStartupExitCode", failedStartupListener.getExitCode());
        return result;
    }

    private static Map<String, Object> characterizePropertyPrecedence() throws Exception
    {
        final String existingProperty = "qpid.embedded.baseline.existing";
        final String missingProperty = "qpid.embedded.baseline.missing";
        final String previousExisting = System.getProperty(existingProperty);
        final String previousMissing = System.getProperty(missingProperty);
        final Path propertiesFile = Files.createTempFile("qpid-embedded-baseline-", ".properties");
        try
        {
            System.setProperty(existingProperty, "host-value");
            System.clearProperty(missingProperty);
            Files.writeString(propertiesFile,
                              existingProperty + "=file-value\n" + missingProperty + "=file-value\n",
                              StandardCharsets.UTF_8);
            SystemLauncher.populateSystemPropertiesFromDefaults(propertiesFile.toString());

            final Map<String, Object> result = new LinkedHashMap<>();
            result.put("existingPropertyValue", System.getProperty(existingProperty));
            result.put("missingPropertyValue", System.getProperty(missingProperty));
            return result;
        }
        finally
        {
            restoreProperty(existingProperty, previousExisting);
            restoreProperty(missingProperty, previousMissing);
            Files.deleteIfExists(propertiesFile);
        }
    }

    private static void restoreProperty(final String name, final String value)
    {
        if (value == null)
        {
            System.clearProperty(name);
        }
        else
        {
            System.setProperty(name, value);
        }
    }

    private static Map<String, Object> environment()
    {
        final Map<String, Object> environment = new LinkedHashMap<>();
        environment.put("capturedAt", Instant.now().toString());
        environment.put("javaVersion", System.getProperty("java.version"));
        environment.put("javaVendor", System.getProperty("java.vendor"));
        environment.put("osName", System.getProperty("os.name"));
        environment.put("osVersion", System.getProperty("os.version"));
        environment.put("availableProcessors", Runtime.getRuntime().availableProcessors());
        environment.put("maximumHeapBytes", Runtime.getRuntime().maxMemory());
        return environment;
    }

    private static long usedHeapAfterGc() throws InterruptedException
    {
        System.gc();
        Thread.sleep(100L);
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    private static void primeHostInfrastructure()
    {
        Locale.getDefault();
        TimeZone.getDefault();
        LogManager.getLogManager();
        LoggerFactory.getLogger(EmbeddedBrokerIsolationProbe.class)
                .info("Host logging initialized before embedded broker baseline");
    }

    private static List<String> attributedThreads(final GlobalState before,
                                                   final GlobalState after,
                                                   final List<String> runtimeMarkers,
                                                   final boolean attributed)
    {
        final List<String> matching = new ArrayList<>();
        for (final String thread : before.addedThreadDescriptions(after))
        {
            boolean containsRuntimeMarker = false;
            for (final String marker : runtimeMarkers)
            {
                if (thread.contains(marker))
                {
                    containsRuntimeMarker = true;
                    break;
                }
            }
            if (containsRuntimeMarker == attributed && !thread.contains("probe-control-"))
            {
                matching.add(thread);
            }
        }
        Collections.sort(matching);
        return matching;
    }

    private static final class GlobalState
    {
        private final Map<String, String> _properties;
        private final String _protocolHandlerPackages;
        private final String _defaultUncaughtExceptionHandler;
        private final String _contextClassLoader;
        private final String _rootLoggingConfiguration;
        private final List<String> _shutdownHooks;
        private final Map<Long, String> _threads;

        private GlobalState(final Map<String, String> properties,
                            final String protocolHandlerPackages,
                            final String defaultUncaughtExceptionHandler,
                            final String contextClassLoader,
                            final String rootLoggingConfiguration,
                            final List<String> shutdownHooks,
                            final Map<Long, String> threads)
        {
            _properties = properties;
            _protocolHandlerPackages = protocolHandlerPackages;
            _defaultUncaughtExceptionHandler = defaultUncaughtExceptionHandler;
            _contextClassLoader = contextClassLoader;
            _rootLoggingConfiguration = rootLoggingConfiguration;
            _shutdownHooks = shutdownHooks;
            _threads = threads;
        }

        private static GlobalState capture()
        {
            final Properties systemProperties = System.getProperties();
            final Map<String, String> properties = new TreeMap<>();
            for (final String name : systemProperties.stringPropertyNames())
            {
                properties.put(name, systemProperties.getProperty(name));
            }

            final Map<Long, String> threads = new TreeMap<>();
            for (final Thread thread : Thread.getAllStackTraces().keySet())
            {
                threads.put(thread.getId(),
                            String.format("%d:%s[state=%s,daemon=%s]",
                                          thread.getId(), thread.getName(), thread.getState(), thread.isDaemon()));
            }
            return new GlobalState(properties,
                                   System.getProperty(PROTOCOL_HANDLER_PROPERTY),
                                   describeObject(Thread.getDefaultUncaughtExceptionHandler()),
                                   describeObject(Thread.currentThread().getContextClassLoader()),
                                   describeRootLoggingConfiguration(),
                                   captureShutdownHooks(),
                                   threads);
        }

        private Map<String, Object> difference(final GlobalState current)
        {
            final Map<String, Object> difference = new LinkedHashMap<>();
            difference.put("propertyChanges", propertyDifference(current));
            difference.put("protocolHandlerPackagesChanged",
                           !Objects.equals(_protocolHandlerPackages, current._protocolHandlerPackages));
            difference.put("protocolHandlerPackagesBefore", _protocolHandlerPackages);
            difference.put("protocolHandlerPackagesAfter", current._protocolHandlerPackages);
            difference.put("defaultUncaughtExceptionHandlerChanged",
                           !_defaultUncaughtExceptionHandler.equals(current._defaultUncaughtExceptionHandler));
            difference.put("contextClassLoaderChanged",
                           !_contextClassLoader.equals(current._contextClassLoader));
            difference.put("rootLoggingConfigurationChanged",
                           !_rootLoggingConfiguration.equals(current._rootLoggingConfiguration));
            difference.put("shutdownHooksAdded", listDifference(current._shutdownHooks, _shutdownHooks));
            difference.put("shutdownHooksRemoved", listDifference(_shutdownHooks, current._shutdownHooks));
            difference.put("threadsAdded", addedThreadDescriptions(current));
            difference.put("threadsRemoved", mapValueDifference(_threads, current._threads));
            return difference;
        }

        private Map<String, Object> propertyDifference(final GlobalState current)
        {
            final List<String> added = new ArrayList<>();
            final List<String> removed = new ArrayList<>();
            final List<String> changed = new ArrayList<>();
            for (final Map.Entry<String, String> entry : current._properties.entrySet())
            {
                if (!_properties.containsKey(entry.getKey()))
                {
                    added.add(entry.getKey());
                }
                else if (!_properties.get(entry.getKey()).equals(entry.getValue()))
                {
                    changed.add(entry.getKey());
                }
            }
            for (final String name : _properties.keySet())
            {
                if (!current._properties.containsKey(name))
                {
                    removed.add(name);
                }
            }

            final Map<String, Object> difference = new LinkedHashMap<>();
            difference.put("added", added);
            difference.put("removed", removed);
            difference.put("changed", changed);
            return difference;
        }

        private List<String> addedThreadDescriptions(final GlobalState current)
        {
            return mapValueDifference(current._threads, _threads);
        }

        private static List<String> mapValueDifference(final Map<Long, String> values,
                                                       final Map<Long, String> valuesToRemove)
        {
            final List<String> difference = new ArrayList<>();
            for (final Map.Entry<Long, String> entry : values.entrySet())
            {
                if (!valuesToRemove.containsKey(entry.getKey()))
                {
                    difference.add(entry.getValue());
                }
            }
            Collections.sort(difference);
            return difference;
        }

        private static List<String> listDifference(final List<String> values,
                                                   final List<String> valuesToRemove)
        {
            final List<String> difference = new ArrayList<>(values);
            for (final String value : valuesToRemove)
            {
                difference.remove(value);
            }
            Collections.sort(difference);
            return difference;
        }

        private static List<String> captureShutdownHooks()
        {
            try
            {
                final Class<?> hooksClass = Class.forName("java.lang.ApplicationShutdownHooks");
                final Field hooksField = hooksClass.getDeclaredField("hooks");
                hooksField.setAccessible(true);
                @SuppressWarnings("unchecked")
                final Map<Thread, Thread> hooks = (Map<Thread, Thread>) hooksField.get(null);
                final List<String> names = new ArrayList<>();
                if (hooks != null)
                {
                    synchronized (hooks)
                    {
                        for (final Thread hook : hooks.keySet())
                        {
                            names.add(hook.getName());
                        }
                    }
                }
                Collections.sort(names);
                return names;
            }
            catch (final ReflectiveOperationException | RuntimeException e)
            {
                return List.of("<unavailable:" + e.getClass().getSimpleName() + ">");
            }
        }

        private static String describeRootLoggingConfiguration()
        {
            final ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
            final StringBuilder description = new StringBuilder(describeObject(loggerFactory));
            try
            {
                final Method getLogger = loggerFactory.getClass().getMethod("getLogger", String.class);
                final Object rootLogger = getLogger.invoke(loggerFactory, "ROOT");
                description.append("|root=").append(describeObject(rootLogger));

                final Method getLevel = rootLogger.getClass().getMethod("getLevel");
                description.append("|level=").append(getLevel.invoke(rootLogger));

                final Method iteratorForAppenders = rootLogger.getClass().getMethod("iteratorForAppenders");
                @SuppressWarnings("unchecked")
                final Iterator<Object> appenders = (Iterator<Object>) iteratorForAppenders.invoke(rootLogger);
                final List<String> appenderDescriptions = new ArrayList<>();
                while (appenders.hasNext())
                {
                    appenderDescriptions.add(describeObject(appenders.next()));
                }
                Collections.sort(appenderDescriptions);
                description.append("|appenders=").append(appenderDescriptions);

                final Method getTurboFilterList = loggerFactory.getClass().getMethod("getTurboFilterList");
                @SuppressWarnings("unchecked")
                final List<Object> filters = (List<Object>) getTurboFilterList.invoke(loggerFactory);
                final List<String> filterDescriptions = new ArrayList<>();
                for (final Object filter : filters)
                {
                    filterDescriptions.add(describeObject(filter));
                }
                Collections.sort(filterDescriptions);
                description.append("|filters=").append(filterDescriptions);
            }
            catch (final ReflectiveOperationException | RuntimeException e)
            {
                description.append("|introspection=").append(e.getClass().getSimpleName());
            }
            return description.toString();
        }

        private static String describeObject(final Object value)
        {
            return value == null
                    ? "<null>"
                    : value.getClass().getName() + "@" + System.identityHashCode(value);
        }
    }

    private static final class ExitCodeListener extends SystemLauncherListener.DefaultSystemLauncherListener
    {
        private int _exitCode = Integer.MIN_VALUE;
        private boolean _startupFailureReported;

        @Override
        public void errorOnStartup(final RuntimeException exception)
        {
            _startupFailureReported = true;
        }

        @Override
        public void onShutdown(final int exitCode)
        {
            _exitCode = exitCode;
        }

        private int getExitCode()
        {
            return _exitCode;
        }

        private boolean isStartupFailureReported()
        {
            return _startupFailureReported;
        }
    }
}
