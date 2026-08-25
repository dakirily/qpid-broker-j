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

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.User;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.security.SubjectExecutionContext;
import org.apache.qpid.server.security.auth.TaskPrincipal;
import org.apache.qpid.server.store.MemorySystemConfigImpl;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhostnode.memory.MemoryVirtualHostNode;

/**
 * Starts and manages a memory-backed Broker-J instance in the current JVM.
 *
 * <p>The broker listens on the IPv4 loopback interface and uses a dynamically allocated port by default. The AMQP
 * port is created only after the configured virtual host, exchanges, queues, and bindings are ready.</p>
 */
public final class EmbeddedQpidBroker implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedQpidBroker.class);

    private static final String DEFAULT_BROKER_NAME = "embedded";
    private static final String DEFAULT_VIRTUAL_HOST_NAME = "test";
    private static final String DEFAULT_USERNAME = "guest";
    private static final String DEFAULT_PASSWORD = "guest";
    private static final String LOOPBACK_ADDRESS = "127.0.0.1";
    private static final String AUTHENTICATION_PROVIDER_NAME = "embeddedPlain";
    private static final String AMQP_PORT_NAME = "AMQP";
    private static final String VIRTUAL_HOST_BLUEPRINT = "{\"type\":\"ProvidedStore\"}";

    private final Configuration _configuration;

    private State _state = State.NEW;
    private SystemLauncher _systemLauncher;
    private Path _workDirectory;
    private InetSocketAddress _amqpAddress;

    EmbeddedQpidBroker(final Configuration configuration)
    {
        _configuration = configuration;
    }

    /**
     * Creates a builder with an AMQP 1.0 port, a memory store, and {@code guest/guest} credentials.
     *
     * @return embedded broker builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Starts the broker and applies its topology.
     *
     * @return this broker
     */
    public synchronized EmbeddedQpidBroker start()
    {
        if (_state != State.NEW)
        {
            throw new IllegalStateException("Embedded broker can only be started once");
        }

        _state = State.STARTING;
        try
        {
            final Path initialConfiguration = createInitialConfiguration();
            final CapturingSystemLauncherListener listener = new CapturingSystemLauncherListener();
            _systemLauncher = new SystemLauncher(listener);
            _systemLauncher.startup(createSystemConfigAttributes(initialConfiguration));
            listener.throwIfStartupFailed();

            final Broker<?> broker = listener.getBroker();
            final Port<?> port = configureBroker(broker);
            final int boundPort = port.getBoundPort();
            if (boundPort <= 0)
            {
                throw new EmbeddedBrokerException("Embedded AMQP port did not bind successfully");
            }

            _amqpAddress = InetSocketAddress.createUnresolved(LOOPBACK_ADDRESS, boundPort);
            _state = State.STARTED;
            return this;
        }
        catch (final Exception e)
        {
            cleanUpAfterFailedStart(e);
            if (e instanceof EmbeddedBrokerException)
            {
                throw (EmbeddedBrokerException) e;
            }
            throw new EmbeddedBrokerException("Failed to start embedded Broker-J instance", e);
        }
    }

    /**
     * Returns whether the broker has started and has not yet been closed.
     *
     * @return {@code true} when the broker is running
     */
    public synchronized boolean isRunning()
    {
        return _state == State.STARTED;
    }

    /**
     * Returns the loopback AMQP socket address selected during startup.
     *
     * @return AMQP socket address
     */
    public synchronized InetSocketAddress getAmqpAddress()
    {
        ensureStarted();
        return _amqpAddress;
    }

    /**
     * Returns the base AMQP URI. Credentials and client-specific virtual-host options are deliberately omitted.
     *
     * @return base AMQP URI
     */
    public synchronized URI getAmqpUri()
    {
        ensureStarted();
        try
        {
            return new URI("amqp", null, _amqpAddress.getHostString(), _amqpAddress.getPort(), null, null, null);
        }
        catch (final URISyntaxException e)
        {
            throw new EmbeddedBrokerException("Failed to construct embedded broker URI", e);
        }
    }

    /**
     * Returns the configured virtual-host name.
     *
     * @return virtual-host name
     */
    public String getVirtualHostName()
    {
        return _configuration._virtualHostName;
    }

    /**
     * Returns the configured AMQP username.
     *
     * @return username
     */
    public String getUsername()
    {
        return _configuration._username;
    }

    /**
     * Returns the configured AMQP password.
     *
     * @return password
     */
    public String getPassword()
    {
        return _configuration._password;
    }

    /**
     * Returns the temporary work directory after startup has begun. The path remains available after close for
     * diagnostics, although the directory itself is normally deleted.
     *
     * @return broker work directory
     */
    public synchronized Path getWorkDirectory()
    {
        if (_workDirectory == null)
        {
            throw new IllegalStateException("Embedded broker has not been started");
        }
        return _workDirectory;
    }

    /**
     * Stops the broker and deletes its temporary work directory. This method is idempotent.
     */
    @Override
    public synchronized void close()
    {
        if (_state == State.CLOSED)
        {
            return;
        }

        RuntimeException shutdownFailure = null;
        try
        {
            if (_systemLauncher != null)
            {
                _systemLauncher.shutdown();
            }
        }
        catch (final RuntimeException e)
        {
            shutdownFailure = e;
        }
        finally
        {
            deleteWorkDirectory();
            _systemLauncher = null;
            _state = State.CLOSED;
        }

        if (shutdownFailure != null)
        {
            throw new EmbeddedBrokerException("Failed to stop embedded Broker-J instance", shutdownFailure);
        }
    }

    private Path createInitialConfiguration() throws Exception
    {
        _workDirectory = Files.createTempDirectory("qpid-embedded-");
        final Path initialConfiguration = _workDirectory.resolve("initial-config.json");
        final Map<String, Object> attributes = Map.of(ConfiguredObject.NAME, _configuration._brokerName,
                                                      Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        new ObjectMapper().writeValue(initialConfiguration.toFile(), attributes);
        return initialConfiguration;
    }

    private Map<String, Object> createSystemConfigAttributes(final Path initialConfiguration)
    {
        final Map<String, String> context =
                Map.of(SystemConfig.QPID_WORK_DIR, _workDirectory.toAbsolutePath().toString());
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.TYPE, MemorySystemConfigImpl.SYSTEM_CONFIG_TYPE);
        attributes.put(ConfiguredObject.CONTEXT, context);
        attributes.put(SystemConfig.INITIAL_CONFIGURATION_LOCATION, initialConfiguration.toAbsolutePath().toString());
        attributes.put(SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, Boolean.FALSE);
        return attributes;
    }

    private Port<?> configureBroker(final Broker<?> broker)
    {
        final Principal systemPrincipal = _systemLauncher.getSystemPrincipal();
        final Principal taskPrincipal = new TaskPrincipal("EmbeddedQpidBroker");
        final Set<Principal> principals = Set.of(systemPrincipal, taskPrincipal);
        final Subject subject = new Subject(true, principals, Set.of(), Set.of());
        return SubjectExecutionContext.withSubjectUnchecked(subject, () -> configureBrokerWithSystemRights(broker));
    }

    private Port<?> configureBrokerWithSystemRights(final Broker<?> broker)
    {
        final AuthenticationProvider<?> authenticationProvider = createAuthenticationProvider(broker);
        final VirtualHost<?> virtualHost = createVirtualHost(broker);
        createTopology(virtualHost);
        return createPort(broker, authenticationProvider);
    }

    private AuthenticationProvider<?> createAuthenticationProvider(final Broker<?> broker)
    {
        final Map<String, Object> providerAttributes = new HashMap<>();
        providerAttributes.put(ConfiguredObject.NAME, AUTHENTICATION_PROVIDER_NAME);
        providerAttributes.put(ConfiguredObject.TYPE, "Plain");
        providerAttributes.put("secureOnlyMechanisms", List.of());
        final AuthenticationProvider<?> provider =
                broker.createChild(AuthenticationProvider.class, providerAttributes);

        final Map<String, Object> userAttributes = new HashMap<>();
        userAttributes.put(ConfiguredObject.NAME, _configuration._username);
        userAttributes.put(ConfiguredObject.TYPE, "managed");
        userAttributes.put(User.PASSWORD, _configuration._password);
        provider.createChild(User.class, userAttributes);
        return provider;
    }

    private VirtualHost<?> createVirtualHost(final Broker<?> broker)
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, _configuration._virtualHostName);
        attributes.put(ConfiguredObject.TYPE, MemoryVirtualHostNode.VIRTUAL_HOST_NODE_TYPE);
        attributes.put(ConfiguredObject.CONTEXT, Map.of("virtualhostBlueprint", VIRTUAL_HOST_BLUEPRINT));
        attributes.put(VirtualHostNode.DEFAULT_VIRTUAL_HOST_NODE, Boolean.TRUE);
        attributes.put(VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION, VIRTUAL_HOST_BLUEPRINT);
        final VirtualHostNode<?> node = broker.createChild(VirtualHostNode.class, attributes);
        final VirtualHost<?> virtualHost = node.getVirtualHost();
        if (virtualHost == null)
        {
            throw new EmbeddedBrokerException("Embedded virtual host was not created");
        }
        return virtualHost;
    }

    private void createTopology(final VirtualHost<?> virtualHost)
    {
        for (final ExchangeDefinition definition : _configuration._exchanges)
        {
            final Map<String, Object> attributes = new HashMap<>(definition._attributes);
            attributes.put(ConfiguredObject.NAME, definition._name);
            attributes.put(ConfiguredObject.TYPE, definition._type.getType());
            virtualHost.createChild(Exchange.class, attributes);
        }

        for (final QueueDefinition definition : _configuration._queues)
        {
            final Map<String, Object> attributes = new HashMap<>(definition._attributes);
            attributes.put(ConfiguredObject.NAME, definition._name);
            attributes.putIfAbsent(ConfiguredObject.TYPE, "standard");
            virtualHost.createChild(Queue.class, attributes);
        }

        for (final BindingDefinition definition : _configuration._bindings)
        {
            final Exchange<?> exchange = virtualHost.getChildByName(Exchange.class, definition._exchangeName);
            if (exchange == null)
            {
                throw new EmbeddedBrokerException("Exchange '" + definition._exchangeName + "' does not exist");
            }

            final Queue<?> queue = virtualHost.getChildByName(Queue.class, definition._queueName);
            if (queue == null)
            {
                throw new EmbeddedBrokerException("Queue '" + definition._queueName + "' does not exist");
            }

            if (!exchange.bind(queue.getName(), definition._bindingKey, definition._arguments, false))
            {
                throw new EmbeddedBrokerException(
                        String.format("Binding from exchange '%s' to queue '%s' already exists",
                                      definition._exchangeName, definition._queueName));
            }
        }
    }

    private Port<?> createPort(final Broker<?> broker, final AuthenticationProvider<?> authenticationProvider)
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, AMQP_PORT_NAME);
        attributes.put(ConfiguredObject.TYPE, "AMQP");
        attributes.put(Port.PORT, _configuration._port);
        attributes.put(Port.BINDING_ADDRESS, LOOPBACK_ADDRESS);
        attributes.put(Port.AUTHENTICATION_PROVIDER, authenticationProvider);
        attributes.put(Port.PROTOCOLS, _configuration._protocols);
        return broker.createChild(Port.class, attributes);
    }

    private void cleanUpAfterFailedStart(final Exception startupFailure)
    {
        try
        {
            if (_systemLauncher != null)
            {
                _systemLauncher.shutdown();
            }
        }
        catch (final RuntimeException cleanupFailure)
        {
            startupFailure.addSuppressed(cleanupFailure);
        }
        finally
        {
            deleteWorkDirectory();
            _systemLauncher = null;
            _state = State.CLOSED;
        }
    }

    private void deleteWorkDirectory()
    {
        if (_workDirectory != null && Files.exists(_workDirectory) &&
                !FileUtils.delete(_workDirectory.toFile(), true))
        {
            LOGGER.warn("Could not delete embedded broker work directory '{}'", _workDirectory);
        }
    }

    private void ensureStarted()
    {
        if (_state != State.STARTED)
        {
            throw new IllegalStateException("Embedded broker is not running");
        }
    }

    private static String requireNonBlank(final String value, final String description)
    {
        Objects.requireNonNull(value, description + " must not be null");
        if (value.isBlank())
        {
            throw new IllegalArgumentException(description + " must not be blank");
        }
        return value;
    }

    private static Map<String, Object> copyAttributes(final Map<String, Object> attributes)
    {
        Objects.requireNonNull(attributes, "Attributes must not be null");
        return Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
    }

    /**
     * Builder for an embedded broker and its initial topology.
     */
    public static final class Builder
    {
        private String _brokerName = DEFAULT_BROKER_NAME;
        private String _virtualHostName = DEFAULT_VIRTUAL_HOST_NAME;
        private String _username = DEFAULT_USERNAME;
        private String _password = DEFAULT_PASSWORD;
        private int _port;
        private final Set<AmqpProtocol> _protocols = EnumSet.of(AmqpProtocol.AMQP_1_0);
        private final Map<String, QueueDefinition> _queues = new LinkedHashMap<>();
        private final Map<String, ExchangeDefinition> _exchanges = new LinkedHashMap<>();
        private final List<BindingDefinition> _bindings = new ArrayList<>();

        private Builder()
        {
        }

        /**
         * Sets the broker name.
         *
         * @param brokerName broker name
         * @return this builder
         */
        public Builder brokerName(final String brokerName)
        {
            _brokerName = requireNonBlank(brokerName, "Broker name");
            return this;
        }

        /**
         * Sets the name of the default memory-backed virtual host.
         *
         * @param virtualHostName virtual-host name
         * @return this builder
         */
        public Builder virtualHost(final String virtualHostName)
        {
            _virtualHostName = requireNonBlank(virtualHostName, "Virtual-host name");
            return this;
        }

        /**
         * Sets the credentials accepted by the broker's plain authentication provider.
         *
         * @param username username
         * @param password password, which may be empty
         * @return this builder
         */
        public Builder credentials(final String username, final String password)
        {
            _username = requireNonBlank(username, "Username");
            _password = Objects.requireNonNull(password, "Password must not be null");
            return this;
        }

        /**
         * Selects a fixed AMQP port. A value of zero, the default, requests a dynamically allocated port.
         *
         * @param port configured port
         * @return this builder
         */
        public Builder port(final int port)
        {
            if (port < 0 || port > 65535)
            {
                throw new IllegalArgumentException("Port must be between 0 and 65535");
            }
            _port = port;
            return this;
        }

        /**
         * Replaces the set of AMQP protocols enabled on the port.
         *
         * @param protocols one or more protocols
         * @return this builder
         */
        public Builder protocols(final AmqpProtocol... protocols)
        {
            Objects.requireNonNull(protocols, "Protocols must not be null");
            if (protocols.length == 0)
            {
                throw new IllegalArgumentException("At least one AMQP protocol is required");
            }
            _protocols.clear();
            for (final AmqpProtocol protocol : protocols)
            {
                _protocols.add(Objects.requireNonNull(protocol, "Protocol must not be null"));
            }
            return this;
        }

        /**
         * Adds a standard queue with default attributes.
         *
         * @param name queue name
         * @return this builder
         */
        public Builder queue(final String name)
        {
            return queue(name, Map.of());
        }

        /**
         * Adds a queue. The optional attributes use Broker-J configured-object attribute names; the queue name is
         * supplied separately and the type defaults to {@code standard}.
         *
         * @param name queue name
         * @param attributes additional queue attributes
         * @return this builder
         */
        public Builder queue(final String name, final Map<String, Object> attributes)
        {
            final String validatedName = requireNonBlank(name, "Queue name");
            if (_queues.containsKey(validatedName))
            {
                throw new IllegalArgumentException("Queue '" + validatedName + "' is already configured");
            }
            _queues.put(validatedName, new QueueDefinition(validatedName, copyAttributes(attributes)));
            return this;
        }

        /**
         * Adds an exchange with default attributes.
         *
         * @param name exchange name
         * @param type exchange type
         * @return this builder
         */
        public Builder exchange(final String name, final ExchangeType type)
        {
            return exchange(name, type, Map.of());
        }

        /**
         * Adds an exchange.
         *
         * @param name exchange name
         * @param type exchange type
         * @param attributes additional exchange attributes
         * @return this builder
         */
        public Builder exchange(final String name,
                                final ExchangeType type,
                                final Map<String, Object> attributes)
        {
            final String validatedName = requireNonBlank(name, "Exchange name");
            if (_exchanges.containsKey(validatedName))
            {
                throw new IllegalArgumentException("Exchange '" + validatedName + "' is already configured");
            }
            _exchanges.put(validatedName,
                           new ExchangeDefinition(validatedName, Objects.requireNonNull(type, "Type must not be null"),
                                                  copyAttributes(attributes)));
            return this;
        }

        /**
         * Adds an exchange-to-queue binding without arguments.
         *
         * @param exchangeName source exchange name
         * @param queueName destination queue name
         * @param bindingKey binding key, which may be empty
         * @return this builder
         */
        public Builder binding(final String exchangeName, final String queueName, final String bindingKey)
        {
            return binding(exchangeName, queueName, bindingKey, Map.of());
        }

        /**
         * Adds an exchange-to-queue binding.
         *
         * @param exchangeName source exchange name
         * @param queueName destination queue name
         * @param bindingKey binding key, which may be empty
         * @param arguments binding arguments
         * @return this builder
         */
        public Builder binding(final String exchangeName,
                               final String queueName,
                               final String bindingKey,
                               final Map<String, Object> arguments)
        {
            _bindings.add(new BindingDefinition(requireNonBlank(exchangeName, "Exchange name"),
                                                requireNonBlank(queueName, "Queue name"),
                                                Objects.requireNonNull(bindingKey, "Binding key must not be null"),
                                                copyAttributes(arguments)));
            return this;
        }

        /**
         * Creates an unstarted embedded broker.
         *
         * @return embedded broker
         */
        public EmbeddedQpidBroker build()
        {
            return new EmbeddedQpidBroker(buildConfiguration());
        }

        Configuration buildConfiguration()
        {
            return new Configuration(this);
        }
    }

    static final class Configuration
    {
        private final String _brokerName;
        private final String _virtualHostName;
        private final String _username;
        private final String _password;
        private final int _port;
        private final Set<Protocol> _protocols;
        private final List<QueueDefinition> _queues;
        private final List<ExchangeDefinition> _exchanges;
        private final List<BindingDefinition> _bindings;

        private Configuration(final Builder builder)
        {
            _brokerName = builder._brokerName;
            _virtualHostName = builder._virtualHostName;
            _username = builder._username;
            _password = builder._password;
            _port = builder._port;

            final Set<Protocol> protocols = EnumSet.noneOf(Protocol.class);
            for (final AmqpProtocol protocol : builder._protocols)
            {
                protocols.add(protocol.getProtocol());
            }
            _protocols = Collections.unmodifiableSet(protocols);
            _queues = List.copyOf(builder._queues.values());
            _exchanges = List.copyOf(builder._exchanges.values());
            _bindings = List.copyOf(builder._bindings);
        }

        String getVirtualHostName()
        {
            return _virtualHostName;
        }

        String getUsername()
        {
            return _username;
        }

        String getPassword()
        {
            return _password;
        }
    }

    private static final class QueueDefinition
    {
        private final String _name;
        private final Map<String, Object> _attributes;

        private QueueDefinition(final String name, final Map<String, Object> attributes)
        {
            _name = name;
            _attributes = attributes;
        }
    }

    private static final class ExchangeDefinition
    {
        private final String _name;
        private final ExchangeType _type;
        private final Map<String, Object> _attributes;

        private ExchangeDefinition(final String name,
                                   final ExchangeType type,
                                   final Map<String, Object> attributes)
        {
            _name = name;
            _type = type;
            _attributes = attributes;
        }
    }

    private static final class BindingDefinition
    {
        private final String _exchangeName;
        private final String _queueName;
        private final String _bindingKey;
        private final Map<String, Object> _arguments;

        private BindingDefinition(final String exchangeName,
                                  final String queueName,
                                  final String bindingKey,
                                  final Map<String, Object> arguments)
        {
            _exchangeName = exchangeName;
            _queueName = queueName;
            _bindingKey = bindingKey;
            _arguments = arguments;
        }
    }

    private static final class CapturingSystemLauncherListener
            extends SystemLauncherListener.DefaultSystemLauncherListener
    {
        private SystemConfig<?> _systemConfig;
        private RuntimeException _startupFailure;

        @Override
        public void errorOnStartup(final RuntimeException e)
        {
            _startupFailure = e;
        }

        @Override
        public void onContainerResolve(final SystemConfig<?> systemConfig)
        {
            _systemConfig = systemConfig;
        }

        private void throwIfStartupFailed()
        {
            if (_startupFailure != null)
            {
                throw new EmbeddedBrokerException("Broker-J reported a startup failure", _startupFailure);
            }
        }

        @SuppressWarnings("unchecked")
        private Broker<?> getBroker()
        {
            if (_systemConfig == null)
            {
                throw new EmbeddedBrokerException("Broker-J did not resolve its system configuration");
            }
            return _systemConfig.getContainer(Broker.class);
        }
    }

    private enum State
    {
        NEW,
        STARTING,
        STARTED,
        CLOSED
    }
}
