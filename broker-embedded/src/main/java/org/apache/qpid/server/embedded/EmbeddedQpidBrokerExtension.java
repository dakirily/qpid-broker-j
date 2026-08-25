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
import java.util.Map;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * Class-scoped JUnit Jupiter extension for {@link EmbeddedQpidBroker}.
 *
 * <p>Register the extension in a {@code static final} field using {@code @RegisterExtension}. The broker is started
 * before the test class and stopped after it. Test methods can either use this extension's getters or request an
 * {@link EmbeddedQpidBroker} parameter.</p>
 */
public final class EmbeddedQpidBrokerExtension
        implements BeforeAllCallback, AfterAllCallback, ParameterResolver
{
    private final EmbeddedQpidBroker.Configuration _configuration;

    private EmbeddedQpidBroker _broker;

    /**
     * Creates a default AMQP 1.0 embedded broker extension.
     */
    public EmbeddedQpidBrokerExtension()
    {
        this(EmbeddedQpidBroker.builder().buildConfiguration());
    }

    private EmbeddedQpidBrokerExtension(final EmbeddedQpidBroker.Configuration configuration)
    {
        _configuration = configuration;
    }

    /**
     * Creates a configurable extension builder.
     *
     * @return extension builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public synchronized void beforeAll(final ExtensionContext context)
    {
        if (_broker != null)
        {
            throw new IllegalStateException("Embedded broker extension has already been started");
        }
        _broker = new EmbeddedQpidBroker(_configuration);
        _broker.start();
    }

    @Override
    public synchronized void afterAll(final ExtensionContext context)
    {
        if (_broker != null)
        {
            try
            {
                _broker.close();
            }
            finally
            {
                _broker = null;
            }
        }
    }

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext,
                                     final ExtensionContext extensionContext)
    {
        return parameterContext.getParameter().getType() == EmbeddedQpidBroker.class;
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext,
                                   final ExtensionContext extensionContext)
            throws ParameterResolutionException
    {
        try
        {
            return getBroker();
        }
        catch (final IllegalStateException e)
        {
            throw new ParameterResolutionException("Embedded broker is not available", e);
        }
    }

    /**
     * Returns the running broker managed by this extension.
     *
     * @return running embedded broker
     */
    public synchronized EmbeddedQpidBroker getBroker()
    {
        if (_broker == null || !_broker.isRunning())
        {
            throw new IllegalStateException("Embedded broker extension has not been started");
        }
        return _broker;
    }

    /**
     * Returns the broker's AMQP socket address.
     *
     * @return AMQP socket address
     */
    public InetSocketAddress getAmqpAddress()
    {
        return getBroker().getAmqpAddress();
    }

    /**
     * Returns the broker's base AMQP URI.
     *
     * @return base AMQP URI
     */
    public URI getAmqpUri()
    {
        return getBroker().getAmqpUri();
    }

    /**
     * Returns the configured virtual-host name.
     *
     * @return virtual-host name
     */
    public String getVirtualHostName()
    {
        return _configuration.getVirtualHostName();
    }

    /**
     * Returns the configured AMQP username.
     *
     * @return username
     */
    public String getUsername()
    {
        return _configuration.getUsername();
    }

    /**
     * Returns the configured AMQP password.
     *
     * @return password
     */
    public String getPassword()
    {
        return _configuration.getPassword();
    }

    /**
     * Builder mirroring the embedded broker topology builder for JUnit usage.
     */
    public static final class Builder
    {
        private final EmbeddedQpidBroker.Builder _delegate = EmbeddedQpidBroker.builder();

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
            _delegate.brokerName(brokerName);
            return this;
        }

        /**
         * Sets the default virtual-host name.
         *
         * @param virtualHostName virtual-host name
         * @return this builder
         */
        public Builder virtualHost(final String virtualHostName)
        {
            _delegate.virtualHost(virtualHostName);
            return this;
        }

        /**
         * Sets the broker credentials.
         *
         * @param username username
         * @param password password, which may be empty
         * @return this builder
         */
        public Builder credentials(final String username, final String password)
        {
            _delegate.credentials(username, password);
            return this;
        }

        /**
         * Selects a fixed AMQP port, or zero for a dynamically allocated port.
         *
         * @param port configured port
         * @return this builder
         */
        public Builder port(final int port)
        {
            _delegate.port(port);
            return this;
        }

        /**
         * Replaces the set of enabled AMQP protocols.
         *
         * @param protocols one or more protocols
         * @return this builder
         */
        public Builder protocols(final AmqpProtocol... protocols)
        {
            _delegate.protocols(protocols);
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
            _delegate.queue(name);
            return this;
        }

        /**
         * Adds a queue with configured-object attributes.
         *
         * @param name queue name
         * @param attributes additional queue attributes
         * @return this builder
         */
        public Builder queue(final String name, final Map<String, Object> attributes)
        {
            _delegate.queue(name, attributes);
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
            _delegate.exchange(name, type);
            return this;
        }

        /**
         * Adds an exchange with configured-object attributes.
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
            _delegate.exchange(name, type, attributes);
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
            _delegate.binding(exchangeName, queueName, bindingKey);
            return this;
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
            _delegate.binding(exchangeName, queueName, bindingKey, arguments);
            return this;
        }

        /**
         * Creates the configured JUnit extension.
         *
         * @return embedded broker extension
         */
        public EmbeddedQpidBrokerExtension build()
        {
            return new EmbeddedQpidBrokerExtension(_delegate.buildConfiguration());
        }
    }
}
