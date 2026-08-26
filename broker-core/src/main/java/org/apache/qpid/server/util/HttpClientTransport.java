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

package org.apache.qpid.server.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.LongSupplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;

/**
 * Reusable HTTP transport for security-sensitive broker integrations.
 * Response bodies are bounded because the supported integrations return small JSON documents.
 */
public final class HttpClientTransport
{
    private static final String HTTPS_SCHEME = "https";
    private static final String ENDPOINT_IDENTIFICATION_ALGORITHM = "HTTPS";
    private static final int MAXIMUM_RESPONSE_BODY_SIZE = 1024 * 1024;
    private static final long STATIC_TRUST_MANAGERS_VERSION = 0L;

    private final ClientConfiguration _clientConfiguration;
    private final Object _httpClientRefreshLock = new Object();
    private final Duration _requestTimeout;
    private volatile HttpClientState _httpClientState;

    HttpClientTransport(final HttpClient httpClient, final Duration requestTimeout)
    {
        _clientConfiguration = null;
        _httpClientState =
                new HttpClientState(Objects.requireNonNull(httpClient, "httpClient"), STATIC_TRUST_MANAGERS_VERSION);
        _requestTimeout = requestTimeout;
    }

    private HttpClientTransport(final ClientConfiguration clientConfiguration, final Duration requestTimeout)
    {
        _clientConfiguration = Objects.requireNonNull(clientConfiguration, "clientConfiguration");
        _requestTimeout = requestTimeout;
        if (clientConfiguration.hasDynamicTrustManagerSource())
        {
            // Managed trust stores are resolved before they attain their desired state during broker recovery.
            // Validate independent TLS settings now and resolve trust managers on first use.
            clientConfiguration.validateTlsConfiguration();
        }
        else
        {
            _httpClientState = clientConfiguration.createHttpClientState();
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public HttpRequest.Builder newRequestBuilder(final URI uri)
    {
        validateEndpointUri(uri);
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri);
        if (_requestTimeout != null)
        {
            requestBuilder.timeout(_requestTimeout);
        }
        return requestBuilder;
    }

    public HttpResponse<byte[]> send(final HttpRequest request) throws IOException
    {
        Objects.requireNonNull(request, "request");
        validateEndpointUri(request.uri());
        try
        {
            return getHttpClient().send(request, HttpClientTransport::createResponseBodySubscriber);
        }
        catch (HttpTimeoutException e)
        {
            final SocketTimeoutException timeoutException = new SocketTimeoutException(e.getMessage());
            timeoutException.initCause(e);
            throw timeoutException;
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            final InterruptedIOException interruptedException =
                    new InterruptedIOException("Interrupted while waiting for an HTTP response");
            interruptedException.initCause(e);
            throw interruptedException;
        }
    }

    HttpClient getHttpClient()
    {
        final ClientConfiguration clientConfiguration = _clientConfiguration;
        if (clientConfiguration == null)
        {
            return _httpClientState.getHttpClient();
        }

        HttpClientState state = _httpClientState;
        if (state == null ||
                state.getTrustManagersVersion() != clientConfiguration.getTrustManagersVersion())
        {
            synchronized (_httpClientRefreshLock)
            {
                state = _httpClientState;
                if (state == null ||
                        state.getTrustManagersVersion() != clientConfiguration.getTrustManagersVersion())
                {
                    state = clientConfiguration.createHttpClientState();
                    _httpClientState = state;
                }
            }
        }
        return state.getHttpClient();
    }

    static HttpResponse.BodySubscriber<byte[]> newLimitedBodySubscriber(final int maximumResponseBodySize)
    {
        return new LimitedBodySubscriber(maximumResponseBodySize);
    }

    private static HttpResponse.BodySubscriber<byte[]> createResponseBodySubscriber(
            final HttpResponse.ResponseInfo responseInfo)
    {
        Objects.requireNonNull(responseInfo, "responseInfo");
        return new LimitedBodySubscriber(MAXIMUM_RESPONSE_BODY_SIZE);
    }

    public static void validateEndpointUri(final URI uri)
    {
        Objects.requireNonNull(uri, "uri");
        if (!HTTPS_SCHEME.equalsIgnoreCase(uri.getScheme()))
        {
            throw new IllegalArgumentException("HTTP endpoint must use HTTPS: " + uri);
        }
        if (uri.getHost() == null)
        {
            throw new IllegalArgumentException("HTTP endpoint must include a host: " + uri);
        }
        if (uri.getUserInfo() != null)
        {
            throw new IllegalArgumentException("HTTP endpoint must not include user information: " + uri);
        }
        if (uri.getFragment() != null)
        {
            throw new IllegalArgumentException("HTTP endpoint must not include a fragment: " + uri);
        }
        if (uri.getPort() == 0 || uri.getPort() > 65535)
        {
            throw new IllegalArgumentException("HTTP endpoint includes an invalid port: " + uri);
        }
    }

    private static final class LimitedBodySubscriber implements HttpResponse.BodySubscriber<byte[]>
    {
        private final HttpResponse.BodySubscriber<byte[]> _delegate = HttpResponse.BodySubscribers.ofByteArray();
        private final int _maximumResponseBodySize;

        private Flow.Subscription _subscription;
        private long _receivedBodySize;
        private boolean _complete;

        private LimitedBodySubscriber(final int maximumResponseBodySize)
        {
            if (maximumResponseBodySize <= 0)
            {
                throw new IllegalArgumentException("Maximum response body size must be positive");
            }
            _maximumResponseBodySize = maximumResponseBodySize;
        }

        @Override
        public CompletionStage<byte[]> getBody()
        {
            return _delegate.getBody();
        }

        @Override
        public void onSubscribe(final Flow.Subscription subscription)
        {
            _subscription = Objects.requireNonNull(subscription, "subscription");
            _delegate.onSubscribe(subscription);
        }

        @Override
        public void onNext(final List<ByteBuffer> buffers)
        {
            if (_complete)
            {
                return;
            }
            long receivedSize = _receivedBodySize;
            for (final ByteBuffer buffer : buffers)
            {
                receivedSize += buffer.remaining();
            }
            if (receivedSize > _maximumResponseBodySize)
            {
                _complete = true;
                _subscription.cancel();
                _delegate.onError(
                        new IOException("HTTP response body exceeded " + _maximumResponseBodySize + " bytes"));
            }
            else
            {
                _receivedBodySize = receivedSize;
                _delegate.onNext(buffers);
            }
        }

        @Override
        public void onError(final Throwable throwable)
        {
            if (!_complete)
            {
                _complete = true;
                _delegate.onError(throwable);
            }
        }

        @Override
        public void onComplete()
        {
            if (!_complete)
            {
                _complete = true;
                _delegate.onComplete();
            }
        }
    }

    @FunctionalInterface
    public interface TrustManagerSupplier
    {
        TrustManager[] getTrustManagers() throws GeneralSecurityException;
    }

    private static final class ClientConfiguration
    {
        private final int _connectTimeout;
        private final TrustManager[] _trustManagers;
        private final TrustManagerSupplier _trustManagerSupplier;
        private final LongSupplier _trustManagersVersionSupplier;
        private final List<String> _tlsProtocolAllowList;
        private final List<String> _tlsProtocolDenyList;
        private final List<String> _tlsCipherSuiteAllowList;
        private final List<String> _tlsCipherSuiteDenyList;

        private ClientConfiguration(final Builder builder)
        {
            _connectTimeout = builder._connectTimeout;
            _trustManagers = builder._trustManagers == null ? null : builder._trustManagers.clone();
            _trustManagerSupplier = builder._trustManagerSupplier;
            _trustManagersVersionSupplier = builder._trustManagersVersionSupplier;
            _tlsProtocolAllowList = List.copyOf(builder._tlsProtocolAllowList);
            _tlsProtocolDenyList = List.copyOf(builder._tlsProtocolDenyList);
            _tlsCipherSuiteAllowList = List.copyOf(builder._tlsCipherSuiteAllowList);
            _tlsCipherSuiteDenyList = List.copyOf(builder._tlsCipherSuiteDenyList);
        }

        private long getTrustManagersVersion()
        {
            return _trustManagersVersionSupplier == null
                    ? STATIC_TRUST_MANAGERS_VERSION
                    : _trustManagersVersionSupplier.getAsLong();
        }

        private boolean hasDynamicTrustManagerSource()
        {
            return _trustManagerSupplier != null;
        }

        private void validateTlsConfiguration()
        {
            try
            {
                final SSLContext sslContext = SSLUtil.tryGetSSLContext();
                sslContext.init(null, new TrustManager[0], null);
                createSslParameters(sslContext);
            }
            catch (GeneralSecurityException e)
            {
                throw new ServerScopedRuntimeException("Cannot initialise TLS", e);
            }
        }

        private HttpClientState createHttpClientState()
        {
            try
            {
                if (_trustManagerSupplier == null)
                {
                    return createHttpClientState(STATIC_TRUST_MANAGERS_VERSION, _trustManagers);
                }

                while (true)
                {
                    final long versionBefore = getTrustManagersVersion();
                    final TrustManager[] trustManagers = _trustManagerSupplier.getTrustManagers();
                    final HttpClientState state = createHttpClientState(versionBefore, trustManagers);
                    if (versionBefore == getTrustManagersVersion())
                    {
                        return state;
                    }
                }
            }
            catch (GeneralSecurityException e)
            {
                throw new ServerScopedRuntimeException("Cannot initialise TLS", e);
            }
        }

        private HttpClientState createHttpClientState(final long trustManagersVersion,
                                                      final TrustManager[] trustManagers)
                throws GeneralSecurityException
        {
            final SSLContext sslContext = createSslContext(trustManagers);
            final SSLParameters sslParameters = createSslParameters(sslContext);
            final HttpClient.Builder httpClientBuilder = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .sslContext(sslContext)
                    .sslParameters(sslParameters);
            if (_connectTimeout > 0)
            {
                httpClientBuilder.connectTimeout(Duration.ofMillis(_connectTimeout));
            }
            return new HttpClientState(httpClientBuilder.build(), trustManagersVersion);
        }

        private SSLContext createSslContext(final TrustManager[] trustManagers) throws GeneralSecurityException
        {
            if (trustManagers == null || trustManagers.length == 0)
            {
                return SSLContext.getDefault();
            }
            final SSLContext sslContext = SSLUtil.tryGetSSLContext();
            sslContext.init(null, trustManagers.clone(), null);
            return sslContext;
        }

        private SSLParameters createSslParameters(final SSLContext sslContext)
        {
            final SSLParameters defaultParameters = sslContext.getDefaultSSLParameters();
            final SSLParameters supportedParameters = sslContext.getSupportedSSLParameters();
            final String[] enabledProtocols = SSLUtil.filterEnabledProtocols(defaultParameters.getProtocols(),
                                                                              supportedParameters.getProtocols(),
                                                                              _tlsProtocolAllowList,
                                                                              _tlsProtocolDenyList);
            final String[] enabledCipherSuites =
                    SSLUtil.filterEnabledCipherSuites(defaultParameters.getCipherSuites(),
                                                      supportedParameters.getCipherSuites(),
                                                      _tlsCipherSuiteAllowList,
                                                      _tlsCipherSuiteDenyList);
            if (enabledProtocols.length == 0)
            {
                throw new IllegalConfigurationException("TLS protocol filtering disabled every supported protocol");
            }
            if (enabledCipherSuites.length == 0)
            {
                throw new IllegalConfigurationException(
                        "TLS cipher suite filtering disabled every supported cipher suite");
            }
            defaultParameters.setProtocols(enabledProtocols);
            defaultParameters.setCipherSuites(enabledCipherSuites);
            defaultParameters.setEndpointIdentificationAlgorithm(ENDPOINT_IDENTIFICATION_ALGORITHM);
            return defaultParameters;
        }
    }

    private static final class HttpClientState
    {
        private final HttpClient _httpClient;
        private final long _trustManagersVersion;

        private HttpClientState(final HttpClient httpClient, final long trustManagersVersion)
        {
            _httpClient = Objects.requireNonNull(httpClient, "httpClient");
            _trustManagersVersion = trustManagersVersion;
        }

        private HttpClient getHttpClient()
        {
            return _httpClient;
        }

        private long getTrustManagersVersion()
        {
            return _trustManagersVersion;
        }
    }

    public static final class Builder
    {
        private int _connectTimeout;
        private int _requestTimeout;
        private TrustManager[] _trustManagers;
        private TrustManagerSupplier _trustManagerSupplier;
        private LongSupplier _trustManagersVersionSupplier;
        private List<String> _tlsProtocolAllowList = List.of();
        private List<String> _tlsProtocolDenyList = List.of();
        private List<String> _tlsCipherSuiteAllowList = List.of();
        private List<String> _tlsCipherSuiteDenyList = List.of();

        private Builder()
        {
        }

        public Builder setConnectTimeout(final int connectTimeout)
        {
            if (connectTimeout < 0)
            {
                throw new IllegalArgumentException("Connect timeout must not be negative");
            }
            _connectTimeout = connectTimeout;
            return this;
        }

        public Builder setRequestTimeout(final int requestTimeout)
        {
            if (requestTimeout < 0)
            {
                throw new IllegalArgumentException("Request timeout must not be negative");
            }
            _requestTimeout = requestTimeout;
            return this;
        }

        public Builder setTrustManagers(final TrustManager[] trustManagers)
        {
            _trustManagers = trustManagers == null ? null : trustManagers.clone();
            _trustManagerSupplier = null;
            _trustManagersVersionSupplier = null;
            return this;
        }

        public Builder setTrustManagerSource(final TrustManagerSupplier trustManagerSupplier,
                                             final LongSupplier trustManagersVersionSupplier)
        {
            _trustManagers = null;
            _trustManagerSupplier = Objects.requireNonNull(trustManagerSupplier, "trustManagerSupplier");
            _trustManagersVersionSupplier =
                    Objects.requireNonNull(trustManagersVersionSupplier, "trustManagersVersionSupplier");
            return this;
        }

        public Builder setTlsProtocolAllowList(final List<String> tlsProtocolAllowList)
        {
            _tlsProtocolAllowList = copyList(tlsProtocolAllowList);
            return this;
        }

        public Builder setTlsProtocolDenyList(final List<String> tlsProtocolDenyList)
        {
            _tlsProtocolDenyList = copyList(tlsProtocolDenyList);
            return this;
        }

        public Builder setTlsCipherSuiteAllowList(final List<String> tlsCipherSuiteAllowList)
        {
            _tlsCipherSuiteAllowList = copyList(tlsCipherSuiteAllowList);
            return this;
        }

        public Builder setTlsCipherSuiteDenyList(final List<String> tlsCipherSuiteDenyList)
        {
            _tlsCipherSuiteDenyList = copyList(tlsCipherSuiteDenyList);
            return this;
        }

        public HttpClientTransport build()
        {
            final Duration requestTimeout =
                    _requestTimeout > 0 ? Duration.ofMillis(_requestTimeout) : null;
            return new HttpClientTransport(new ClientConfiguration(this), requestTimeout);
        }

        private static List<String> copyList(final List<String> values)
        {
            return values == null ? List.of() : List.copyOf(values);
        }
    }
}
