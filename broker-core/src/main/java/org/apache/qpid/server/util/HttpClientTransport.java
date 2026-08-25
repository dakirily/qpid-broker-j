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

    private final HttpClient _httpClient;
    private final Duration _requestTimeout;

    HttpClientTransport(final HttpClient httpClient, final Duration requestTimeout)
    {
        _httpClient = Objects.requireNonNull(httpClient, "httpClient");
        _requestTimeout = requestTimeout;
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public HttpRequest.Builder newRequestBuilder(final URI uri)
    {
        validateUri(uri);
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
        validateUri(request.uri());
        try
        {
            return _httpClient.send(request, HttpClientTransport::createResponseBodySubscriber);
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
        return _httpClient;
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

    private static void validateUri(final URI uri)
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

    public static final class Builder
    {
        private int _connectTimeout;
        private int _requestTimeout;
        private TrustManager[] _trustManagers;
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
            final SSLContext sslContext = createSslContext();
            final SSLParameters sslParameters = createSslParameters(sslContext);
            final HttpClient.Builder httpClientBuilder = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .sslContext(sslContext)
                    .sslParameters(sslParameters);
            if (_connectTimeout > 0)
            {
                httpClientBuilder.connectTimeout(Duration.ofMillis(_connectTimeout));
            }
            final Duration requestTimeout =
                    _requestTimeout > 0 ? Duration.ofMillis(_requestTimeout) : null;
            return new HttpClientTransport(httpClientBuilder.build(), requestTimeout);
        }

        private SSLContext createSslContext()
        {
            try
            {
                if (_trustManagers == null || _trustManagers.length == 0)
                {
                    return SSLContext.getDefault();
                }
                final SSLContext sslContext = SSLUtil.tryGetSSLContext();
                sslContext.init(null, _trustManagers.clone(), null);
                return sslContext;
            }
            catch (GeneralSecurityException e)
            {
                throw new ServerScopedRuntimeException("Cannot initialise TLS", e);
            }
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

        private static List<String> copyList(final List<String> values)
        {
            return values == null ? List.of() : List.copyOf(values);
        }
    }
}
