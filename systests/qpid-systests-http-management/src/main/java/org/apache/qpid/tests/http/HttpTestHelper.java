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
package org.apache.qpid.tests.http;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class HttpTestHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTestHelper.class);

    private static final TypeReference<List<Map<String, Object>>> TYPE_LIST_OF_MAPS = new TypeReference<>() { };

    private static final TypeReference<Map<String, Object>> TYPE_MAP = new TypeReference<>() { };

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String API_BASE = "/api/latest/";
    private static final String REQUEST_TIMEOUT_PROPERTY = "qpid.resttest_request_timeout";
    private static final int DEFAULT_REQUEST_TIMEOUT = 30_000;

    private final int _httpPort;
    private final Duration _requestTimeout = getRequestTimeout();

    private String _username;
    private String _password;
    private String _acceptEncoding;
    private boolean _tls = false;
    private KeyStore _keyStore;
    private String _keyStorePassword;
    private KeyStore _trustStore;
    private HttpClient _httpClient;

    public HttpTestHelper(final BrokerAdmin admin)
    {
        this(admin, BrokerAdmin.PortType.HTTP_BROKER);
    }

    public HttpTestHelper(final BrokerAdmin admin, final BrokerAdmin.PortType portType)
    {
        this(admin, admin.getBrokerAddress(portType).getPort());
    }

    public HttpTestHelper(final BrokerAdmin admin, final int httpPort)
    {
        _httpPort = httpPort;
        _username = admin.getValidUsername();
        _password = admin.getValidPassword();
        _trustStore = admin.getHttpManagementTrustStore();
        _httpClient = createHttpClient();
    }

    public void setTls(final boolean tls)
    {
        _tls = tls;
    }

    private int getHttpPort()
    {
        return _httpPort;
    }

    private String getHostName()
    {
        return "localhost";
    }

    private String getManagementURL()
    {
        return (_tls ? "https" : "http") + "://" + getHostName() + ":" + getHttpPort();
    }

    private URI getManagementURI(final String path)
    {
        final String resolvedPath = path.startsWith("/") ? path : API_BASE + path;
        return URI.create(getManagementURL() + resolvedPath);
    }

    public HttpRequest.Builder createRequest(final String path, final String method)
    {
        final URI uri = getManagementURI(path);
        LOGGER.debug("Creating request : {} {}", method, uri);

        final HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .method(method, HttpRequest.BodyPublishers.noBody());
        if (!_requestTimeout.isZero())
        {
            builder.timeout(_requestTimeout);
        }
        if (_username != null)
        {
            final String credentials = _username + ":" + _password;
            final String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(UTF_8));
            builder.header("Authorization", "Basic " + encoded);
        }
        if (_acceptEncoding != null && !_acceptEncoding.isEmpty())
        {
            builder.header("Accept-Encoding", _acceptEncoding);
        }
        return builder;
    }

    public HttpResponse<byte[]> send(final HttpRequest.Builder builder) throws IOException
    {
        return send(builder.build());
    }

    public HttpResponse<byte[]> send(final HttpRequest request) throws IOException
    {
        try
        {
            final HttpResponse<byte[]> response =
                    _httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (LOGGER.isTraceEnabled())
            {
                LOGGER.trace("RESPONSE:{}", new String(response.body(), UTF_8));
            }
            return response;
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for HTTP response from " + request.uri(), e);
        }
    }

    private HttpClient createHttpClient()
    {
        try
        {
            final SSLContext sslContext = SSLUtil.tryGetSSLContext();
            KeyManager[] keyManagers = null;
            if (_keyStore != null)
            {
                final char[] password =
                        _keyStorePassword == null ? null : _keyStorePassword.toCharArray();
                final KeyManagerFactory factory =
                        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                factory.init(_keyStore, password);
                keyManagers = factory.getKeyManagers();
            }
            final TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(_trustStore);
            sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), null);
            final HttpClient.Builder httpClientBuilder = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .sslContext(sslContext)
                    .version(HttpClient.Version.HTTP_1_1);
            if (!_requestTimeout.isZero())
            {
                httpClientBuilder.connectTimeout(_requestTimeout);
            }
            return httpClientBuilder.build();
        }
        catch (final KeyStoreException
                | UnrecoverableKeyException
                | KeyManagementException
                | NoSuchAlgorithmException e)
        {
            throw new IllegalStateException("Cannot create HTTP test client", e);
        }
    }

    private static Duration getRequestTimeout()
    {
        final int requestTimeout = Integer.getInteger(REQUEST_TIMEOUT_PROPERTY, DEFAULT_REQUEST_TIMEOUT);
        if (requestTimeout < 0)
        {
            throw new IllegalArgumentException(
                    "System property '" + REQUEST_TIMEOUT_PROPERTY + "' must not be negative: " + requestTimeout);
        }
        return Duration.ofMillis(requestTimeout);
    }

    public Map<String, Object> readJsonResponseAsMap(final HttpResponse<byte[]> response) throws IOException
    {
        return OBJECT_MAPPER.readValue(response.body(), TYPE_MAP);
    }

    public Map<String, Object> getJsonAsSingletonList(final String path) throws IOException
    {
        final List<Map<String, Object>> response = getJsonAsList(path);

        assertNotNull(response, "Response cannot be null");
        assertEquals(1, response.size(), "Unexpected response from " + path);
        return response.get(0);
    }

    public List<Map<String, Object>> getJsonAsList(final String path) throws IOException
    {
        return getJson(path, TYPE_LIST_OF_MAPS, HttpServletResponse.SC_OK);
    }

    public Map<String, Object> getJsonAsMap(final String path) throws IOException
    {
        return getJson(path, TYPE_MAP, HttpServletResponse.SC_OK);
    }

    public <T> T getJson(final String path,
                         final TypeReference<T> valueTypeRef,
                         final int expectedResponseCode)
            throws IOException
    {
        final HttpResponse<byte[]> response = send(createRequest(path, "GET"));
        assertEquals(expectedResponseCode, response.statusCode(),
                     String.format("Unexpected response code from : %s", path));

        LOGGER.debug("Response : {}", new String(response.body(), UTF_8));
        return OBJECT_MAPPER.readValue(response.body(), valueTypeRef);
    }

    public <T> T postJson(final String path,
                          final Object data,
                          final TypeReference<T> valueTypeRef,
                          final int expectedResponseCode)
            throws IOException
    {
        final byte[] requestBody = OBJECT_MAPPER.writeValueAsBytes(data);
        final HttpRequest.Builder request = createRequest(path, "POST")
                .header("Content-Type", "application/json")
                .method("POST", HttpRequest.BodyPublishers.ofByteArray(requestBody));
        final HttpResponse<byte[]> response = send(request);
        assertEquals(expectedResponseCode, response.statusCode(),
                     String.format("Unexpected response code from : %s", path));

        LOGGER.debug("Response data: {}", new String(response.body(), UTF_8));
        return OBJECT_MAPPER.readValue(response.body(), valueTypeRef);
    }

    public int submitRequest(final String url, final String method, final Object data) throws IOException
    {
        return submitRequest(url, method, data, null);
    }

    public int submitRequest(final String url, final String method) throws IOException
    {
        return submitRequest(url, method, null, null);
    }

    public void submitRequest(final String url,
                              final String method,
                              final Object data,
                              final int expectedResponseCode)
            throws IOException
    {
        final int responseCode = submitRequest(url, method, data, null);
        assertEquals(expectedResponseCode, responseCode, "Unexpected response code from " + method + " " + url);
    }

    public void submitRequest(final String url, final String method, final int expectedResponseCode) throws IOException
    {
        submitRequest(url, method, null, expectedResponseCode);
    }

    public int submitRequest(final String url,
                             final String method,
                             final Object data,
                             final Map<String, List<String>> responseHeadersToCapture)
            throws IOException
    {
        final HttpRequest.Builder request = createRequest(url, method);
        if (data != null)
        {
            final byte[] requestBody = OBJECT_MAPPER.writeValueAsBytes(data);
            request.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofByteArray(requestBody));
        }
        final HttpResponse<byte[]> response = send(request);
        if (responseHeadersToCapture != null)
        {
            responseHeadersToCapture.putAll(response.headers().map());
        }
        LOGGER.debug("URL request completed : {}", response.statusCode());
        return response.statusCode();
    }

    public byte[] getBytes(final String path) throws IOException
    {
        return send(createRequest(path, "GET")).body();
    }

    public String getAcceptEncoding()
    {
        return _acceptEncoding;
    }

    public void setAcceptEncoding(final String acceptEncoding)
    {
        _acceptEncoding = acceptEncoding;
    }

    public void setKeyStore(final String keystore, final String password) throws Exception
    {
        _keyStorePassword = password;
        _keyStore = loadKeyStore(keystore, password);
        _httpClient = createHttpClient();
    }

    public void setTrustStore(final String trustStore, final String password) throws Exception
    {
        _trustStore = loadKeyStore(trustStore, password);
        _httpClient = createHttpClient();
    }

    private KeyStore loadKeyStore(final String store, final String password)
            throws IOException, GeneralSecurityException
    {
        if (store == null)
        {
            return null;
        }
        if (store.startsWith("data:"))
        {
            final URL url = new URL(null, store, new Handler());
            return SSLUtil.getInitializedKeyStore(url, password, KeyStore.getDefaultType());
        }
        try
        {
            final URL url = new URL(store);
            return SSLUtil.getInitializedKeyStore(url, password, KeyStore.getDefaultType());
        }
        catch (final MalformedURLException e)
        {
            return SSLUtil.getInitializedKeyStore(store, password, KeyStore.getDefaultType());
        }
    }

    public void setPassword(final String password)
    {
        _password = password;
    }

    public void setUserName(final String username)
    {
        _username = username;
    }

}
