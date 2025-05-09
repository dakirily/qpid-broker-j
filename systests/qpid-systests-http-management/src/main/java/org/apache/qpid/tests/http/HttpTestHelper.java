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

import static jakarta.servlet.http.HttpServletResponse.SC_CREATED;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import jakarta.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.security.NonJavaKeyStore;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class HttpTestHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTestHelper.class);

    private static final TypeReference<List<Map<String, Object>>> TYPE_LIST_OF_MAPS = new TypeReference<>() { };

    private static final TypeReference<Map<String, Object>> TYPE_MAP = new TypeReference<>() { };

    private static final TypeReference<Boolean> TYPE_BOOLEAN = new TypeReference<>() { };

    private static final String API_BASE = "/api/latest/";
    private final int _httpPort;
    private String _username;
    private String _password;
    private String _requestHostName;
    private final int _connectTimeout = Integer.getInteger("qpid.resttest_connection_timeout", 30000);

    private String _acceptEncoding;
    private boolean _tls = false;

    private KeyStore _keyStore;
    private String _keyStorePassword;

    public HttpTestHelper(final BrokerAdmin admin)
    {
        this(admin, null);
    }

    public HttpTestHelper(BrokerAdmin admin, final String requestHostName)
    {
        this(admin, requestHostName, admin.getBrokerAddress(BrokerAdmin.PortType.HTTP).getPort());
    }

    public HttpTestHelper(BrokerAdmin admin, final String requestHostName, final int httpPort)
    {
        _httpPort = httpPort;
        _username = admin.getValidUsername();
        _password = admin.getValidPassword();
        _requestHostName = requestHostName;
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

    private URL getManagementURL(String path) throws MalformedURLException
    {
        return new URL(getManagementURL() + path);
    }

    public HttpURLConnection openManagementConnection(String path, String method) throws IOException
    {
        if (!path.startsWith("/"))
        {
            path = API_BASE + path;
        }
        final URL url = getManagementURL(path);
        LOGGER.debug("Opening connection : {} {}", method, url);
        HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
        if (httpCon instanceof HttpsURLConnection)
        {
            HttpsURLConnection httpsCon = (HttpsURLConnection) httpCon;
            try
            {
                SSLContext sslContext = SSLUtil.tryGetSSLContext();
                TrustManager[] trustAllCerts = new TrustManager[] {new TrustAllTrustManager()};

                KeyManager[] keyManagers = null;
                if (_keyStore != null)
                {
                    char[] keyStoreCharPassword = _keyStorePassword == null ? null : _keyStorePassword.toCharArray();
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(_keyStore, keyStoreCharPassword);
                    keyManagers = kmf.getKeyManagers();
                }
                sslContext.init(keyManagers, trustAllCerts, null);
                httpsCon.setSSLSocketFactory(sslContext.getSocketFactory());
                httpsCon.setHostnameVerifier((s, sslSession) -> true);
            }
            catch (KeyStoreException | UnrecoverableKeyException | KeyManagementException | NoSuchAlgorithmException e)
            {
                throw new RuntimeException(e);
            }
        }
        httpCon.setConnectTimeout(_connectTimeout);
        if (_requestHostName != null)
        {
            httpCon.setRequestProperty("Host", _requestHostName);
        }

        if(_username != null)
        {
            String encoded = Base64.getEncoder().encodeToString((_username + ":" + _password).getBytes(UTF_8));
            httpCon.setRequestProperty("Authorization", "Basic " + encoded);
        }

        if (_acceptEncoding != null && !"".equals(_acceptEncoding))
        {
            httpCon.setRequestProperty("Accept-Encoding", _acceptEncoding);
        }

        httpCon.setDoOutput(true);
        httpCon.setRequestMethod(method);
        return httpCon;
    }

    public Map<String, Object> readJsonResponseAsMap(HttpURLConnection connection) throws IOException
    {
        byte[] data = readConnectionInputStream(connection);

        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ByteArrayInputStream(data), TYPE_MAP);
    }

    private byte[] readConnectionInputStream(HttpURLConnection connection) throws IOException
    {
        try (InputStream is = connection.getInputStream())
        {
            final byte[] bytes = is.readAllBytes();
            if (LOGGER.isTraceEnabled())
            {
                LOGGER.trace("RESPONSE:" + new String(bytes, UTF_8));
            }
            return bytes;
        }
    }

    private void writeJsonRequest(HttpURLConnection connection, Object data) throws IOException
    {
        try (OutputStream outputStream = connection.getOutputStream())
        {
            new ObjectMapper().writeValue(outputStream, data);
        }
    }

    public Map<String, Object> getJsonAsSingletonList(String path) throws IOException
    {
        List<Map<String, Object>> response = getJsonAsList(path);

        assertNotNull(response, "Response cannot be null");
        assertEquals(1, response.size(), "Unexpected response from " + path);
        return response.get(0);
    }

    public List<Map<String, Object>> getJsonAsList(String path) throws IOException
    {
        return getJson(path, TYPE_LIST_OF_MAPS, HttpServletResponse.SC_OK);
    }

    public Map<String, Object> getJsonAsMap(String path) throws IOException
    {
        return getJson(path, TYPE_MAP, HttpServletResponse.SC_OK);
    }

    public <T> T getJson(String path, final TypeReference<T> valueTypeRef, int expectedResponseCode) throws IOException
    {
        int responseCode = -1;
        HttpURLConnection connection = openManagementConnection(path, "GET");
        try
        {
            connection.connect();
            responseCode = connection.getResponseCode();
            assertEquals(expectedResponseCode, responseCode, String.format("Unexpected response code from : %s", path));

            byte[] data = readConnectionInputStream(connection);
            LOGGER.debug("Response : {}", new String(data, StandardCharsets.UTF_8));
            return new ObjectMapper().readValue(new ByteArrayInputStream(data), valueTypeRef);
        }
        finally
        {

            LOGGER.debug("URL request completed : {}", responseCode);
            connection.disconnect();
        }
    }

    public <T> T postJson(String path, final Object data, final TypeReference<T> valueTypeRef, int expectedResponseCode) throws IOException
    {
        int responseCode = -1;
        HttpURLConnection connection = openManagementConnection(path, "POST");

        try
        {
            connection.connect();
            writeJsonRequest(connection, data);
            responseCode = connection.getResponseCode();
            assertEquals(expectedResponseCode, responseCode, String.format("Unexpected response code from : %s", path));

            byte[] buf = readConnectionInputStream(connection);
            LOGGER.debug("Response data: {}", new String(buf, StandardCharsets.UTF_8));
            return new ObjectMapper().readValue(new ByteArrayInputStream(buf), valueTypeRef);
        }
        finally
        {
            LOGGER.debug("URL request completed : {}", responseCode);
            connection.disconnect();
        }
    }

    public int submitRequest(String url, String method, Object data) throws IOException
    {
        return submitRequest(url, method, data, null);
    }

    public int submitRequest(String url, String method) throws IOException
    {
        return submitRequest(url, method, null, null);
    }

    public void submitRequest(String url, String method, Object data, int expectedResponseCode) throws IOException
    {
        Map<String, List<String>> headers = new HashMap<>();
        int responseCode = submitRequest(url, method, data, headers);
        assertEquals(expectedResponseCode, responseCode, "Unexpected response code from " + method + " " + url);
    }

    public void submitRequest(String url, String method, int expectedResponseCode) throws IOException
    {
        submitRequest(url, method, null, expectedResponseCode);
    }

    public int submitRequest(String url, String method, Object data, Map<String, List<String>> responseHeadersToCapture) throws IOException
    {
        HttpURLConnection connection = openManagementConnection(url, method);
        int responseCode = -1;
        try
        {
            if (data != null)
            {
                writeJsonRequest(connection, data);
            }
            responseCode = connection.getResponseCode();
            if (responseHeadersToCapture != null)
            {
                responseHeadersToCapture.putAll(connection.getHeaderFields());
            }
            return responseCode;
        }
        finally
        {
            LOGGER.debug("URL request completed : {}", responseCode);
            connection.disconnect();
        }
    }

    public byte[] getBytes(String path) throws IOException
    {
        HttpURLConnection connection = openManagementConnection(path, "GET");
        try
        {
            return readConnectionInputStream(connection);
        }
        finally
        {
            connection.disconnect();
        }
    }

    public String getAcceptEncoding()
    {
        return _acceptEncoding;
    }

    public void setAcceptEncoding(String acceptEncoding)
    {
        _acceptEncoding = acceptEncoding;
    }

    public void setKeyStore(final String keystore, final String password) throws Exception
    {
        _keyStorePassword = password;

        if (keystore != null)
        {
            try
            {
                URL ks = new URL(keystore);
                _keyStore = SSLUtil.getInitializedKeyStore(ks, password, KeyStore.getDefaultType());
            }
            catch (MalformedURLException e)
            {
                _keyStore = SSLUtil.getInitializedKeyStore(keystore, password, KeyStore.getDefaultType());
            }
        }
        else
        {
            _keyStore = null;
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

    /**
     * System tests use "Host" header to supply virtualhost name (which is build from test class name and test method
     * name). This leads to invalid SNI error on Jetty side when using HTTPS connection.
     * This method provides workaround by generating new keystore containing certificate with the correct CN and replacing
     * the default keystore with the generated one.
     *
     * @param cn CN Certificate CN (consists of test class name and test method name)
     *
     * @throws Exception
     */
    public void createKeyStoreAndSetItOnPort(final String cn) throws Exception
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned("CN=" + cn);
        final String privateKey = DataUrlUtils.getDataUrlForBytes(TlsResourceHelper.toPEM(keyCertPair.getPrivateKey())
                .getBytes(UTF_8));
        final String certificate = DataUrlUtils.getDataUrlForBytes(TlsResourceHelper.toPEM(keyCertPair.getCertificate())
                .getBytes(UTF_8));
        final Map<String, Object> attributes = Map.of(
                NonJavaKeyStore.NAME, cn,
                NonJavaKeyStore.PRIVATE_KEY_URL, privateKey,
                NonJavaKeyStore.CERTIFICATE_URL, certificate,
                NonJavaKeyStore.TYPE, "NonJavaKeyStore");

        final String requestHostName = _requestHostName;
        _requestHostName = "localhost";
        setTls(false);
        submitRequest("keystore/" + cn, "PUT", attributes, SC_CREATED);
        submitRequest("port/HTTP", "POST", Map.of(Port.KEY_STORE, cn), SC_OK);
        postJson("port/HTTP/updateTLS", Map.of(), TYPE_BOOLEAN, SC_OK);
        setTls(true);
        _requestHostName = requestHostName;
    }

    /**
     * System tests use "Host" header to supply virtualhost name (which is build from test class name and test method
     * name). This leads to invalid SNI error on Jetty side when using HTTPS connection.
     * This method removes previously generated keystore and replaces it with the default one.
     *
     * @param cn CN Certificate CN (consists of test class name and test method name)
     *
     * @throws Exception
     */
    public void removeKeyStoreFromPort(final String cn) throws Exception
    {
        submitRequest("port/HTTP", "POST", Map.of(Port.KEY_STORE, "systestsKeyStore"), SC_OK);
        submitRequest("keystore/" + cn, "DELETE", Map.of(), SC_OK);
        postJson("port/HTTP/updateTLS", Map.of(), TYPE_BOOLEAN, SC_OK);
        setTls(false);
    }

    private static class TrustAllTrustManager implements X509TrustManager
    {
        public X509Certificate[] getAcceptedIssuers()
        {
            return new X509Certificate[0];
        }

        @Override
        public void checkClientTrusted(X509Certificate[] certs, String authType)
        {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] certs, String authType)
        {
        }
    }
}
