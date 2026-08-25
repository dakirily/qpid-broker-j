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
package org.apache.qpid.tests.http.authentication;

import static jakarta.servlet.http.HttpServletResponse.SC_CREATED;
import static jakarta.servlet.http.HttpServletResponse.SC_MOVED_TEMPORARILY;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tools.jackson.core.type.TypeReference;

import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.ManagedPeerCertificateTrustStore;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;
import org.apache.qpid.server.util.BaseAction;
import org.apache.qpid.test.utils.tls.AltNameType;
import org.apache.qpid.test.utils.tls.AlternativeName;
import org.apache.qpid.test.utils.tls.CertificateEntry;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;

public class PreemptiveAuthenticationTest extends HttpTestBase
{
    private static final TypeReference<String> STRING_TYPE_REF = new TypeReference<>() { };
    private static final String STORE_PASSWORD = "password";

    private Deque<BaseAction<Void, Exception>> _tearDownActions;

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_tearDownActions != null)
        {
            Exception exception = null;
            while (!_tearDownActions.isEmpty())
            {
                try
                {
                    _tearDownActions.removeLast().performAction(null);
                }
                catch (Exception e)
                {
                    exception = e;
                }
            }

            if (exception != null)
            {
                throw exception;
            }
        }
    }

    @Test
    public void clientAuthSuccess() throws Exception
    {
        final HttpTestHelper helper = configForClientAuth("CN=localhost");

        final String userId = helper.getJson("broker/getUser", STRING_TYPE_REF, SC_OK);
        assertThat(userId, startsWith("localhost@"));
    }

    @Test
    public void clientAuthenticationWebManagementConsole() throws Exception
    {
        final HttpTestHelper helper = configForClientAuth("CN=localhost");

        final HttpResponse<byte[]> redirect =
                helper.send(helper.createRequest(HttpManagement.DEFAULT_LOGIN_URL, "GET"));
        final String cookies = redirect.headers().allValues("Set-Cookie").stream()
                .map(value -> value.split(";", 2)[0])
                .collect(Collectors.joining("; "));

        assertThat(redirect.statusCode(), is(equalTo(SC_MOVED_TEMPORARILY)));

        final HttpRequest.Builder authenticatedRequest =
                helper.createRequest(HttpManagement.DEFAULT_LOGIN_URL, "GET").header("Cookie", cookies);
        final HttpResponse<byte[]> authenticatedResponse = helper.send(authenticatedRequest);

        assertThat(authenticatedResponse.statusCode(), is(equalTo(SC_OK)));
    }

    @Test
    public void clientAuthUnrecognisedCert() throws Exception
    {
        final HttpTestHelper helper = configForClientAuth("CN=foo");

        final String keyStore = createKeyStoreDataUrl(getKeyCertPair("CN=bar"));
        helper.setKeyStore(keyStore, STORE_PASSWORD);

        try
        {
            helper.getJson("broker/getUser", STRING_TYPE_REF, SC_OK);
            Assertions.fail("Exception not thrown");
        }
        catch (IOException e)
        {
            // PASS
        }
    }

    @Test
    public void basicAuth() throws Exception
    {
        verifyGetBroker(SC_OK);
    }

    @Test
    public void basicAuthWrongPassword() throws Exception
    {
        getBrokerHelper().setPassword("badpassword");

        verifyGetBroker(HttpServletResponse.SC_UNAUTHORIZED);
    }

    @Test
    public void httpBasicAuthDisabled() throws Exception
    {
        doBasicAuthDisabledTest(false);
    }

    @Test
    public void httpsBasicAuthDisabled() throws Exception
    {
        doBasicAuthDisabledTest(true);
    }

    @Test
    public void anonymousTest() throws Exception
    {
        final HttpTestHelper helper = configForAnonymous();

        final String userId = helper.getJson("broker/getUser", STRING_TYPE_REF, SC_OK);
        assertThat(userId, startsWith("ANONYMOUS@"));
    }

    @Test
    public void noSessionCreated() throws Exception
    {
        final HttpResponse<byte[]> response =
                getBrokerHelper().send(getBrokerHelper().createRequest("broker", "GET"));
        assertThat("Unexpected server response", response.statusCode(), is(equalTo(SC_OK)));
        Assertions.assertTrue(response.headers().firstValue("Set-Cookie").isEmpty(), "Unexpected cookie");
    }

    private void verifyGetBroker(final int expectedResponseCode) throws Exception
    {
        assertThat(getBrokerHelper().submitRequest("broker", "GET"), is(equalTo(expectedResponseCode)));
    }

    private void doBasicAuthDisabledTest(final boolean tls) throws Exception
    {
        final HttpTestHelper configHelper = new HttpTestHelper(getBrokerAdmin());
        configHelper.setTls(!tls);
        final String authEnabledAttrName = tls
                ? HttpManagement.HTTPS_BASIC_AUTHENTICATION_ENABLED
                : HttpManagement.HTTP_BASIC_AUTHENTICATION_ENABLED;
        try
        {
            final HttpTestHelper helper = new HttpTestHelper(getBrokerAdmin());
            helper.setTls(tls);
            assertThat(helper.submitRequest("broker", "GET"), is(equalTo(SC_OK)));

            configHelper.submitRequest("plugin/httpManagement",
                                       "POST",
                                       Map.of(authEnabledAttrName, Boolean.FALSE),
                                       SC_OK);

            assertThat(helper.submitRequest("broker", "GET"), is(equalTo(SC_UNAUTHORIZED)));
        }
        finally
        {
            configHelper.submitRequest("plugin/httpManagement",
                                       "POST",
                                       Map.of(authEnabledAttrName, Boolean.TRUE),
                                       SC_OK);
        }
    }

    private HttpTestHelper configForClientAuth(final String x500Name) throws Exception
    {
        final KeyCertificatePair clientKeyCertPair = getKeyCertPair(x500Name);
        final byte[] clientCertificate = clientKeyCertPair.certificate().getEncoded();
        final String clientKeyStore = createKeyStoreDataUrl(clientKeyCertPair);

        final KeyCertificatePair brokerKeyCertPair = getKeyCertPair(x500Name);
        final String brokerKeyStore = createKeyStoreDataUrl(brokerKeyCertPair);
        final String brokerTrustStore = createTrustStoreDataUrl(brokerKeyCertPair);

        final Deque<BaseAction<Void, Exception>> deleteActions = new ArrayDeque<>();

        final Map<String, Object> authAttr = new HashMap<>();
        authAttr.put(ExternalAuthenticationManager.TYPE, "External");
        authAttr.put(ExternalAuthenticationManager.ATTRIBUTE_USE_FULL_DN, false);

        getBrokerHelper().submitRequest("authenticationprovider/myexternal", "PUT", authAttr, SC_CREATED);

        deleteActions.add(object ->
                getBrokerHelper().submitRequest("authenticationprovider/myexternal", "DELETE", SC_OK));

        final Map<String, Object> keystoreAttr = new HashMap<>();
        keystoreAttr.put(FileKeyStore.TYPE, "FileKeyStore");
        keystoreAttr.put(FileKeyStore.STORE_URL, brokerKeyStore);
        keystoreAttr.put(FileKeyStore.PASSWORD, STORE_PASSWORD);
        keystoreAttr.put(FileKeyStore.KEY_STORE_TYPE, KeyStore.getDefaultType());

        getBrokerHelper().submitRequest("keystore/mykeystore", "PUT", keystoreAttr, SC_CREATED);
        deleteActions.add(object -> getBrokerHelper().submitRequest("keystore/mykeystore", "DELETE", SC_OK));

        final Map<String, Object> truststoreAttr = new HashMap<>();
        truststoreAttr.put(ManagedPeerCertificateTrustStore.TYPE, ManagedPeerCertificateTrustStore.TYPE_NAME);
        truststoreAttr.put(ManagedPeerCertificateTrustStore.STORED_CERTIFICATES,
                List.of(Base64.getEncoder().encodeToString(clientCertificate)));


        getBrokerHelper().submitRequest("truststore/mytruststore", "PUT", truststoreAttr, SC_CREATED);
        deleteActions.add(object -> getBrokerHelper().submitRequest("truststore/mytruststore", "DELETE", SC_OK));

        final Map<String, Object> portAttr = new HashMap<>();
        portAttr.put(Port.TYPE, "HTTP");
        portAttr.put(Port.PORT, 0);
        portAttr.put(Port.AUTHENTICATION_PROVIDER, "myexternal");
        portAttr.put(Port.PROTOCOLS, Set.of(Protocol.HTTP));
        portAttr.put(Port.TRANSPORTS, Set.of(Transport.SSL));
        portAttr.put(Port.NEED_CLIENT_AUTH, true);
        portAttr.put(Port.KEY_STORE, "mykeystore");
        portAttr.put(Port.TRUST_STORES, List.of("mytruststore"));

        getBrokerHelper().submitRequest("port/myport", "PUT", portAttr, SC_CREATED);
        deleteActions.add(object -> getBrokerHelper().submitRequest("port/myport", "DELETE", SC_OK));

        final Map<String, Object> clientAuthPort = getBrokerHelper().getJsonAsMap("port/myport");
        final int boundPort = Integer.parseInt(String.valueOf(clientAuthPort.get("boundPort")));

        assertThat(boundPort, is(greaterThan(0)));

        _tearDownActions = deleteActions;

        final HttpTestHelper helper = new HttpTestHelper(getBrokerAdmin(), boundPort);
        helper.setTls(true);
        helper.setKeyStore(clientKeyStore, STORE_PASSWORD);
        helper.setTrustStore(brokerTrustStore, STORE_PASSWORD);
        return helper;
    }

    private HttpTestHelper configForAnonymous() throws Exception
    {
        final Deque<BaseAction<Void, Exception>> deleteActions = new ArrayDeque<>();

        final Map<String, Object> authAttr = new HashMap<>();
        authAttr.put(AnonymousAuthenticationManager.TYPE, AnonymousAuthenticationManager.PROVIDER_TYPE);

        getBrokerHelper().submitRequest("authenticationprovider/myanon", "PUT", authAttr, SC_CREATED);

        deleteActions.add(object -> getBrokerHelper().submitRequest("authenticationprovider/myanon", "DELETE", SC_OK));

        final Map<String, Object> portAttr = new HashMap<>();
        portAttr.put(Port.TYPE, "HTTP");
        portAttr.put(Port.PORT, 0);
        portAttr.put(Port.AUTHENTICATION_PROVIDER, "myanon");
        portAttr.put(Port.PROTOCOLS, Set.of(Protocol.HTTP));
        portAttr.put(Port.TRANSPORTS, Set.of(Transport.TCP));

        getBrokerHelper().submitRequest("port/myport", "PUT", portAttr, SC_CREATED);
        deleteActions.add(object -> getBrokerHelper().submitRequest("port/myport", "DELETE", SC_OK));

        final Map<String, Object> clientAuthPort = getBrokerHelper().getJsonAsMap("port/myport");
        final int boundPort = Integer.parseInt(String.valueOf(clientAuthPort.get("boundPort")));

        assertThat(boundPort, is(greaterThan(0)));

        _tearDownActions = deleteActions;

        final HttpTestHelper helper = new HttpTestHelper(getBrokerAdmin(), boundPort);
        helper.setPassword(null);
        helper.setUserName(null);
        return helper;
    }

    private String createKeyStoreDataUrl(final KeyCertificatePair keyCertPair) throws Exception
    {
        return TlsResourceHelper.createKeyStoreAsDataUrl(KeyStore.getDefaultType(),
                STORE_PASSWORD.toCharArray(),
                new PrivateKeyEntry("key1", keyCertPair));
    }

    private String createTrustStoreDataUrl(final KeyCertificatePair keyCertPair) throws Exception
    {
        return TlsResourceHelper.createKeyStoreAsDataUrl(KeyStore.getDefaultType(),
                                                         STORE_PASSWORD.toCharArray(),
                                                         new CertificateEntry("certificate",
                                                                              keyCertPair.certificate()));
    }

    private KeyCertificatePair getKeyCertPair(final String x500Name) throws Exception
    {
        final String loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();
        final AlternativeName ipAddress = new AlternativeName(AltNameType.IP_ADDRESS, loopbackAddress);
        final AlternativeName localhost = new AlternativeName(AltNameType.DNS_NAME, "localhost");
        return TlsResourceBuilder.createSelfSigned(x500Name, ipAddress, localhost);
    }

}
