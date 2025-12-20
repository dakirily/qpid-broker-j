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

package org.apache.qpid.systests.jms_3_1.extensions.tls;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.jms_3_1.extensions.BrokerManagementHelper;
import org.apache.qpid.systests.jms_3_1.extensions.TlsHelper;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.tests.utils.BrokerAdmin;

@JmsSystemTest
@Tag("security")
class TlsTest
{
    @Test
    void createSSLConnectionUsingConnectionURLParams(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);

        //Start the broker (NEEDing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), true, false, false);
        final var brokerAddress = jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);

        try (final var connection = jms.builder().connection().port(port)
                .host(brokerAddress.getHostName())
                .tls(true)
                .keyStoreLocation(tlsHelper.getClientKeyStore())
                .keyStorePassword(tls.getSecret())
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .create())
        {
            assertConnection(connection);
        }
    }

    @Test
    @Disabled("Qpid JMS Client does not support trusting of a certificate")
    void createSSLConnectionWithCertificateTrust(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), false, false, false);
        final var trustCertFile = tls.saveCertificateAsPem(tlsHelper.getCaCertificate()).toFile();
        final var brokerAddress = jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);

        try (final var connection = jms.builder().connection().port(port)
                .host(brokerAddress.getHostName())
                .tls(true)
                .options(Map.of("trusted_certs_path", encodePathOption(trustCertFile.getCanonicalPath())))
                .create())
        {
            assertConnection(connection);
        }
    }

    @Test
    void sslConnectionToPlainPortRejected(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        final var brokerAddress = jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);

        assertThrows(JMSException.class, () -> jms.builder().connection().port(brokerAddress.getPort())
                .host(brokerAddress.getHostName())
                .tls(true)
                .keyStoreLocation(tlsHelper.getClientKeyStore())
                .keyStorePassword(tls.getSecret())
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret()).create(),
                "Exception not thrown");

    }

    @Test
    void hostVerificationIsOnByDefault(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (NEEDing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), true, false, false);

        assertThrows(JMSException.class, () -> jms.builder().connection().port(port)
                .host("127.0.0.1")
                .tls(true)
                .keyStoreLocation(tlsHelper.getClientKeyStore())
                .keyStorePassword(tls.getSecret())
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .create(),
                "Exception not thrown");

        try (final var connection = jms.builder().connection().port(port)
                .host("127.0.0.1")
                .tls(true)
                .keyStoreLocation(tlsHelper.getClientKeyStore())
                .keyStorePassword(tls.getSecret())
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .verifyHostName(false)
                .create())
        {
            assertConnection(connection);
        }
    }

    @Test
    void createSslConnectionUsingJVMSettings(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (NEEDing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), true, false, false);

        try (final var connection = jms.builder().connection().port(port).tls(true)
                .keyStoreLocation(tlsHelper.getClientKeyStore())
                .keyStorePassword(tls.getSecret())
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .create())
        {
            assertConnection(connection);
        }
    }

    @Test
    void multipleCertsInSingleStore(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (NEEDing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), true, false, false);

        try (final var connection = jms.builder().connection().clientId(jms.testMethodName())
                .port(port)
                .tls(true)
                .keyStoreLocation(tlsHelper.getClientKeyStore())
                .keyStorePassword(tls.getSecret())
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .keyAlias(TlsHelper.CERT_ALIAS_APP1)
                .create())
        {
            assertConnection(connection);

            try (final var connection2 = jms.builder().connection().port(port)
                    .tls(true)
                    .keyStoreLocation(tlsHelper.getClientKeyStore())
                    .keyStorePassword(tls.getSecret())
                    .trustStoreLocation(tlsHelper.getClientTrustStore())
                    .trustStorePassword(tls.getSecret())
                    .keyAlias(TlsHelper.CERT_ALIAS_APP2)
                    .create())
            {
                assertConnection(connection2);
            }
        }
    }

    @Test
    void verifyHostNameWithIncorrectHostname(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (WANTing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), false, true, false);

        assertThrows(JMSException.class, () -> jms.builder().connection().port(port)
                        .host("127.0.0.1")
                        .tls(true)
                        .keyStoreLocation(tlsHelper.getClientKeyStore())
                        .keyStorePassword(tls.getSecret())
                        .trustStoreLocation(tlsHelper.getClientTrustStore())
                        .trustStorePassword(tls.getSecret())
                        .verifyHostName(true)
                        .create(),
                "Exception not thrown");
    }

    @Test
    void verifyLocalHost(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (WANTing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), false, true, false);

        try (final var connection = jms.builder().connection().port(port)
                .host("localhost")
                .tls(true)
                .keyStoreLocation(tlsHelper.getClientKeyStore())
                .keyStorePassword(tls.getSecret())
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .create())
        {
            assertConnection(connection);
        }
    }

    @Test
    void createSSLConnectionUsingConnectionURLParamsTrustStoreOnly(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (WANTing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), false, true, false);
        final var brokerAddress = jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);

        try (final var connection = jms.builder().connection().port(port)
                .host(brokerAddress.getHostName())
                .tls(true)
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .create())
        {
            assertConnection(connection);
        }
    }

    @Test
    void clientCertificateMissingWhilstNeeding(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (NEEDing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), true, false, false);

        assertThrows(JMSException.class, () -> jms.builder().connection().port(port)
                .host(jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP).getHostName())
                .tls(true)
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .create(),
                "Connection was established successfully");
    }

    @Test
    void clientCertificateMissingWhilstWanting(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (WANTing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), false, true, false);
        final var brokerAddress = jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);

        try (final var connection = jms.builder().connection().port(port)
                .host(brokerAddress.getHostName())
                .tls(true)
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .create())
        {
            assertConnection(connection);
        }
    }

    @Test
    void clientCertMissingWhilstWantingAndNeeding(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (NEEDing and WANTing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), true, true, false);

        assertThrows(JMSException.class, () -> jms.builder().connection().port(port)
                .host(jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP).getHostName())
                .tls(true)
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .create(),
                "Connection was established successfully");
    }

    @Test
    void createSSLandTCPonSamePort(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (WANTing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), false, true, true);
        final var brokerAddress = jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);

        try (final var connection = jms.builder().connection().port(port)
                .host(brokerAddress.getHostName())
                .tls(true)
                .keyStoreLocation(tlsHelper.getClientKeyStore())
                .keyStorePassword(tls.getSecret())
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .create())
        {
            assertConnection(connection);
        }

        try (final var connection2 = jms.builder().connection().port(port)
                .host(brokerAddress.getHostName())
                .create())
        {
            assertConnection(connection2);
        }
    }

    @Test
    @Disabled("Qpid JMS Client does not support trusting of a certificate")
    void createSSLWithCertFileAndPrivateKey(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        assumeTrue(is(equalTo("jks")).matches(java.security.KeyStore.getDefaultType()), "QPID-8255: certificate can only be loaded using jks keystore");

        final var tlsHelper = new TlsHelper(tls);
        //Start the broker (NEEDing client certificate authentication)
        final int port = configureTlsPort(jms, tls, tlsHelper, getTestPortName(jms), true, false, false);

        final Map<String, String> options = new HashMap<>();
        final var keyFile = tls.savePrivateKeyAsPem(tlsHelper.getClientPrivateKey()).toFile();
        final var certificateFile = tls.saveCertificateAsPem(tlsHelper.getClientCerificate(), tlsHelper.getCaCertificate()).toFile();
        options.put("client_cert_path", encodePathOption(certificateFile.getCanonicalPath()));
        options.put("client_cert_priv_key_path", encodePathOption(keyFile.getCanonicalPath()));
        final var brokerAddress = jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);

        try (final var connection = jms.builder().connection().port(port)
                .host(brokerAddress.getHostName())
                .tls(true)
                .trustStoreLocation(tlsHelper.getClientTrustStore())
                .trustStorePassword(tls.getSecret())
                .verifyHostName(false)
                .options(options)
                .create())
        {
            assertConnection(connection);
        }
    }

    private int configureTlsPort(final JmsSupport jms,
                                 final TlsResource tls,
                                 final TlsHelper tlsHelper,
                                 final String portName,
                                 final boolean needClientAuth,
                                 final boolean wantClientAuth,
                                 final boolean samePort) throws Exception
    {

        final String keyStoreName = portName + "KeyStore";
        final String trustStoreName = portName + "TrustStore";
        try (final var helper = new BrokerManagementHelper(jms))
        {
            helper.openManagementConnection();

            final int port = jms.brokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP).getPort();
            final String authenticationManager = helper.getAuthenticationProviderNameForAmqpPort(port);
            return helper.createKeyStore(keyStoreName, tlsHelper.getBrokerKeyStore(), tls.getSecret())
                    .createTrustStore(trustStoreName, tlsHelper.getBrokerTrustStore(), tls.getSecret())
                    .createAmqpTlsPort(portName, authenticationManager, keyStoreName, samePort,
                        needClientAuth, wantClientAuth, trustStoreName).getAmqpBoundPort(portName);
        }
    }

    private String  getTestPortName(final JmsSupport jms)
    {
        return jms.testMethodName() + "TlsPort";
    }

    private void assertConnection(final Connection connection) throws JMSException
    {
        assertNotNull(connection, "connection should be successful");
        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            assertNotNull(session, "create session should be successful");
        }
    }

    private String encodePathOption(final String canonicalPath)
    {
        return URLEncoder.encode(canonicalPath, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
