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

package org.apache.qpid.systests.jms_3_1.extensions.sasl;

import static org.apache.qpid.systests.EntityTypes.AMQP_PORT;
import static org.apache.qpid.test.utils.UnitTestBase.getJvmVendor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import javax.naming.NamingException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.Callback;

import org.hamcrest.Matchers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManagerImpl;
import org.apache.qpid.server.security.auth.manager.ScramSHA1AuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.systests.builder.ConnectionBuilder;
import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;
import org.apache.qpid.systests.jms_3_1.extensions.BrokerManagementHelper;
import org.apache.qpid.test.utils.JvmVendor;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.apache.qpid.test.utils.tls.types.CertificateEntry;
import org.apache.qpid.test.utils.tls.types.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.types.PrivateKeyEntry;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;

@JmsSystemTest
@Tag("security")
class AuthenticationTest
{
    private static final String DN_CA = "CN=MyRootCA,O=ACME,ST=Ontario,C=CA";
    private static final String DN_BROKER = "CN=localhost,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown";
    private static final String DN_INTERMEDIATE = "CN=intermediate_ca@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_APP1 = "CN=app1@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_APP2 = "CN=app2@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_INT =
            "CN=allowed_by_ca_with_intermediate@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_ALLOWED = "CN=allowed_by_ca@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_REVOKED = "CN=revoked_by_ca@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_REVOKED_BY_EMPTY =
            "CN=revoked_by_ca_empty_crl@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_REVOKED_INVALID_CRL =
            "CN=revoked_by_ca_invalid_crl_path@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA";
    private static final String DN_CLIENT_UNTRUSTED = "CN=untrusted_client";
    private static final String CERT_ALIAS_ROOT_CA = "rootca";
    private static final String CERT_ALIAS_APP1 = "app1";
    private static final String CERT_ALIAS_APP2 = "app2";
    private static final String CERT_ALIAS_ALLOWED = "allowed_by_ca";
    private static final String CERT_ALIAS_REVOKED = "revoked_by_ca";
    private static final String CERT_ALIAS_REVOKED_EMPTY_CRL = "revoked_by_ca_empty_crl";
    private static final String CERT_ALIAS_REVOKED_INVALID_CRL_PATH = "revoked_by_ca_invalid_crl_path";
    private static final String CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE = "allowed_by_ca_with_intermediate";
    private static final String CERT_ALIAS_UNTRUSTED_CLIENT = "untrusted_client";

    private static final String USER = "user";
    private static final String USER_PASSWORD = "user";

    private static final Server CRL_SERVER = new Server();
    private static final Handler.Sequence HANDLERS = new Handler.Sequence();

    private static final String CRL_TEMPLATE = "http://localhost:%d/%s";

    private static int crlHttpPort = -1;
    private static String _brokerKeyStore;
    private static String _brokerTrustStore;
    private static String _clientKeyStore;
    private static String _clientTrustStore;
    private static String _brokerPeerStore;
    private static String _clientExpiredKeyStore;
    private static String _clientUntrustedKeyStore;
    private static Path _crlFile;
    private static Path _emptyCrlFile;
    private static Path _intermediateCrlFile;

    @BeforeAll
    public static void setUp(final TlsResource tls) throws Exception
    {
        _crlFile = tls.createFile(".crl");
        _emptyCrlFile = tls.createFile("-empty.crl");
        _intermediateCrlFile = tls.createFile("-intermediate.crl");

        final var connector = new ServerConnector(CRL_SERVER);
        connector.setPort(0);
        connector.setHost("localhost");

        CRL_SERVER.addConnector(connector);
        createContext(_crlFile);
        createContext(_emptyCrlFile);
        createContext(_intermediateCrlFile);
        CRL_SERVER.setHandler(HANDLERS);
        CRL_SERVER.start();
        crlHttpPort = connector.getLocalPort();

        buildTlsResources(tls);
    }

    private static void buildTlsResources(final TlsResource tls) throws Exception
    {
        final String crlUri = CRL_TEMPLATE.formatted(crlHttpPort, _crlFile.toFile().getName());
        final String emptyCrlUri = CRL_TEMPLATE.formatted(crlHttpPort, _emptyCrlFile.toFile().getName());
        final String intermediateCrlUri = CRL_TEMPLATE.formatted(crlHttpPort, _intermediateCrlFile.toFile().getName());
        final String nonExistingCrlUri = CRL_TEMPLATE.formatted(crlHttpPort, "not/a/crl");

        final var caPair = TlsResourceBuilder.createKeyPairAndRootCA(DN_CA);
        final var brokerKeyPair = TlsResourceBuilder.createRSAKeyPair();
        final var brokerCertificate = TlsResourceBuilder
                .createCertificateForServerAuthorization(brokerKeyPair, caPair, DN_BROKER);

        final var privateKeyEntry = new PrivateKeyEntry("java-broker", brokerKeyPair.getPrivate(),
                brokerCertificate, caPair.certificate());
        final var certificateEntry = new CertificateEntry(CERT_ALIAS_ROOT_CA, caPair.certificate());
        _brokerKeyStore = tls.createKeyStore(privateKeyEntry, certificateEntry)
                .toFile().getAbsolutePath();
        _brokerTrustStore = tls.createKeyStore(new CertificateEntry(CERT_ALIAS_ROOT_CA, caPair.certificate()))
                .toFile().getAbsolutePath();

        final var clientApp1KeyPair = TlsResourceBuilder.createRSAKeyPair();
        final var clientApp1Certificate = TlsResourceBuilder
                .createCertificateForClientAuthorization(clientApp1KeyPair, caPair, DN_CLIENT_APP1);

        _brokerPeerStore = tls.createKeyStore(new CertificateEntry(DN_CLIENT_APP1, clientApp1Certificate))
                .toFile().getAbsolutePath();

        final var clientApp2KeyPair = TlsResourceBuilder.createRSAKeyPair();
        final var clientApp2Certificate = TlsResourceBuilder.createCertificateForClientAuthorization(clientApp2KeyPair,
                                                                           caPair, DN_CLIENT_APP2);

        final var clientAllowedKeyPair = TlsResourceBuilder.createRSAKeyPair();
        final var clientAllowedCertificate = TlsResourceBuilder
                .createCertificateWithCrlDistributionPoint(clientAllowedKeyPair, caPair, DN_CLIENT_ALLOWED, crlUri);

        final var clientRevokedKeyPair = TlsResourceBuilder.createRSAKeyPair();
        final var clientRevokedCertificate = TlsResourceBuilder
                .createCertificateWithCrlDistributionPoint(clientRevokedKeyPair, caPair, DN_CLIENT_REVOKED, crlUri);

        final var clientKeyPairRevokedByEmpty = TlsResourceBuilder.createRSAKeyPair();
        final var clientCertificateRevokedByEmpty = TlsResourceBuilder
                .createCertificateWithCrlDistributionPoint(clientKeyPairRevokedByEmpty, caPair, DN_CLIENT_REVOKED_BY_EMPTY, emptyCrlUri);

        final var clientKeyPairInvalidClr = TlsResourceBuilder.createRSAKeyPair();
        final var clientCertificateInvalidClr = TlsResourceBuilder
                .createCertificateWithCrlDistributionPoint(clientKeyPairInvalidClr, caPair, DN_CLIENT_REVOKED_INVALID_CRL, nonExistingCrlUri);

        final var intermediateCA = TlsResourceBuilder.createKeyPairAndIntermediateCA(DN_INTERMEDIATE, caPair, crlUri);
        final var clientKeyPairIntermediate = TlsResourceBuilder.createRSAKeyPair();
        final var clientCertificateIntermediate = TlsResourceBuilder
                .createCertificateWithCrlDistributionPoint(clientKeyPairIntermediate, intermediateCA, DN_CLIENT_INT, intermediateCrlUri);

        final var clientKeyPairExpired = TlsResourceBuilder.createRSAKeyPair();
        final var from = Instant.now().minus(10, ChronoUnit.DAYS);
        final var to = Instant.now().minus(5, ChronoUnit.DAYS);
        final var clientCertificateExpired = TlsResourceBuilder
                .createCertificate(clientKeyPairExpired, caPair, "CN=user1", from, to);
        _clientExpiredKeyStore = tls.createKeyStore(new PrivateKeyEntry("user1", clientKeyPairExpired.getPrivate(),
                clientCertificateExpired, caPair.certificate())).toFile().getAbsolutePath();

        _clientKeyStore = tls.createKeyStore(
                new PrivateKeyEntry(CERT_ALIAS_APP1,
                                    clientApp1KeyPair.getPrivate(),
                                    clientApp1Certificate,
                                    caPair.certificate()),
                new PrivateKeyEntry(CERT_ALIAS_APP2,
                                    clientApp2KeyPair.getPrivate(),
                                    clientApp2Certificate,
                                    caPair.certificate()),
                new PrivateKeyEntry(CERT_ALIAS_ALLOWED,
                                    clientAllowedKeyPair.getPrivate(),
                                    clientAllowedCertificate,
                                    caPair.certificate()),
                new PrivateKeyEntry(CERT_ALIAS_REVOKED,
                                    clientRevokedKeyPair.getPrivate(),
                                    clientRevokedCertificate,
                                    caPair.certificate()),
                new PrivateKeyEntry(CERT_ALIAS_REVOKED_EMPTY_CRL,
                                    clientKeyPairRevokedByEmpty.getPrivate(),
                                    clientCertificateRevokedByEmpty,
                                    caPair.certificate()),
                new PrivateKeyEntry(CERT_ALIAS_REVOKED_INVALID_CRL_PATH,
                                    clientKeyPairInvalidClr.getPrivate(),
                                    clientCertificateInvalidClr,
                                    caPair.certificate()),
                new PrivateKeyEntry(CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE,
                                    clientKeyPairIntermediate.getPrivate(),
                                    clientCertificateIntermediate,
                                    intermediateCA.certificate(),
                                    caPair.certificate()),
                new CertificateEntry(CERT_ALIAS_ROOT_CA, caPair.certificate())).toFile().getAbsolutePath();

        _clientTrustStore = tls.createKeyStore(new CertificateEntry(CERT_ALIAS_ROOT_CA, caPair.certificate()))
                .toFile().getAbsolutePath();

        final Path crl = tls.createCrlAsDer(caPair, clientRevokedCertificate, intermediateCA.certificate());
        Files.copy(crl, _crlFile, StandardCopyOption.REPLACE_EXISTING);

        final Path emptyCrl = tls.createCrlAsDer(caPair);
        Files.copy(emptyCrl, _emptyCrlFile, StandardCopyOption.REPLACE_EXISTING);

        final Path intermediateCrl = tls.createCrlAsDer(caPair);
        Files.copy(intermediateCrl, _intermediateCrlFile, StandardCopyOption.REPLACE_EXISTING);

        final KeyCertificatePair clientKeyPairUntrusted = TlsResourceBuilder.createSelfSigned(DN_CLIENT_UNTRUSTED);
        _clientUntrustedKeyStore = tls.createKeyStore(new PrivateKeyEntry(CERT_ALIAS_APP1, clientKeyPairUntrusted.privateKey(),
                clientKeyPairUntrusted.certificate())).toFile().getAbsolutePath();
    }

    @AfterAll
    public static void tearDown() throws Exception
    {
        CRL_SERVER.stop();
    }

    @Test
    void sha256(final JmsSupport jms) throws Exception
    {
        final int port = createAuthenticationProviderAndUserAndPort(jms, ScramSHA256AuthenticationManager.PROVIDER_TYPE);

        assertPlainConnectivity(jms, port, ScramSHA256AuthenticationManager.MECHANISM);
    }

    @Test
    void sha1(final JmsSupport jms) throws Exception
    {
        final int port = createAuthenticationProviderAndUserAndPort(jms, ScramSHA1AuthenticationManager.PROVIDER_TYPE);

        assertPlainConnectivity(jms, port, ScramSHA1AuthenticationManager.MECHANISM);
    }

    @Test
    void external(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final int port = createExternalProviderAndTlsPort(jms, tls, getBrokerTrustStoreAttributes(tls), null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_ALLOWED);
    }

    @Test
    void externalWithRevocationWithDataUrlCrlFileAndAllowedCertificate(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, createDataUrlForFile(_crlFile));
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_ALLOWED);
    }

    @Test
    void externalWithRevocationWithDataUrlCrlFileAndRevokedCertificate(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, createDataUrlForFile(_crlFile));
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertNoTlsConnectivity(jms, tls, port, CERT_ALIAS_REVOKED);
    }

    @Test
    void externalWithRevocationWithCrlFileAndAllowedCertificate(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, _crlFile.toFile().getAbsolutePath());
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_ALLOWED);
    }

    @Test
    void externalWithRevocationWithCrlFileAndAllowedCertificateWithoutPreferCrls(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, _crlFile.toFile().getAbsolutePath());
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_PREFERRING_CERTIFICATE_REVOCATION_LIST,
                                 false);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertNoTlsConnectivity(jms, tls, port, CERT_ALIAS_ALLOWED);
    }

    @Test
    void externalWithRevocationWithCrlFileAndRevokedCertificate(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, _crlFile.toFile().getAbsolutePath());
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertNoTlsConnectivity(jms, tls, port, CERT_ALIAS_REVOKED);
    }

    @Test
    void externalWithRevocationWithEmptyCrlFileAndRevokedCertificate(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL,
                                 _emptyCrlFile.toFile().getAbsolutePath());
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_ALLOWED);
    }

    @Test
    void externalWithRevocationAndAllowedCertificateWithCrlUrl(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        assumeTrue(Matchers.not(JvmVendor.IBM).matches(getJvmVendor()));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_ALLOWED);
    }

    @Test
    void externalWithRevocationAndRevokedCertificateWithCrlUrl(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertNoTlsConnectivity(jms, tls, port, CERT_ALIAS_REVOKED);
    }

    @Test
    void externalWithRevocationAndRevokedCertificateWithCrlUrlWithEmptyCrl(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        assumeTrue(Matchers.not(JvmVendor.IBM).matches(getJvmVendor()));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_REVOKED_EMPTY_CRL);
    }

    @Test
    void externalWithRevocationDisabledWithCrlFileAndRevokedCertificate(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, false);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_LIST_URL, _crlFile.toFile().getAbsolutePath());
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_REVOKED);
    }

    @Test
    void externalWithRevocationDisabledWithCrlUrlInRevokedCertificate(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, false);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_REVOKED);
    }

    @Test
    void externalWithRevocationAndRevokedCertificateWithCrlUrlWithSoftFail(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        assumeTrue(Matchers.not(JvmVendor.IBM).matches(getJvmVendor()));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_IGNORING_SOFT_FAILURES, true);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_REVOKED_INVALID_CRL_PATH);
    }

    @Test
    void externalWithRevocationAndRevokedCertificateWithCrlUrlWithoutPreferCrls(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_PREFERRING_CERTIFICATE_REVOCATION_LIST,
                                 false);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertNoTlsConnectivity(jms, tls, port, CERT_ALIAS_ALLOWED);
    }

    @Test
    void externalWithRevocationAndRevokedCertificateWithCrlUrlWithoutPreferCrlsWithFallback(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        assumeTrue(Matchers.not(JvmVendor.IBM).matches(getJvmVendor()));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_PREFERRING_CERTIFICATE_REVOCATION_LIST,
                                 false);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_NO_FALLBACK, false);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_ALLOWED);
    }

    @Test
    void externalWithRevocationAndRevokedIntermediateCertificateWithCrlUrl(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_OF_ONLY_END_ENTITY_CERTIFICATES, false);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_IGNORING_SOFT_FAILURES, true);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertNoTlsConnectivity(jms, tls, port, CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE);
    }

    @Test
    void externalWithRevocationAndRevokedIntermediateCertificateWithCrlUrlOnlyEndEntity(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        assumeTrue(Matchers.not(JvmVendor.IBM).matches(getJvmVendor()));
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_ENABLED, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_OF_ONLY_END_ENTITY_CERTIFICATES, true);
        trustStoreAttributes.put(FileTrustStore.CERTIFICATE_REVOCATION_CHECK_WITH_IGNORING_SOFT_FAILURES, true);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE);
    }

    @Test
    void externalDeniesUntrustedClientCert(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final int port = createExternalProviderAndTlsPort(jms, tls, getBrokerTrustStoreAttributes(tls), null, false);

        assertThrows(JMSException.class, () -> getConnectionBuilder(jms, tls, port, CERT_ALIAS_UNTRUSTED_CLIENT)
                .keyStoreLocation(_clientUntrustedKeyStore)
                .create()
                .close(),
                "Should not be able to create a connection to the SSL port");
    }

    @Test
    void externalDeniesExpiredClientCert(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, _brokerPeerStore);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, tls.getSecret());
        trustStoreAttributes.put(FileTrustStore.TRUST_ANCHOR_VALIDITY_ENFORCED, true);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);

        assertThrows(JMSException.class, () ->
                {
                    try (final var connection = jms.builder().connection().port(port)
                            .tls(true)
                            .saslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                            .keyStoreLocation(_clientExpiredKeyStore)
                            .keyStorePassword(tls.getSecret())
                            .trustStoreLocation(_clientTrustStore)
                            .trustStorePassword(tls.getSecret())
                            .create())
                    {
                        connection.start();
                    }
                },
                "Connection should not succeed");
    }

    @Test
    void externalWithPeersOnlyTrustStore(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, _brokerPeerStore);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, tls.getSecret());
        trustStoreAttributes.put(FileTrustStore.PEERS_ONLY, true);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_APP1);

        assertNoTlsConnectivity(jms, tls, port, CERT_ALIAS_APP2);
    }

    @Test
    void externalWithRegularAndPeersOnlyTrustStores(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final String trustStoreName = jms.testMethodName() + "RegularTrustStore";

        try (final var helper = new BrokerManagementHelper(jms))
        {
            final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);
            helper.openManagementConnection()
                  .createEntity(trustStoreName, FileTrustStore.class.getName(), trustStoreAttributes);
        }

        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, _brokerPeerStore);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, tls.getSecret());
        trustStoreAttributes.put(FileTrustStore.PEERS_ONLY, true);
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, trustStoreName, false);
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_APP1);

        //use the app2 cert, which is NOT in the peerstore (but is signed by the same CA as app1)
        assertTlsConnectivity(jms, tls, port, CERT_ALIAS_APP2);
    }

    @Test
    void externalUsernameAsDN(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);

        final String clientId = jms.testMethodName();
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, true);

        try (final var connection = getConnectionBuilder(jms, tls, port, CERT_ALIAS_APP2).clientId(clientId).create())
        {
            try (final BrokerManagementHelper helper = new BrokerManagementHelper(jms))
            {
                String principal =
                        helper.openManagementConnection().getConnectionPrincipalByClientId(getPortName(jms), clientId);
                assertEquals("CN=app2@acme.org,OU=art,O=acme,L=Toronto,ST=ON,C=CA", principal,
                        "Unexpected principal");
            }
        }
        catch (Exception e)
        {
            fail("Should be able to create a connection to the SSL port: " + e.getMessage());
        }
    }

    @Test
    void externalUsernameAsCN(final TlsResource tls, final JmsSupport jms) throws Exception
    {
        final Map<String, Object> trustStoreAttributes = getBrokerTrustStoreAttributes(tls);

        final String clientId = jms.testMethodName();
        final int port = createExternalProviderAndTlsPort(jms, tls, trustStoreAttributes, null, false);

        try (final var connection = getConnectionBuilder(jms, tls, port, CERT_ALIAS_APP2).clientId(clientId).create())
        {
            try (final BrokerManagementHelper helper = new BrokerManagementHelper(jms))
            {
                String principal =
                        helper.openManagementConnection().getConnectionPrincipalByClientId(getPortName(jms), clientId);
                assertEquals("app2@acme.org", principal, "Unexpected principal");
            }
        }
        catch (Exception e)
        {
            fail("Should be able to create a connection to the SSL port: " + e.getMessage());
        }
    }

    private Map<String, Object> getBrokerTrustStoreAttributes(final TlsResource tls)
    {
        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put(FileTrustStore.STORE_URL, _brokerTrustStore);
        trustStoreAttributes.put(FileTrustStore.PASSWORD, tls.getSecret());
        trustStoreAttributes.put(FileTrustStore.TRUST_STORE_TYPE, tls.getKeyStoreType());
        return trustStoreAttributes;
    }

    private int createExternalProviderAndTlsPort(final JmsSupport jms,
                                                 final TlsResource tls,
                                                 final Map<String, Object> trustStoreAttributes,
                                                 final String additionalTrustStore,
                                                 final boolean useFullDN) throws Exception
    {
        final String providerName = jms.testMethodName();
        final String keyStoreName = providerName + "KeyStore";
        final String trustStoreName = providerName + "TrustStore";
        final String portName = getPortName(jms);
        final Map<String, Object> trustStoreSettings = new HashMap<>(trustStoreAttributes);

        final String[] trustStores = additionalTrustStore == null
                ? new String[]{trustStoreName}
                : new String[]{trustStoreName, additionalTrustStore};

        try (BrokerManagementHelper helper = new BrokerManagementHelper(jms))
        {
            return helper.openManagementConnection()
                         .createExternalAuthenticationProvider(providerName, useFullDN)
                         .createKeyStore(keyStoreName, _brokerKeyStore, tls.getSecret())
                         .createEntity(trustStoreName, FileTrustStore.class.getName(), trustStoreSettings)
                         .createAmqpTlsPort(portName, providerName, keyStoreName, false, true, false, trustStores)
                         .getAmqpBoundPort(portName);
        }
    }

    private String getPortName(final JmsSupport jms)
    {
        return jms.testMethodName() + "TlsPort";
    }

    private int createAuthenticationProviderAndUserAndPort(final JmsSupport jms,
                                                           final String providerType) throws Exception
    {
        final String providerName = jms.testMethodName();
        final String portName = providerName + "Port";
        final Map<String, Object> portAttributes = new HashMap<>();
        portAttributes.put(Port.AUTHENTICATION_PROVIDER, providerName);
        portAttributes.put(Port.PORT, 0);

        try (BrokerManagementHelper helper = new BrokerManagementHelper(jms))
        {
            return helper.openManagementConnection()
                         .createAuthenticationProvider(providerName, providerType)
                         .createUser(providerName, USER, USER_PASSWORD)
                         .createEntity(portName, AMQP_PORT, portAttributes)
                         .getAmqpBoundPort(portName);
        }
    }

    private Connection getConnection(final JmsSupport jms, final TlsResource tls, int port, String certificateAlias) throws NamingException, JMSException
    {
        return getConnectionBuilder(jms, tls, port, certificateAlias).create();
    }

    private ConnectionBuilder getConnectionBuilder(final JmsSupport jms, final TlsResource tls, int port, String certificateAlias)
    {
        return jms.builder().connection().port(port)
                                     .tls(true)
                                     .saslMechanisms(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
                                     .keyStoreLocation(_clientKeyStore)
                                     .keyStorePassword(tls.getSecret())
                                     .keyAlias(certificateAlias)
                                     .trustStoreLocation(_clientTrustStore)
                                     .trustStorePassword(tls.getSecret());
    }

    private void assertTlsConnectivity(final JmsSupport jms, final TlsResource tls, int port, String certificateAlias) throws NamingException, JMSException
    {
        try (final var connection = getConnection(jms, tls, port, certificateAlias);
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            assertNotNull(session.createTemporaryQueue(), "Temporary queue was not created");
        }
    }

    private void assertNoTlsConnectivity(final JmsSupport jms, final TlsResource tls, int port, String certificateAlias) throws NamingException
    {
        assertThrows(JMSException.class,
                () -> getConnection(jms, tls, port, certificateAlias),
                "Connection should not succeed");
    }


    private void assertPlainConnectivity(final JmsSupport jms,
                                         final int port,
                                         final String mechanism) throws Exception
    {
        try (final var connection = jms.builder().connection().port(port)
                .username(USER)
                .password(USER_PASSWORD)
                .saslMechanisms(mechanism)
                .create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            final TemporaryQueue queue = session.createTemporaryQueue();
            assertNotNull(queue, "Temporary queue was not created");
        }

        assertThrows(JMSException.class, () ->
                {
                    try (final var connection = jms.builder().connection().port(port)
                            .username(USER)
                            .password("invalid" + USER_PASSWORD)
                            .saslMechanisms(mechanism)
                            .create())
                    {
                        connection.start();
                    }
                },
                "Connection is established for invalid password");

        assertThrows(JMSException.class, () ->
                {
                    try (final var connection = jms.builder().connection().port(port)
                            .username("invalid" + AuthenticationTest.USER)
                            .password(USER_PASSWORD)
                            .saslMechanisms(mechanism)
                            .create())
                    {
                        connection.start();
                    }
                },
                "Connection is established for invalid user name");
    }

    private static void createContext(final Path crlPath)
    {
        final var contextHandler = new ContextHandler();
        contextHandler.setContextPath("/" + crlPath.getFileName());
        contextHandler.setHandler(new CrlServerHandler(crlPath));
        HANDLERS.addHandler(contextHandler);
    }

    public static String createDataUrlForFile(final Path file) throws IOException
    {
        return DataUrlUtils.getDataUrlForBytes(Files.readAllBytes(file));
    }

    private static class CrlServerHandler extends Handler.Abstract
    {
        final Path _crlPath;

        CrlServerHandler(final Path crlPath)
        {
            _crlPath = crlPath;
        }

        @Override
        public boolean handle(final Request request, final Response response, final Callback callback) throws Exception
        {
            final byte[] crlBytes = Files.readAllBytes(_crlPath);
            response.setStatus(200);
            response.write(true, ByteBuffer.wrap(crlBytes), callback);
            return true;
        }
    }
}
