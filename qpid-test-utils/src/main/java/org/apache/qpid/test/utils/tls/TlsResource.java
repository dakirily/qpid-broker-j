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

package org.apache.qpid.test.utils.tls;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CRLException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.qpid.test.utils.exception.QpidTestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TlsResource implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TlsResource.class);

    private static final String PRIVATE_KEY_ALIAS = "private-key-alias";
    private static final String CERTIFICATE_ALIAS = "certificate-alias";
    private static final String SECRET = "secret";

    private Path _keystoreDirectory;
    private final String _privateKeyAlias;
    private final String _certificateAlias;
    private final char[] _secret;
    private final String _keyStoreType;

    public TlsResource()
    {
        this(PRIVATE_KEY_ALIAS, CERTIFICATE_ALIAS, SECRET, KeyStore.getDefaultType());
    }

    public TlsResource(final String privateKeyAlias,
                       final String certificateAlias,
                       final String secret,
                       final String defaultType)
    {
        _privateKeyAlias = privateKeyAlias;
        _certificateAlias = certificateAlias;
        _secret = secret == null ? null : secret.toCharArray();
        _keyStoreType = defaultType;
    }

    @Override
    public void close()
    {
        deleteFiles();
        clearSecret();
    }

    public String getSecret()
    {
        return _secret == null ? null : new String(_secret);
    }

    public char[] getSecretAsCharacters()
    {
        return _secret == null ? new char[]{} : _secret.clone();
    }

    public String getPrivateKeyAlias()
    {
        return _privateKeyAlias;
    }

    public String getCertificateAlias()
    {
        return _certificateAlias;
    }

    public String getKeyStoreType()
    {
        return _keyStoreType;
    }

    public Path createKeyStore(final KeyStoreEntry... entries)
    {
        return createKeyStore(getKeyStoreType(), entries);
    }

    public Path createKeyStore(final String keyStoreType, final KeyStoreEntry... entries)
    {
        final KeyStore keyStore = TlsResourceHelper.createKeyStore(keyStoreType, getSecretAsCharacters(), entries);
        return saveKeyStore(keyStoreType, keyStore);
    }

    public String createKeyStoreAsDataUrl(final KeyStoreEntry... entries)
    {
        return TlsResourceHelper.createKeyStoreAsDataUrl(getKeyStoreType(), getSecretAsCharacters(), entries);
    }

    public Path createSelfSignedKeyStore(final String dn)
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStore(new PrivateKeyEntry(_privateKeyAlias, keyCertPair));
    }

    public String createSelfSignedKeyStoreAsDataUrl(final String dn)
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStoreAsDataUrl(new PrivateKeyEntry(_privateKeyAlias, keyCertPair));
    }

    public Path createSelfSignedTrustStore(final String dn)
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStore(new CertificateEntry(_certificateAlias, keyCertPair.certificate()));
    }

    public Path createSelfSignedTrustStore(final String dn, final Instant from, final Instant to)
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn, from, to);
        return createKeyStore(new CertificateEntry(_certificateAlias, keyCertPair.certificate()));
    }

    public String createSelfSignedTrustStoreAsDataUrl(final String dn)
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        return createKeyStoreAsDataUrl(new CertificateEntry(_certificateAlias, keyCertPair.certificate()));
    }

    public Path createTrustStore(final String dn, final KeyCertificatePair ca)
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createKeyPairAndCertificate(dn, ca);
        return createKeyStore(new CertificateEntry(_certificateAlias, keyCertPair.certificate()));
    }

    public Path createSelfSignedKeyStoreWithCertificate(final String dn)
    {
        final KeyCertificatePair keyCertPair = TlsResourceBuilder.createSelfSigned(dn);
        final PrivateKeyEntry privateKeyEntry = new PrivateKeyEntry(_privateKeyAlias, keyCertPair);
        final CertificateEntry certificateEntry = new CertificateEntry(_certificateAlias, keyCertPair.certificate());
        return createKeyStore(privateKeyEntry, certificateEntry);
    }

    public Path createCrl(final KeyCertificatePair caPair, final X509Certificate... certificate)
    {
        final X509CRL crl = TlsResourceBuilder.createCertificateRevocationList(caPair, certificate);
        final Path pkFile = createFile(".crl");
        PemUtils.saveCrlAsPem(pkFile, crl);
        return pkFile;
    }

    public Path createCrlAsDer(final KeyCertificatePair caPair, final X509Certificate... certificate)
    {
        final X509CRL crl = TlsResourceBuilder.createCertificateRevocationList(caPair, certificate);
        try
        {
            return saveBytes(crl.getEncoded(), ".crl");
        }
        catch (final CRLException e)
        {
            throw new QpidTestException("Failed to encode CRL as DER", e);
        }
    }

    public String createCrlAsDataUrl(final KeyCertificatePair caPair, final X509Certificate... certificate)
    {
        final X509CRL crl = TlsResourceBuilder.createCertificateRevocationList(caPair, certificate);
        try
        {
            return TlsResourceHelper.getDataUrlForBytes(crl.getEncoded());
        }
        catch (final CRLException e)
        {
            throw new QpidTestException("Failed to encode CRL for data URL", e);
        }
    }

    public Path savePrivateKeyAsPem(final PrivateKey privateKey)
    {
        final Path pkFile = createFile(".pk.pem");
        PemUtils.savePrivateKeyAsPem(pkFile, privateKey);
        return pkFile;
    }

    public Path saveCertificateAsPem(final X509Certificate certificate)
    {
        final Path certificateFile = createFile(".cer.pem");
        PemUtils.saveCertificateAsPem(certificateFile, List.of(certificate));
        return certificateFile;
    }

    public Path saveCertificateAsPem(final List<X509Certificate> certificates)
    {
        final Path certificateFile = createFile(".cer.pem");
        PemUtils.saveCertificateAsPem(certificateFile, certificates);
        return certificateFile;
    }

    public Path savePrivateKeyAsDer(final PrivateKey privateKey)
    {
        return saveBytes(privateKey.getEncoded(), ".pk.der");
    }

    public Path saveCertificateAsDer(final X509Certificate certificate)
    {
        try
        {
            return saveBytes(certificate.getEncoded(), ".cer.der");
        }
        catch (CertificateEncodingException e)
        {
            throw new QpidTestException("Failed to encode X509 certificate to DER", e);
        }
    }

    public Path createFile(final String suffix)
    {
        try
        {
            return Files.createTempFile(ensureKeystoreDirectory(), "tls", suffix);
        }
        catch (IOException e)
        {
            throw new QpidTestException("Failed to create temp file in '%s' with suffix '%s'"
                    .formatted(_keystoreDirectory, suffix), e);
        }
    }

    private Path saveBytes(final byte[] bytes, final String extension)
    {
        final Path path = createFile(extension);
        try
        {
            Files.write(path, bytes);
        }
        catch (IOException e)
        {
            throw new QpidTestException("Failed to write %d bytes to '%s'".formatted(bytes.length, path), e);
        }
        return path;
    }

    private Path saveKeyStore(final String keyStoreType, final KeyStore ks)
    {
        final Path storePath = createFile("." + keyStoreType);
        TlsResourceHelper.saveKeyStoreIntoFile(ks, getSecretAsCharacters(), storePath);
        return storePath;
    }

    private void deleteFiles()
    {
        if (_keystoreDirectory == null)
        {
            return;
        }
        if (!Files.exists(_keystoreDirectory))
        {
            return;
        }
        try (final Stream<Path> stream = Files.walk(_keystoreDirectory))
        {
            stream.sorted(Comparator.reverseOrder()).forEach(path ->
            {
                try
                {
                    Files.deleteIfExists(path);
                }
                catch (IOException e)
                {
                    final var message = "Could not delete path at %s".formatted(path);
                    LOGGER.warn(message, e);
                }
            });
        }
        catch (Exception e)
        {
            LOGGER.warn("Failure to clean up test resources", e);
        }
    }

    private void clearSecret()
    {
        if (_secret != null)
        {
            Arrays.fill(_secret, '\0');
        }
    }

    private synchronized Path ensureKeystoreDirectory()
    {
        if (_keystoreDirectory != null)
        {
            return _keystoreDirectory;
        }
        try
        {
            final Path targetDir = Path.of("target");
            Files.createDirectories(targetDir);
            _keystoreDirectory = Files.createTempDirectory(targetDir, "test-tls-resources-");
            LOGGER.debug("Test keystore directory is created : '{}'", _keystoreDirectory);
            return _keystoreDirectory;
        }
        catch (IOException e)
        {
            throw new QpidTestException("Failed to create TLS resource directory under 'target'", e);
        }
    }
}
