package org.apache.qpid.test.utils.tls.utils;

import org.apache.qpid.test.utils.exception.QpidTestException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.CRLException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PEMUtils
{
    private static final byte[] LINE_SEPARATOR = new byte[]{'\r', '\n'};
    private static final String BEGIN_X_509_CRL = "-----BEGIN X509 CRL-----";
    private static final String END_X_509_CRL = "-----END X509 CRL-----";
    private static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----";
    private static final String END_PRIVATE_KEY = "-----END PRIVATE KEY-----";
    private static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERTIFICATE = "-----END CERTIFICATE-----";
    private static final int PEM_LINE_LENGTH = 76;

    public static void saveCertificateAsPem(final Path path, final List<X509Certificate> certificates)
    {
        try
        {
            final StringBuilder stringBuilder = new StringBuilder();
            for (final X509Certificate x509Certificate : certificates)
            {
                stringBuilder.append(toPEM(x509Certificate));
            }
            Files.writeString(path, stringBuilder.toString(), UTF_8);
        }
        catch (final IOException e)
        {
            throw new QpidTestException(e);
        }
    }

    public static void savePrivateKeyAsPem(final Path path, final PrivateKey key)
    {
        try
        {
            Files.writeString(path, toPEM(key), UTF_8);
        }
        catch (final IOException e)
        {
            throw new QpidTestException(e);
        }
    }

    public static void saveCrlAsPem(final Path path, final X509CRL crl)
    {
        try
        {
            Files.writeString(path, toPEM(crl.getEncoded(), BEGIN_X_509_CRL, END_X_509_CRL), UTF_8);
        }
        catch (IOException | CRLException e)
        {
            throw new QpidTestException(e);
        }
    }

    public static String toPEM(final Certificate certificate)
    {
        try
        {
            return toPEM(certificate.getEncoded(), BEGIN_CERTIFICATE, END_CERTIFICATE);
        }
        catch (final CertificateEncodingException e)
        {
            throw new QpidTestException(e);
        }
    }

    public static String toPEM(final PrivateKey key)
    {
        return toPEM(key.getEncoded(), BEGIN_PRIVATE_KEY, END_PRIVATE_KEY);
    }

    private static String toPEM(final byte[] bytes, final String header, final String footer)
    {
        return header + new String(LINE_SEPARATOR, UTF_8) +
                Base64.getMimeEncoder(PEM_LINE_LENGTH, LINE_SEPARATOR).encodeToString(bytes) +
                new String(LINE_SEPARATOR, UTF_8) + footer + new String(LINE_SEPARATOR, UTF_8);
    }
}
