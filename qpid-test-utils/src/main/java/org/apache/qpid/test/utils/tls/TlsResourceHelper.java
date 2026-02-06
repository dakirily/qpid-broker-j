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

import org.apache.qpid.test.utils.exception.QpidTestException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

public class TlsResourceHelper
{
    public static KeyStore createKeyStore(final String keyStoreType, final char[] secret, final KeyStoreEntry... entries)
    {
        final KeyStore keyStore = createKeyStoreOfType(keyStoreType);
        for (KeyStoreEntry keyStoreEntry : entries)
        {
            keyStoreEntry.addToKeyStore(keyStore, secret);
        }
        return keyStore;
    }

    public static String createKeyStoreAsDataUrl(final String keyStoreType,
                                                 final char[] secret,
                                                 final KeyStoreEntry... entries)
    {
        final KeyStore keyStore = createKeyStore(keyStoreType, secret, entries);
        return toDataUrl(keyStore, secret);
    }

    public static KeyStore createKeyStoreOfType(final String keyStoreType)
    {
        try
        {
            final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
            keyStore.load(null, null);
            return keyStore;
        }
        catch (final GeneralSecurityException | IOException e)
        {
            throw new QpidTestException(e);
        }
    }

    private static byte[] keyStoreToBytes(final KeyStore keyStore, final char[] secret)
    {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream())
        {
            keyStore.store(out, secret);
            return out.toByteArray();
        }
        catch (GeneralSecurityException | IOException e)
        {
            throw new QpidTestException(e);
        }
    }

    public static void saveKeyStoreIntoFile(final KeyStore keyStore, final char[] secret, final Path storePath)
    {
        try
        {
            Files.write(storePath, keyStoreToBytes(keyStore, secret));
        }
        catch (IOException e)
        {
            throw new QpidTestException(e);
        }
    }

    public static String toDataUrl(final KeyStore keyStore, final char[] secret)
    {
        return getDataUrlForBytes(keyStoreToBytes(keyStore, secret));
    }

    public static String getDataUrlForBytes(final byte[] bytes)
    {
        return "data:;base64," + Base64.getEncoder().encodeToString(bytes);
    }

    public static SecretKey createAESSecretKey()
    {
        try
        {
            final KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256);
            return keyGen.generateKey();
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new QpidTestException(e);
        }
    }
}
