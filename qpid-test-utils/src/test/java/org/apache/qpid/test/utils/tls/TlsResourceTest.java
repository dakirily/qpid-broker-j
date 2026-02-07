package org.apache.qpid.test.utils.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

import org.junit.jupiter.api.Test;

class TlsResourceTest
{
    @Test
    void secretIsClearedOnClose()
    {
        final TlsResource tls = new TlsResource("private", "cert", "secret", KeyStore.getDefaultType());
        try
        {
            final char[] before = tls.getSecretAsCharacters();
            assertEquals("secret", new String(before));
        }
        finally
        {
            tls.close();
        }

        final char[] after = tls.getSecretAsCharacters();
        for (char value : after)
        {
            assertEquals('\0', value);
        }
    }

    @Test
    void deletesTempDirectoryOnClose() throws Exception
    {
        final TlsResource tls = new TlsResource();
        Path directory = null;
        try
        {
            final Path file = tls.createFile(".tmp");
            directory = file.getParent();
            assertTrue(Files.exists(directory));
        }
        finally
        {
            tls.close();
        }

        assertNotNull(directory);
        assertFalse(Files.exists(directory));
    }
}
