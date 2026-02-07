package org.apache.qpid.test.utils.tls;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
@ExtendWith({ TlsResourceExtensionBeforeAllTest.ClassCleanupVerifier.class, TlsResourceExtension.class })
class TlsResourceExtensionBeforeAllTest
{
    private static TlsResource _beforeAllResource;
    private static Path _classDirectory;

    @BeforeAll
    static void setUpClass(final TlsResource tls) throws Exception
    {
        _beforeAllResource = tls;
        final Path file = tls.createFile(".tmp");
        _classDirectory = file.getParent();
        assertTrue(Files.exists(_classDirectory));
    }

    @AfterAll
    static void tearDownClass(final TlsResource tls)
    {
        assertSame(_beforeAllResource, tls);
    }

    @Test
    void testMethodUsesDifferentInstance(final TlsResource tls)
    {
        assertNotSame(_beforeAllResource, tls);
    }

    static class ClassCleanupVerifier implements AfterAllCallback
    {
        @Override
        public void afterAll(final ExtensionContext context) throws Exception
        {
            if (_classDirectory != null)
            {
                assertFalse(Files.exists(_classDirectory),
                        "Expected TLS resource directory to be deleted: " + _classDirectory);
            }
        }
    }
}
