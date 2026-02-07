# qpid-test-utils

## TlsResourceExtension

`TlsResourceExtension` injects a `TlsResource` into JUnit lifecycle methods and test methods.
The extension creates a `TlsResource` lazily (only when injected) and ensures it is closed correctly.

## TlsResource

`TlsResource` is a test utility for creating and storing TLS-related artifacts. It can generate key/trust stores,
self-signed certificates, CRLs, and PEM/DER files under a temporary directory that is deleted on `close()`.
Use it directly with try-with-resources, or inject it via `TlsResourceExtension`.

### Common operations

- Create a self-signed key store or trust store
- Save private keys or certificates as PEM/DER files
- Create CRLs and convert them to data URLs

### Example: try-with-resources

```java
try (TlsResource tls = new TlsResource()) {
    Path keyStore = tls.createSelfSignedKeyStore("CN=localhost");
    Path trustStore = tls.createSelfSignedTrustStore("CN=localhost");
    Path privateKeyPem = tls.savePrivateKeyAsPem(
            TlsResourceBuilder.createRSAKeyPair().getPrivate());
}
```

### Lifecycle rules

- `@BeforeAll` receives one shared `TlsResource` per test class and it is closed after `@AfterAll`.
- `@BeforeEach` and `@Test` receive one `TlsResource` per test invocation; it is closed after the test completes.
- Within one invocation, `@BeforeEach` and the corresponding `@Test` use the same instance.

### JUnit configuration note

JUnit has a configuration parameter `junit.jupiter.extensions.store.close.autocloseable.enabled` that controls
whether `AutoCloseable` values stored in an `ExtensionContext.Store` are closed automatically. If this is disabled,
extensions that rely on that behavior can leak resources. `TlsResourceExtension` does not rely on this parameter and
closes resources explicitly, so it remains safe even if the parameter is set to `false`. Keep this in mind if you
implement other extensions that store `AutoCloseable` values in the JUnit store.

### Example: injection into a test method

```java
@ExtendWith({ TlsResourceExtension.class })
class MyTest {
    @Test
    void testTls(final TlsResource tls) throws Exception {
        final Path ks = tls.createSelfSignedKeyStore("CN=localhost");
        // use ks ...
    }
}
```

### Example: injection into @BeforeEach

```java
@ExtendWith({ TlsResourceExtension.class })
class MyTest {
    private Path trustStore;

    @BeforeEach
    void setUp(final TlsResource tls) throws Exception {
        trustStore = tls.createSelfSignedTrustStore("CN=localhost");
    }

    @Test
    void testUsesSameInstance(final TlsResource tls) throws Exception {
        // same tls instance as in @BeforeEach for this invocation
    }
}
```

### Example: injection into @BeforeAll

```java
@ExtendWith({ TlsResourceExtension.class })
class MyTest {
    private static Path keyStore;

    @BeforeAll
    static void setUpClass(final TlsResource tls) throws Exception {
        keyStore = tls.createSelfSignedKeyStore("CN=localhost");
    }

    @Test
    void testUsesClassResource() {
        // keyStore is created once per class
    }
}
```
