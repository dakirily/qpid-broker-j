package org.apache.qpid.test.utils.tls;

import java.lang.reflect.Executable;
import java.util.Optional;

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * JUnit extension allowing to inject {@link TlsResource} into test lifecycle methods and test methods.
 * To enable extension on test class add annotation on the class level:
 * <pre> {@code @ExtendWith({ TlsResourceExtension.class }) } </pre>
 * To inject {@link TlsResource} into the test method pass it as a method parameter:
 * <pre> {@code
 * void test(final TlsResource tls)
 * {
 *
 * } } </pre>
 * A new {@link TlsResource} instance is created lazily per test invocation and will be closed after the invocation completes.
 * When injected into a {@link org.junit.jupiter.api.BeforeEach} method, the same instance is reused for the corresponding test invocation.
 * When injected into a {@link BeforeAll} method, a single instance is created per test class and will be closed after
 * all test methods in that class complete.
 */
public class TlsResourceExtension implements ParameterResolver,
                                              AfterAllCallback,
                                              AfterEachCallback
{
    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(TlsResourceExtension.class);

    private enum Scope
    {
        CLASS,
        INVOCATION
    }

    private record StoreKey(Scope scope, String id) { }

    /**
     * Returns true when test method argument is of type {@link TlsResource}
     * @param parameterContext the context for the parameter for which an argument should
     * be resolved; never {@code null}
     * @param extensionContext the extension context for the {@code Executable}
     * about to be invoked; never {@code null}
     * @return true when method parameter is a {@link TlsResource} instance, false otherwise
     * @throws ParameterResolutionException When failed to resolve method parameter
     */
    @Override
    public boolean supportsParameter(final @NonNull ParameterContext parameterContext,
                                     final @NonNull ExtensionContext extensionContext) throws ParameterResolutionException
    {
        return TlsResource.class.equals(parameterContext.getParameter().getType());
    }

    /**
     * Closes class-scoped resource after all tests in the class.
     */
    @Override
    public void afterAll(final ExtensionContext extensionContext)
    {
        closeAndRemove(Scope.CLASS, extensionContext);
    }

    /**
     * Closes invocation-scoped resource after each test invocation.
     */
    @Override
    public void afterEach(final ExtensionContext extensionContext)
    {
        closeAndRemove(Scope.INVOCATION, extensionContext);
    }

    /**
     * Resolves {@link TlsResource} for the appropriate lifecycle scope.
     * Retrieves {@link TlsResource} from the {@link ExtensionContext.Store} or creates it when absent.
     * @param parameterContext {@link ParameterContext} instance
     * @param extensionContext {@link ExtensionContext} instance
     * @return {@link TlsResource} instance
     * @throws ParameterResolutionException When failed to resolve method parameter
     */
    @Override
    public Object resolveParameter(final @NonNull ParameterContext parameterContext,
                                   final @NonNull ExtensionContext extensionContext) throws ParameterResolutionException
    {
        final Scope scope = resolveScope(parameterContext.getDeclaringExecutable());
        final StoreKey key = storeKey(scope, extensionContext)
                .orElseThrow(() -> new ParameterResolutionException("Unable to resolve TlsResource scope"));
        return store(extensionContext).computeIfAbsent(key, k -> new TlsResource(), TlsResource.class);
    }

    /**
     * Returns {@link ExtensionContext.Store} for extension-level storage
     * @param extensionContext {@link ExtensionContext} instance
     * @return {@link ExtensionContext.Store} instance
     */
    private ExtensionContext.Store store(final @NonNull ExtensionContext extensionContext)
    {
        return extensionContext.getRoot().getStore(NAMESPACE);
    }

    /**
     * Determines resource scope based on the declaring executable.
     */
    private Scope resolveScope(final Executable executable)
    {
        if (executable.isAnnotationPresent(BeforeAll.class) || executable.isAnnotationPresent(AfterAll.class))
        {
            return Scope.CLASS;
        }
        return Scope.INVOCATION;
    }

    /**
     * Closes and removes the {@link TlsResource} for the given scope if present.
     */
    private void closeAndRemove(final Scope scope, final ExtensionContext extensionContext)
    {
        final var key = storeKey(scope, extensionContext);
        if (key.isEmpty())
        {
            return;
        }
        final var resource = store(extensionContext).remove(key.get(), TlsResource.class);
        if (resource != null)
        {
            resource.close();
        }
    }

    /**
     * Builds the store key for the scope based on the relevant context.
     */
    private Optional<StoreKey> storeKey(final Scope scope, final ExtensionContext extensionContext)
    {
        final Optional<ExtensionContext> keyContext = switch (scope)
        {
            case CLASS -> findTestClassContext(extensionContext);
            case INVOCATION -> findTestInvocationContext(extensionContext);
        };
        return keyContext.map(context -> new StoreKey(scope, context.getUniqueId()));
    }

    /**
     * Finds the nearest test invocation context in the hierarchy.
     */
    private Optional<ExtensionContext> findTestInvocationContext(final ExtensionContext extensionContext)
    {
        ExtensionContext current = extensionContext;
        while (true)
        {
            if (current.getTestMethod().isPresent())
            {
                return Optional.of(current);
            }
            final var parent = current.getParent();
            if (parent.isEmpty())
            {
                return Optional.empty();
            }
            current = parent.get();
        }
    }

    /**
     * Finds the test class context in the hierarchy.
     */
    private Optional<ExtensionContext> findTestClassContext(final ExtensionContext extensionContext)
    {
        ExtensionContext current = extensionContext;
        while (true)
        {
            if (current.getTestClass().isPresent() && current.getTestMethod().isEmpty())
            {
                return Optional.of(current);
            }
            final var parent = current.getParent();
            if (parent.isEmpty())
            {
                return Optional.empty();
            }
            current = parent.get();
        }
    }
}
