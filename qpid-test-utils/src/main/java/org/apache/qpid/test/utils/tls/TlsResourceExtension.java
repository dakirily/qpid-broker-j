package org.apache.qpid.test.utils.tls;

import org.apache.qpid.test.utils.tls.utils.TlsResource;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class TlsResourceExtension implements ParameterResolver
{
    @Override
    public boolean supportsParameter(final ParameterContext parameterContext,
                                     final ExtensionContext extensionContext) throws ParameterResolutionException
    {
        return TlsResource.class.isAssignableFrom(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext,
                                   final ExtensionContext extensionContext) throws ParameterResolutionException
    {
        final var store = store(extensionContext);
        return store.computeIfAbsent(TlsResource.class, key -> new TlsResource(), TlsResource.class);
    }

    private ExtensionContext.Store store(final ExtensionContext context)
    {
        final var testClass = context.getRequiredTestClass();
        return context.getStore(ExtensionContext.Namespace.create(testClass));
    }
}
