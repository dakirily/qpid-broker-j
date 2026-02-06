package org.apache.qpid.test.utils.exception;

import java.util.Optional;

public class QpidTestException extends RuntimeException
{
    public QpidTestException()
    {
        // no-op
    }

    public QpidTestException(final String message)
    {
        super(message);
    }

    public QpidTestException(final Throwable cause)
    {
        super(Optional.ofNullable(cause).map(ex -> ex.getClass().getCanonicalName() + ": " +
                ex.getMessage()).orElse(null), cause);
    }

    public QpidTestException(final String message, final Throwable cause)
    {
        super(message + ": " + Optional.ofNullable(cause).map(ex -> ex.getClass().getCanonicalName() + ": " +
                ex.getMessage()).orElse(null), cause);
    }
}
