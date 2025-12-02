package org.apache.qpid.test.utils.exception;

public class QpidTestException extends RuntimeException
{
    public QpidTestException()
    {

    }

    public QpidTestException(final String message)
    {
        super(message);
    }

    public QpidTestException(final Throwable cause)
    {
        super(cause);
    }

    public QpidTestException(final String message, final Throwable cause)
    {
        super(message, cause);
    }
}
