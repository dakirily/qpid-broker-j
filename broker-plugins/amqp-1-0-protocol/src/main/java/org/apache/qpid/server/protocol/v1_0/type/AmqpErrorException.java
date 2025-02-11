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
package org.apache.qpid.server.protocol.v1_0.type;

import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;

import java.util.Formatter;

public class AmqpErrorException extends Exception
{
    private final Error _error;

    public AmqpErrorException(final Error error)
    {
        _error = error;
    }

    public AmqpErrorException(final Error error, final Throwable cause)
    {
        super(cause);
        _error = error;
    }

    public AmqpErrorException(ErrorCondition condition)
    {
        _error = new Error();
        _error.setCondition(condition);
    }

    public AmqpErrorException(ErrorCondition condition, String format, Object... args)
    {
        _error = new Error();
        _error.setCondition(condition);
        _error.setDescription((new Formatter()).format(format, args).toString());
    }

    public Error getError()
    {
        return _error;
    }

    @Override
    public String toString()
    {
        return "AmqpErrorException{error=" + _error + '}';
    }

    public static AmqpErrorExceptionBuilder decode()
    {
        return new AmqpErrorExceptionBuilder().condition(AmqpError.DECODE_ERROR);
    }

    public static AmqpErrorExceptionBuilder framing()
    {
        return new AmqpErrorExceptionBuilder().condition(ConnectionError.FRAMING_ERROR);
    }

    public static AmqpErrorExceptionBuilder of(final Error error)
    {
        return new AmqpErrorExceptionBuilder().error(error);
    }

    public static AmqpErrorExceptionBuilder of(final ErrorCondition condition)
    {
        return new AmqpErrorExceptionBuilder().condition(condition);
    }

    public static class AmqpErrorExceptionBuilder
    {
        Error error;
        ErrorCondition condition;
        String message;
        Object[] args;

        AmqpErrorExceptionBuilder error(final Error error)
        {
            this.error = error;
            return this;
        }

        AmqpErrorExceptionBuilder condition(final ErrorCondition condition)
        {
            this.condition = condition;
            return this;
        }

        public AmqpErrorExceptionBuilder message(final String message)
        {
            this.message = message;
            return this;
        }

        public AmqpErrorException args(final Object... args)
        {
            this.args = args;
            return new AmqpErrorException(condition, message, args);
        }

        public AmqpErrorException build()
        {
            return new AmqpErrorException(condition, message);
        }
    }
}
