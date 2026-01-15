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

package org.apache.qpid.server.protocol.v1_0.type.transport;

import java.util.Map;

import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;

@CompositeType( symbolicDescriptor = "amqp:error:list", numericDescriptor = 0x000000000000001DL)
public class Error
{
    @CompositeTypeField(index = 0, mandatory = true,
            deserializationConverter = "org.apache.qpid.server.protocol.v1_0.DeserializationFactories.convertToErrorCondition")
    private ErrorCondition _condition;

    @CompositeTypeField(index = 1)
    private String _description;

    @CompositeTypeField(index = 2)
    private Map<Symbol, Object> _info;

    public Error()
    {
    }

    public Error(final ErrorCondition condition, final String description)
    {
        _condition = condition;
        _description = description;
    }

    public static class Amqp
    {
        public static Error decode(final String fieldName, final String typeName)
        {
            final String description = "Could not decode field '%s' of '%s'".formatted(fieldName, typeName);
            return new Error(AmqpError.DECODE_ERROR, description);
        }

        public static Error frameSizeTooSmall()
        {
            final String description = "Cannot fit a single unsettled delivery into Attach frame.";
            return new Error(AmqpError.FRAME_SIZE_TOO_SMALL, description);
        }

        public static Error illegalState(final String description)
        {
            return new Error(AmqpError.ILLEGAL_STATE, description);
        }

        public static Error internalError(final String description)
        {
            return new Error(AmqpError.INTERNAL_ERROR, description);
        }

        public static Error invalidField(final String description)
        {
            return new Error(AmqpError.INVALID_FIELD, description);
        }

        public static Error linkNotFound()
        {
            return new Error(AmqpError.NOT_FOUND, "Link not found");
        }

        public static Error linkNotFound(final String linkName)
        {
            final String description = "Link '%s' not found".formatted(linkName);
            return new Error(AmqpError.NOT_FOUND, description);
        }

        public static Error notAllowed(final String description)
        {
            return new Error(AmqpError.NOT_ALLOWED, description);
        }

        public static Error notFound(final String description)
        {
            return new Error(AmqpError.NOT_FOUND, description);
        }

        public static Error notImplemented(final String description)
        {
            return new Error(AmqpError.NOT_IMPLEMENTED, description);
        }

        public static Error preconditionFailed(final String description)
        {
            return new Error(AmqpError.PRECONDITION_FAILED, description);
        }

        public static Error resourceDeleted(final String destination)
        {
            final String description = "Destination '%s' has been removed.".formatted(destination);
            return new Error(AmqpError.RESOURCE_DELETED, description);
        }

        public static Error resourceLimitExceeded(final String endpoint, final String session)
        {
            final String description = "Cannot find free handle for endpoint '%s' on session '%s'".formatted(endpoint, session);
            return new Error(AmqpError.RESOURCE_LIMIT_EXCEEDED, description);
        }

        public static Error resourceLocked(final String description)
        {
            return new Error(AmqpError.RESOURCE_LOCKED, description);
        }
    }

    public static class Connection
    {
        public static Error channelLargerThanMax(final int channel, final int channelMax)
        {
            final String description = "specified channel %d larger than maximum channel %d".formatted(channel, channelMax);
            return new Error(ConnectionError.FRAMING_ERROR, description);
        }

        public static Error unknownChannel(final int channel)
        {
            final String description = "Frame received on channel %d which is not known as a begun session.".formatted(channel);
            return new Error(ConnectionError.FRAMING_ERROR, description);
        }
    }

    public static class Link
    {
        public static Error detachForced()
        {
            final String description = "Force detach the link because the session is remotely ended.";
            return new Error(LinkError.DETACH_FORCED, description);
        }

        public static Error messageSizeExceeded(final Binary deliveryTag, final Long maxMessageSize)
        {
            final String description = "delivery '%s' exceeds max-message-size %d".formatted(deliveryTag, maxMessageSize);
            return new Error(LinkError.MESSAGE_SIZE_EXCEEDED, description);
        }

        public static Error stolen(final String connection)
        {
            final String description = "Link is being stolen by connection '%s'".formatted(connection);
            return new Error(LinkError.STOLEN, description);
        }
    }

    public static class Session
    {
        public static Error errantLink(final UnsignedInteger handle)
        {
            final String description = "Received TRANSFER for link handle %s which is in errored state.".formatted(handle);
            return new Error(SessionError.ERRANT_LINK, description);
        }

        public static Error handleInUse(final int handle)
        {
            final String description = "Input Handle '%d' already in use".formatted(handle);
            return new Error(SessionError.HANDLE_IN_USE, description);
        }

        public static Error unattachedFlowHandle(final int handle)
        {
            final String description = "Received Flow with unknown handle %d".formatted(handle);
            return new Error(SessionError.UNATTACHED_HANDLE, description);
        }

        public static Error unattachedTransferHandle(final int handle)
        {
            final String description = "TRANSFER called on Session for link handle %s which is not attached.".formatted(handle);
            return new Error(SessionError.UNATTACHED_HANDLE, description);
        }

        public static Error windowViolation(final long nextIncomingId, final long nextOutgoingId)
        {
            final String description = "Next incoming id '%d' exceeds next outgoing id '%d'".formatted(nextIncomingId, nextOutgoingId);
            return new Error(SessionError.WINDOW_VIOLATION, description);
        }
    }

    public static class Transaction
    {
        public static Error txRollback()
        {
            final String description = "The transaction was rolled back due to an earlier issue (e.g. a published " +
                    "message was sent settled but could not be enqueued)";
            return new Error(TransactionError.TRANSACTION_ROLLBACK, description);
        }

        public static Error txTimeout(final String description)
        {
            return new Error(TransactionError.TRANSACTION_TIMEOUT, description);
        }

        public static Error unknownId(final Binary txnId)
        {
            final String description = "Unknown transaction-id '%s'.".formatted(txnId);
            return new Error(TransactionError.UNKNOWN_ID, description);
        }
    }

    public ErrorCondition getCondition()
    {
        return _condition;
    }

    public void setCondition(ErrorCondition condition)
    {
        _condition = condition;
    }

    public String getDescription()
    {
        return _description;
    }

    public void setDescription(String description)
    {
        _description = description;
    }

    public Map<Symbol, Object> getInfo()
    {
        return _info;
    }

    public void setInfo(Map<Symbol, Object> info)
    {
        _info = info;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Error{");
        final int origLength = builder.length();

        if (_condition != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("condition=").append(_condition);
        }

        if (_description != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("description=").append(_description);
        }

        if (_info != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("info=").append(_info);
        }

        builder.append('}');
        return builder.toString();
    }
}
