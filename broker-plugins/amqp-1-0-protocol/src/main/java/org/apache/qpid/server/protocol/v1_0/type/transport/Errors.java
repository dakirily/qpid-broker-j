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

import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionError;

public final class Errors
{
    private Errors()
    {

    }

    public static class IllegalState
    {
        public static final Error CANNOT_ACCEPT_DELIVERIES_INCOMPLETE_UNSETTLED_TRUE =
                new Error(AmqpError.ILLEGAL_STATE, "Cannot accept new deliveries while incomplete-unsettled is true.");
        public static final Function<Binary, Error> DELIVERY_TAG_USED_BY_ANOTHER_UNSETTLED_DELIVERY =
                deliveryTag -> new Error(AmqpError.ILLEGAL_STATE,
                ("Delivery-tag '%s' is used by another unsettled delivery." +
                 " The delivery-tag MUST be unique amongst all deliveries that" +
                 " could be considered unsettled by either end of the link.").formatted(deliveryTag));
    }

    public static class InvalidField
    {
        public static final Error DELIVERY_ID_REQUIRED =
                new Error(AmqpError.INVALID_FIELD, "Transfer \"delivery-id\" is required for a new delivery.");
        public static final Error DELIVERY_TAG_REQUIRED =
                new Error(AmqpError.INVALID_FIELD, "Transfer \"delivery-tag\" is required for a new delivery.");
        public static final Error MSG_FMT_SET_TO_DIFFERENT_VALUE =
                new Error(AmqpError.INVALID_FIELD,
                "Transfer \"message-format\" is set to different value than on previous transfer.");
        public static final Error TFR_RCV_SETTLE_MODE_CANNOT_BE_FIRST =
                new Error(AmqpError.INVALID_FIELD, "Transfer \"rcv-settle-mode\" cannot be \"first\" when link \"rcv-settle-mode\" is set to \"second\".");
        public static final Error TFR_RCV_SETTLE_MODE_SET_TO_DIFFERENT_VALUE =
                new Error(AmqpError.INVALID_FIELD, "Transfer \"rcv-settle-mode\" is set to different value than on previous transfer.");
    }

    public static class Link
    {
        public static final BiFunction<Binary, Long, Error> MAX_MSG_SIZE_EXCEEDED =
                (deliveryTag, maxMessageSize) ->
                new Error(LinkError.MESSAGE_SIZE_EXCEEDED, "delivery '%s' exceeds max-message-size %d".formatted(deliveryTag, maxMessageSize));
    }

    public static class Session
    {
        public static final Function<UnsignedInteger, Error> LINK_HANDLE_IN_ERRORED_STATE =
                handle -> new Error(SessionError.ERRANT_LINK, "Received TRANSFER for link handle %s which is in errored state.".formatted(handle));
    }

    public static class Txn
    {
        public static final Function<Binary, Error> UNKNOWN_TXN_ID =
                txnId -> new Error(TransactionError.UNKNOWN_ID,
                  "Transfer has an unknown transaction-id '%s'.".formatted(txnId));
    }
}
