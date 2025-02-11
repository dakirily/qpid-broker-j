/*
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

public final class Symbols
{
    private Symbols()
    {

    }

    public static final Symbol AMQP_ACCEPTED = Symbol.valueOf("amqp:accepted:list");
    public static final Symbol AMQP_APPLICATION_PROPERTIES = Symbol.valueOf("amqp:application-properties:map");
    public static final Symbol AMQP_CONN_ESTABLISHMENT_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
    public static final Symbol AMQP_CONN_FORCED = Symbol.valueOf("amqp:connection:forced");
    public static final Symbol AMQP_CONN_FRAMING_ERROR = Symbol.valueOf("amqp:connection:framing-error");
    public static final Symbol AMQP_CONN_REDIRECT = Symbol.valueOf("amqp:connection:redirect");
    public static final Symbol AMQP_CONN_SOCKET_ERROR = Symbol.valueOf("amqp:connection:socket-error");
    public static final Symbol AMQP_ERR_DECODE = Symbol.valueOf("amqp:decode-error");
    public static final Symbol AMQP_ERR_FRAME_SIZE_TOO_SMALL = Symbol.valueOf("amqp:frame-size-too-small");
    public static final Symbol AMQP_ERR_ILLEGAL_STATE = Symbol.valueOf("amqp:illegal-state");
    public static final Symbol AMQP_ERR_INTERNAL = Symbol.valueOf("amqp:internal-error");
    public static final Symbol AMQP_ERR_INVALID_FIELD = Symbol.valueOf("amqp:invalid-field");
    public static final Symbol AMQP_ERR_NOT_ALLOWED = Symbol.valueOf("amqp:not-allowed");
    public static final Symbol AMQP_ERR_NOT_AUTHORIZED = Symbol.valueOf("amqp:unauthorized-access");
    public static final Symbol AMQP_ERR_NOT_FOUND = Symbol.valueOf("amqp:not-found");
    public static final Symbol AMQP_ERR_NOT_IMPLEMENTED = Symbol.valueOf("amqp:not-implemented");
    public static final Symbol AMQP_ERR_PRECONDITION_FAILED = Symbol.valueOf("amqp:precondition-failed");
    public static final Symbol AMQP_ERR_RESOURCE_DELETED = Symbol.valueOf("amqp:resource-deleted");
    public static final Symbol AMQP_ERR_RESOURCE_LIMIT_EXCEEDED = Symbol.valueOf("amqp:resource-limit-exceeded");
    public static final Symbol AMQP_ERR_RESOURCE_LOCKED = Symbol.valueOf("amqp:resource-locked");
    public static final Symbol AMQP_DATA = Symbol.valueOf("amqp:data:binary");
    public static final Symbol AMQP_DECLARED = Symbol.valueOf("amqp:declared:list");
    public static final Symbol AMQP_DELIVERY_ANNOTATIONS = Symbol.valueOf("amqp:delivery-annotations:map");
    public static final Symbol AMQP_DISTRIBUTED_TXN = Symbol.valueOf("amqp:distributed-transactions");
    public static final Symbol AMQP_FOOTER = Symbol.valueOf("amqp:footer:map");
    public static final Symbol AMQP_HEADER = Symbol.valueOf("amqp:header:list");
    public static final Symbol AMQP_LINK_DETACH_FORCED = Symbol.valueOf("amqp:link:detach-forced");
    public static final Symbol AMQP_LINK_MSG_SIZE_EXCEEDED = Symbol.valueOf("amqp:link:message-size-exceeded");
    public static final Symbol AMQP_LINK_REDIRECT = Symbol.valueOf("amqp:link:redirect");
    public static final Symbol AMQP_LINK_STOLEN = Symbol.valueOf("amqp:link:stolen");
    public static final Symbol AMQP_LINK_TRANSFER_LIMIT_EXCEEDED = Symbol.valueOf("amqp:link:transfer-limit-exceeded");
    public static final Symbol AMQP_LOCAL_TXN = Symbol.valueOf("amqp:local-transactions");
    public static final Symbol AMQP_MESSAGE_ANNOTATIONS = Symbol.valueOf("amqp:message-annotations:map");
    public static final Symbol AMQP_MODIFIED = Symbol.valueOf("amqp:modified:list");
    public static final Symbol AMQP_MULTI_SESSIONS_PER_TXN = Symbol.valueOf("amqp:multi-ssns-per-txn");
    public static final Symbol AMQP_MULTI_TXN_PER_SESSION = Symbol.valueOf("amqp:multi-txns-per-ssn");
    public static final Symbol AMQP_PROMOTABLE_TXN = Symbol.valueOf("amqp:promotable-transactions");
    public static final Symbol AMQP_PROPERTIES = Symbol.valueOf("amqp:properties:list");
    public static final Symbol AMQP_REJECTED = Symbol.valueOf("amqp:rejected:list");
    public static final Symbol AMQP_RELEASED = Symbol.valueOf("amqp:released:list");
    public static final Symbol AMQP_SEQUENCE = Symbol.valueOf("amqp:amqp-sequence:list");
    public static final Symbol AMQP_SESSION_ERRANT_LINK = Symbol.valueOf("amqp:session:errant-link");
    public static final Symbol AMQP_SESSION_HANDLE_IN_USE = Symbol.valueOf("amqp:session:handle-in-use");
    public static final Symbol AMQP_SESSION_UNATTACHED_HANDLE = Symbol.valueOf("amqp:session:unattached-handle");
    public static final Symbol AMQP_SESSION_WINDOW_VIOLATION = Symbol.valueOf("amqp:session:window-violation");
    public static final Symbol AMQP_TXN_ROLLBACK = Symbol.valueOf("amqp:transaction:rollback");
    public static final Symbol AMQP_TXN_TIMEOUT = Symbol.valueOf("amqp:transaction:timeout");
    public static final Symbol AMQP_TXN_UNKNOWN_ID = Symbol.valueOf("amqp:transaction:unknown-id");
    public static final Symbol AMQP_VALUE = Symbol.valueOf("amqp:amqp-value:*");
    public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
    public static final Symbol ANNOTATION_KEY = Symbol.valueOf("x-opt-jms-msg-type");
    public static final Symbol APACHE_LEGACY_DIRECT_BINDING = Symbol.valueOf("apache.org:legacy-amqp-direct-binding:string");
    public static final Symbol APACHE_LEGACY_NO_LOCAL_FILTER = Symbol.valueOf("apache.org:jms-no-local-filter:list");
    public static final Symbol APACHE_LEGACY_SELECTOR_FILTER = Symbol.valueOf("apache.org:jms-selector-filter:string");
    public static final Symbol APACHE_LEGACY_TOPIC_BINDING = Symbol.valueOf("apache.org:legacy-amqp-topic-binding:string");
    public static final Symbol APACHE_NO_LOCAL_FILTER = Symbol.valueOf("apache.org:no-local-filter:list");
    public static final Symbol APACHE_SELECTOR_FILTER = Symbol.valueOf("apache.org:selector-filter:string");
    public static final Symbol CONNECTION_CLOSE = Symbol.valueOf("connection-close");
    public static final Symbol CONTAINER_ID = Symbol.valueOf("container-id");
    public static final Symbol COPY = Symbol.valueOf("copy");
    public static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");
    public static final Symbol DELIVERY_TAG = Symbol.valueOf("delivery-tag");
    public static final Symbol DELIVERY_TIME = Symbol.valueOf("x-opt-delivery-time");
    public static final Symbol DISCARD_UNROUTABLE = Symbol.valueOf("DISCARD_UNROUTABLE");
    public static final Symbol FIELD = Symbol.valueOf("field");
    public static final Symbol FILTER = Symbol.valueOf("filter");
    public static final Symbol GLOBAL_CAPABILITY = Symbol.getSymbol("global");
    public static final Symbol INVALID_FIELD = Symbol.valueOf("invalid-field");
    public static final Symbol LIFETIME_POLICY = Symbol.valueOf("lifetime-policy");
    public static final Symbol LINK_DETACH = Symbol.valueOf("link-detach");
    public static final Symbol MOVE = Symbol.valueOf("move");
    public static final Symbol NETWORK_HOST = Symbol.valueOf("network-host");
    public static final Symbol NEVER = Symbol.valueOf("never");
    public static final Symbol NOT_VALID_BEFORE = Symbol.valueOf("x-qpid-not-valid-before");
    public static final Symbol PRIORITY = Symbol.valueOf("priority");
    public static final Symbol PORT = Symbol.valueOf("port");
    public static final Symbol PRODUCT = Symbol.valueOf("product");
    public static final Symbol REJECT_UNROUTABLE = Symbol.valueOf("REJECT_UNROUTABLE");
    public static final Symbol SHARED_CAPABILITY = Symbol.getSymbol("shared");
    public static final Symbol SESSION_END = Symbol.valueOf("session-end");
    public static final Symbol SHARED_SUBSCRIPTIONS = Symbol.valueOf("SHARED-SUBS");
    public static final Symbol SOLE_CONNECTION_ENFORCEMENT = Symbol.valueOf("sole-connection-enforcement");
    public static final Symbol SOLE_CONNECTION_ENFORCEMENT_POLICY = Symbol.valueOf("sole-connection-enforcement-policy");
    public static final Symbol SOLE_CONNECTION_DETECTION_POLICY = Symbol.valueOf("sole-connection-detection-policy");
    public static final Symbol SOLE_CONNECTION_FOR_CONTAINER = Symbol.valueOf("sole-connection-for-container");
    public static final Symbol SUPPORTED_DIST_MODES = Symbol.valueOf("supported-dist-modes");
    public static final Symbol TEMPORARY_QUEUE = Symbol.valueOf("temporary-queue");
    public static final Symbol TEMPORARY_TOPIC = Symbol.valueOf("temporary-topic");
    public static final Symbol TOPIC = Symbol.valueOf("topic");
    public static final Symbol TXN_ID = Symbol.valueOf("txn-id");
    public static final Symbol VERSION = Symbol.valueOf("version");
}
