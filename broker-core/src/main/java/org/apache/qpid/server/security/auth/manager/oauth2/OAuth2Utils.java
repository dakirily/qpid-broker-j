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
package org.apache.qpid.server.security.auth.manager.oauth2;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URLEncoder;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.util.HttpClientTransport;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public final class OAuth2Utils
{
    private OAuth2Utils()
    {
    }

    public static String buildRequestQuery(final Map<String, String> requestBodyParameters)
    {
        final StringJoiner query = new StringJoiner("&");
        for (final Map.Entry<String, String> entry : requestBodyParameters.entrySet())
        {
            query.add(formEncode(entry.getKey()) + "=" + formEncode(entry.getValue()));
        }
        return query.toString();
    }

    public static String buildBasicAuthorization(final String clientId, final String clientSecret)
    {
        final String encodedClientId = formEncode(Objects.requireNonNull(clientId, "clientId"));
        final String encodedClientSecret = formEncode(clientSecret == null ? "" : clientSecret);
        final String credentials = encodedClientId + ":" + encodedClientSecret;
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(UTF_8));
    }

    public static HttpClientTransport createHttpClientTransport(
            final OAuth2AuthenticationProvider<?> authenticationProvider)
    {
        final HttpClientTransport.Builder builder = HttpClientTransport.newBuilder()
                .setConnectTimeout(authenticationProvider.getConnectTimeout())
                .setRequestTimeout(authenticationProvider.getReadTimeout())
                .setTlsProtocolAllowList(authenticationProvider.getTlsProtocolAllowList())
                .setTlsProtocolDenyList(authenticationProvider.getTlsProtocolDenyList())
                .setTlsCipherSuiteAllowList(authenticationProvider.getTlsCipherSuiteAllowList())
                .setTlsCipherSuiteDenyList(authenticationProvider.getTlsCipherSuiteDenyList());
        final TrustStore<?> trustStore = authenticationProvider.getTrustStore();
        if (trustStore != null)
        {
            try
            {
                builder.setTrustManagers(trustStore.getTrustManagers());
            }
            catch (GeneralSecurityException e)
            {
                throw new ServerScopedRuntimeException("Cannot initialise TLS", e);
            }
        }
        return builder.build();
    }

    private static String formEncode(final String value)
    {
        return URLEncoder.encode(value, UTF_8);
    }
}
