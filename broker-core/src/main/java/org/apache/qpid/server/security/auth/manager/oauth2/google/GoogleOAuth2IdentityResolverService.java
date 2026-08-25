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

package org.apache.qpid.server.security.auth.manager.oauth2.google;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.Principal;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.oauth2.IdentityResolverException;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2IdentityResolverService;
import org.apache.qpid.server.util.HttpClientTransport;

/**
 * An identity resolver that calls Google's userinfo endpoint https://www.googleapis.com/oauth2/v3/userinfo.
 *
 * It requires that the authentication request includes the scope 'profile' in order that 'sub'
 * (the user identifier) appears in userinfo's response.
 *
 * For endpoint is documented:
 *
 * https://developers.google.com/identity/protocols/OpenIDConnect
 */
@PluggableService
public class GoogleOAuth2IdentityResolverService implements OAuth2IdentityResolverService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleOAuth2IdentityResolverService.class);

    public static final String TYPE = "GoogleUserInfo";

    private final ObjectMapper _objectMapper = JsonMapper.builder().build();

    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public void validate(final OAuth2AuthenticationProvider<?> authProvider) throws IllegalConfigurationException
    {
        if (Stream.of(authProvider.getScope().split("\\s")).noneMatch("profile"::equals))
        {
            throw new IllegalConfigurationException(
                    "This identity resolver requires that scope 'profile' is included in"
                    + " the authentication request.");
        }
    }

    @Override
    public Principal getUserPrincipal(final OAuth2AuthenticationProvider<?> authenticationProvider,
                                      final String accessToken,
                                      final NamedAddressSpace addressSpace)
            throws IOException, IdentityResolverException
    {
        final URI userInfoEndpoint = authenticationProvider.getIdentityResolverEndpointURI(addressSpace);
        LOGGER.debug("About to call identity service '{}'", userInfoEndpoint);
        final HttpClientTransport transport = authenticationProvider.getHttpClientTransport();
        final HttpRequest request = transport.newRequestBuilder(userInfoEndpoint)
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + accessToken)
                .GET()
                .build();
        final HttpResponse<byte[]> response = transport.send(request);
        final int responseCode = response.statusCode();
        LOGGER.debug("Call to identity service '{}' complete, response code : {}",
                     userInfoEndpoint,
                     responseCode);

        final Map<String, String> responseMap;
        try
        {
            responseMap = _objectMapper.readValue(response.body(), Map.class);
        }
        catch (JacksonException e)
        {
            throw new IOException(String.format("Identity resolver '%s' did not return json",
                                                userInfoEndpoint), e);
        }
        if (responseCode != 200)
        {
            throw new IdentityResolverException(String.format(
                    "Identity resolver '%s' failed, response code %d",
                    userInfoEndpoint, responseCode));
        }

        final String googleId = responseMap.get("sub");
        if (googleId == null)
        {
            throw new IdentityResolverException(String.format(
                    "Identity resolver '%s' failed, response did not include 'sub'",
                    userInfoEndpoint));
        }
        return new UsernamePrincipal(googleId, authenticationProvider);
    }

    @Override
    public URI getDefaultAuthorizationEndpointURI(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        try
        {
            return new URI("https://accounts.google.com/o/oauth2/v2/auth");
        }
        catch (URISyntaxException e)
        {
            return null;
        }
    }

    @Override
    public URI getDefaultTokenEndpointURI(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        try
        {
            return new URI("https://www.googleapis.com/oauth2/v4/token");
        }
        catch (URISyntaxException e)
        {
            return null;
        }
    }

    @Override
    public URI getDefaultIdentityResolverEndpointURI(
            final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        try
        {
            return new URI("https://www.googleapis.com/oauth2/v3/userinfo");
        }
        catch (URISyntaxException e)
        {
            return null;
        }
    }

    @Override
    public String getDefaultScope(final OAuth2AuthenticationProvider<?> oAuth2AuthenticationProvider)
    {
        return "profile";
    }
}
