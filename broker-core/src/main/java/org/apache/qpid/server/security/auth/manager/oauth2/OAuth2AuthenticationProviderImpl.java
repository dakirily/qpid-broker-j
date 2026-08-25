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
 */

package org.apache.qpid.server.security.auth.manager.oauth2;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.manager.AbstractAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.AuthenticationResultCacher;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.oauth2.OAuth2Negotiator;
import org.apache.qpid.server.util.HttpClientTransport;
import org.apache.qpid.server.util.ParameterizedTypes;
import org.apache.qpid.server.util.Strings;

public class OAuth2AuthenticationProviderImpl
        extends AbstractAuthenticationManager<OAuth2AuthenticationProviderImpl>
        implements OAuth2AuthenticationProvider<OAuth2AuthenticationProviderImpl>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2AuthenticationProviderImpl.class);
    private static final String TRUST_STORE_ATTRIBUTE = "trustStore";

    private final ObjectMapper _objectMapper = JsonMapper.builder().build();

    @ManagedAttributeField
    private URI _authorizationEndpointURI;

    @ManagedAttributeField
    private URI _tokenEndpointURI;

    @ManagedAttributeField
    private URI _identityResolverEndpointURI;

    @ManagedAttributeField
    private boolean _tokenEndpointNeedsAuth;

    @ManagedAttributeField
    private URI _postLogoutURI;

    @ManagedAttributeField
    private String _clientId;

    @ManagedAttributeField
    private String _clientSecret;

    @ManagedAttributeField
    private TrustStore<?> _trustStore;

    @ManagedAttributeField
    private String _scope;

    @ManagedAttributeField
    private String _identityResolverType;

    private OAuth2IdentityResolverService _identityResolverService;

    private List<String> _tlsProtocolAllowList;
    private List<String> _tlsProtocolDenyList;

    private List<String> _tlsCipherSuiteAllowList;
    private List<String> _tlsCipherSuiteDenyList;

    private int _connectTimeout;
    private int _readTimeout;

    private AuthenticationResultCacher _authenticationResultCacher;
    private volatile HttpClientTransport _httpClientTransport;

    @ManagedObjectFactoryConstructor
    protected OAuth2AuthenticationProviderImpl(final Map<String, Object> attributes,
                                               final Container<?> container)
    {
        super(attributes, container);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        final String type = getIdentityResolverType();
        _identityResolverService =
                new QpidServiceLoader().getInstancesByType(OAuth2IdentityResolverService.class).get(type);
        _tlsProtocolAllowList =
                getContextValue(List.class,
                                ParameterizedTypes.LIST_OF_STRINGS,
                                CommonProperties.QPID_SECURITY_TLS_PROTOCOL_ALLOW_LIST);
        _tlsProtocolDenyList =
                getContextValue(List.class,
                                ParameterizedTypes.LIST_OF_STRINGS,
                                CommonProperties.QPID_SECURITY_TLS_PROTOCOL_DENY_LIST);
        _tlsCipherSuiteAllowList =
                getContextValue(List.class,
                                ParameterizedTypes.LIST_OF_STRINGS,
                                CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_ALLOW_LIST);
        _tlsCipherSuiteDenyList =
                getContextValue(List.class,
                                ParameterizedTypes.LIST_OF_STRINGS,
                                CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_DENY_LIST);
        _connectTimeout = getContextValue(Integer.class, AUTHENTICATION_OAUTH2_CONNECT_TIMEOUT);
        _readTimeout = getContextValue(Integer.class, AUTHENTICATION_OAUTH2_READ_TIMEOUT);
        _httpClientTransport = OAuth2Utils.createHttpClientTransport(this);

        final Integer configuredCacheMaxSize = getContextValue(Integer.class, AUTHENTICATION_CACHE_MAX_SIZE);
        final Long configuredCacheExpirationTime =
                getContextValue(Long.class, AUTHENTICATION_CACHE_EXPIRATION_TIME);
        final Integer configuredCacheIterationCount =
                getContextValue(Integer.class, AUTHENTICATION_CACHE_ITERATION_COUNT);
        final int cacheMaxSize;
        final long cacheExpirationTime;
        final int cacheIterationCount;
        if (configuredCacheMaxSize == null || configuredCacheMaxSize <= 0
            || configuredCacheExpirationTime == null || configuredCacheExpirationTime <= 0
            || configuredCacheIterationCount == null || configuredCacheIterationCount < 0)
        {
            LOGGER.debug("disabling authentication result caching");
            cacheMaxSize = 0;
            cacheExpirationTime = 1L;
            cacheIterationCount = 0;
        }
        else
        {
            cacheMaxSize = configuredCacheMaxSize;
            cacheExpirationTime = configuredCacheExpirationTime;
            cacheIterationCount = configuredCacheIterationCount;
        }
        _authenticationResultCacher =
                new AuthenticationResultCacher(cacheMaxSize, cacheExpirationTime, cacheIterationCount);
    }

    @Override
    protected void postSetAttributes(final Set<String> actualUpdatedAttributes)
    {
        super.postSetAttributes(actualUpdatedAttributes);
        if (_httpClientTransport != null && actualUpdatedAttributes.contains(TRUST_STORE_ATTRIBUTE))
        {
            _httpClientTransport = OAuth2Utils.createHttpClientTransport(this);
        }
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        final OAuth2AuthenticationProvider<?> validationProxy = (OAuth2AuthenticationProvider<?>) proxyForValidation;
        validateResolver(validationProxy);
        validateSecureEndpoints(validationProxy);
        validatePostLogoutURI(validationProxy);
    }

    @Override
    public void onValidate()
    {
        super.onValidate();
        validateResolver(this);
        validateSecureEndpoints(this);
        validatePostLogoutURI(this);
    }

    private void validateSecureEndpoints(final OAuth2AuthenticationProvider<?> provider)
    {
        if (!"https".equals(provider.getAuthorizationEndpointURI().getScheme()))
        {
            throw new IllegalConfigurationException(
                    String.format("Authorization endpoint is not secure: '%s'",
                                  provider.getAuthorizationEndpointURI()));
        }
        if (!"https".equals(provider.getTokenEndpointURI().getScheme()))
        {
            throw new IllegalConfigurationException(
                    String.format("Token endpoint is not secure: '%s'", provider.getTokenEndpointURI()));
        }
        if (!"https".equals(provider.getIdentityResolverEndpointURI().getScheme()))
        {
            throw new IllegalConfigurationException(
                    String.format("Identity resolver endpoint is not secure: '%s'",
                                  provider.getIdentityResolverEndpointURI()));
        }
    }

    private void validatePostLogoutURI(final OAuth2AuthenticationProvider<?> provider)
    {
        if (provider.getPostLogoutURI() != null)
        {
            final String scheme = provider.getPostLogoutURI().getScheme();
            if (!"https".equals(scheme) && !"http".equals(scheme))
            {
                throw new IllegalConfigurationException(
                        String.format("Post logout URI does not have a http or https scheme: '%s'",
                                      provider.getPostLogoutURI()));
            }
        }
    }

    private void validateResolver(final OAuth2AuthenticationProvider<?> provider)
    {
        final OAuth2IdentityResolverService identityResolverService =
                new QpidServiceLoader().getInstancesByType(OAuth2IdentityResolverService.class)
                        .get(provider.getIdentityResolverType());

        if (identityResolverService == null)
        {
            throw new IllegalConfigurationException("Unknown identity resolver " + provider.getType());
        }
        else
        {
            identityResolverService.validate(provider);
        }
    }

    @Override
    public List<String> getMechanisms()
    {
        return Collections.singletonList(OAuth2Negotiator.MECHANISM);
    }

    @Override
    public SaslNegotiator createSaslNegotiator(final String mechanism,
                                               final SaslSettings saslSettings,
                                               final NamedAddressSpace addressSpace)
    {
        if(OAuth2Negotiator.MECHANISM.equals(mechanism))
        {
            return new OAuth2Negotiator(this, addressSpace);
        }
        else
        {
            return null;
        }
    }

    @Override
    public AuthenticationResult authenticateViaAuthorizationCode(final String authorizationCode,
                                                                 final String redirectUri,
                                                                 final NamedAddressSpace addressSpace)
    {
        final URI tokenEndpoint = getTokenEndpointURI(addressSpace);
        try
        {
            LOGGER.debug("About to call token endpoint '{}'", tokenEndpoint);
            final HttpRequest.Builder requestBuilder = _httpClientTransport.newRequestBuilder(tokenEndpoint)
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/x-www-form-urlencoded");
            final Map<String, String> requestBody = new HashMap<>();
            final String clientSecret = getClientSecret() == null ? "" : getClientSecret();
            if (getTokenEndpointNeedsAuth())
            {
                requestBuilder.header("Authorization",
                                      OAuth2Utils.buildBasicAuthorization(getClientId(), clientSecret));
            }
            else
            {
                requestBody.put("client_id", getClientId());
                if (!"".equals(clientSecret))
                {
                    requestBody.put("client_secret", clientSecret);
                }
            }

            requestBody.put("code", authorizationCode);
            requestBody.put("redirect_uri", redirectUri);
            requestBody.put("grant_type", "authorization_code");
            requestBody.put("response_type", "token");
            final HttpRequest request = requestBuilder
                    .POST(HttpRequest.BodyPublishers.ofString(OAuth2Utils.buildRequestQuery(requestBody), UTF_8))
                    .build();
            final HttpResponse<byte[]> response = _httpClientTransport.send(request);
            final int responseCode = response.statusCode();
            LOGGER.debug("Call to token endpoint '{}' complete, response code : {}", tokenEndpoint, responseCode);

            try
            {
                final Map<String, Object> responseMap = _objectMapper.readValue(response.body(), Map.class);
                if (responseCode != 200 || responseMap.containsKey("error"))
                {
                    final IllegalStateException e = new IllegalStateException(
                            String.format("Token endpoint failed, response code %d, error '%s', description '%s'",
                                          responseCode,
                                          responseMap.get("error"),
                                          responseMap.get("error_description")));
                    LOGGER.error(e.getMessage());
                    return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
                }

                final Object accessTokenObject = responseMap.get("access_token");
                if (accessTokenObject == null)
                {
                    final IllegalStateException e =
                            new IllegalStateException("Token endpoint response did not include 'access_token'");
                    LOGGER.error("Unexpected token endpoint response", e);
                    return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
                }
                final String accessToken = String.valueOf(accessTokenObject);

                return authenticateViaAccessToken(accessToken, addressSpace);
            }
            catch (JacksonException e)
            {
                final IllegalStateException ise =
                        new IllegalStateException(String.format("Token endpoint '%s' did not return json",
                                                               tokenEndpoint),
                                                  e);
                LOGGER.error("Unexpected token endpoint response", e);
                return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, ise);
            }
        }
        catch (IOException e)
        {
            LOGGER.error("Call to token endpoint failed", e);
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
    }

    @Override
    public HttpClientTransport getHttpClientTransport()
    {
        return _httpClientTransport;
    }

    @Override
    public AuthenticationResult authenticateViaAccessToken(final String accessToken,
                                                           final NamedAddressSpace addressSpace)
    {
        return _authenticationResultCacher.getOrLoad(new String[]{accessToken}, () ->
        {
            try
            {
                final Principal userPrincipal =
                        _identityResolverService.getUserPrincipal(OAuth2AuthenticationProviderImpl.this,
                                                                  accessToken,
                                                                  addressSpace);
                final OAuth2UserPrincipal oauthUserPrincipal =
                        new OAuth2UserPrincipal(userPrincipal.getName(),
                                                accessToken,
                                                OAuth2AuthenticationProviderImpl.this);
                return new AuthenticationResult(oauthUserPrincipal);
            }
            catch (IOException | IdentityResolverException e)
            {
                LOGGER.error("Call to identity resolver failed", e);
                return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
            }
        });
    }

    @Override
    public URI getAuthorizationEndpointURI()
    {
        return _authorizationEndpointURI;
    }

    @Override
    public URI getAuthorizationEndpointURI(final NamedAddressSpace addressSpace)
    {
        return getUriForAddressSpace(getAuthorizationEndpointURI(), addressSpace);
    }


    @Override
    public URI getTokenEndpointURI()
    {
        return _tokenEndpointURI;
    }

    @Override
    public URI getTokenEndpointURI(final NamedAddressSpace addressSpace)
    {

        return getUriForAddressSpace(getTokenEndpointURI(), addressSpace);
    }

    @Override
    public URI getIdentityResolverEndpointURI()
    {
        return _identityResolverEndpointURI;
    }

    @Override
    public URI getIdentityResolverEndpointURI(final NamedAddressSpace addressSpace)
    {
        return getUriForAddressSpace(getIdentityResolverEndpointURI(), addressSpace);
    }

    private URI getUriForAddressSpace(final URI uri, final NamedAddressSpace addressSpace)
    {
        try
        {
            final String vhostName = URLEncoder.encode(addressSpace == null
                                                               ? ""
                                                               : addressSpace.getName(),
                                                       UTF_8);

            final Strings.MapResolver virtualhostResolver =
                    new Strings.MapResolver(Collections.singletonMap("virtualhost", vhostName));

            final String substitutedURI = Strings.expand(uri.toString(), false, virtualhostResolver);
            return new URI(substitutedURI);
        }
        catch (URISyntaxException e)
        {
            LOGGER.error("Error when attempting to build URI from address space: ", e);
        }
        return uri;
    }


    @Override
    public URI getPostLogoutURI()
    {
        return _postLogoutURI;
    }

    @Override
    public boolean getTokenEndpointNeedsAuth()
    {
        return _tokenEndpointNeedsAuth;
    }

    @Override
    public String getIdentityResolverType()
    {
        return _identityResolverType;
    }

    @Override
    public String getClientId()
    {
        return _clientId;
    }

    @Override
    public String getClientSecret()
    {
        return _clientSecret;
    }

    @Override
    public TrustStore<?> getTrustStore()
    {
        return _trustStore;
    }

    @Override
    public String getScope()
    {
        return _scope;
    }

    @Override
    public URI getDefaultAuthorizationEndpointURI()
    {
        final OAuth2IdentityResolverService identityResolverService =
                new QpidServiceLoader().getInstancesByType(OAuth2IdentityResolverService.class)
                        .get(getIdentityResolverType());
        return identityResolverService == null
                ? null
                : identityResolverService.getDefaultAuthorizationEndpointURI(this);
    }

    @Override
    public URI getDefaultTokenEndpointURI()
    {
        final OAuth2IdentityResolverService identityResolverService =
                new QpidServiceLoader().getInstancesByType(OAuth2IdentityResolverService.class)
                        .get(getIdentityResolverType());
        return identityResolverService == null ? null : identityResolverService.getDefaultTokenEndpointURI(this);
    }

    @Override
    public URI getDefaultIdentityResolverEndpointURI()
    {
        final OAuth2IdentityResolverService identityResolverService =
                new QpidServiceLoader().getInstancesByType(OAuth2IdentityResolverService.class)
                        .get(getIdentityResolverType());
        return identityResolverService == null
                ? null
                : identityResolverService.getDefaultIdentityResolverEndpointURI(this);
    }

    @Override
    public String getDefaultScope()
    {
        final OAuth2IdentityResolverService identityResolverService =
                new QpidServiceLoader().getInstancesByType(OAuth2IdentityResolverService.class)
                        .get(getIdentityResolverType());
        return identityResolverService == null ? null : identityResolverService.getDefaultScope(this);
    }

    @Override
    public List<String> getTlsProtocolAllowList()
    {
        return _tlsProtocolAllowList;
    }

    @Override
    public List<String> getTlsProtocolDenyList()
    {
        return _tlsProtocolDenyList;
    }

    @Override
    public List<String> getTlsCipherSuiteAllowList()
    {
        return _tlsCipherSuiteAllowList;
    }

    @Override
    public List<String> getTlsCipherSuiteDenyList()
    {
        return _tlsCipherSuiteDenyList;
    }

    @Override
    public int getConnectTimeout()
    {
        return _connectTimeout;
    }

    @Override
    public int getReadTimeout()
    {
        return _readTimeout;
    }

    @SuppressWarnings("unused")
    public static Collection<String> validIdentityResolvers()
    {
        return new QpidServiceLoader().getInstancesByType(OAuth2IdentityResolverService.class).keySet();
    }
}
