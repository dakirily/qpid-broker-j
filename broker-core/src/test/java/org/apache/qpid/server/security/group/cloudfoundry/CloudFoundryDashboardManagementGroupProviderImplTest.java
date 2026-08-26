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

package org.apache.qpid.server.security.group.cloudfoundry;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2UserPrincipal;
import org.apache.qpid.server.util.ExternalServiceException;
import org.apache.qpid.server.util.ExternalServiceTimeoutException;
import org.apache.qpid.server.util.HttpClientTransport;
import org.apache.qpid.test.utils.UnitTestBase;

public class CloudFoundryDashboardManagementGroupProviderImplTest extends UnitTestBase
{
    private static final String ACCESS_TOKEN = "access-token";

    private CloudFoundryDashboardManagementGroupProviderImpl _provider;
    private HttpClientTransport _transport;
    private Principal _userPrincipal;

    @BeforeEach
    public void setUp() throws Exception
    {
        final Broker<?> broker = BrokerTestHelper.createBrokerMock();
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, "cloudFoundryGroups");
        attributes.put("cloudFoundryEndpointURI", URI.create("https://cloudfoundry.example.org"));
        attributes.put("serviceToManagementGroupMapping", Map.of("service-id", "managers"));
        final TrustStore<?> trustStore = mock(TrustStore.class);
        attributes.put("trustStore", trustStore);
        _provider = new CloudFoundryDashboardManagementGroupProviderImpl(attributes, broker);
        _provider.open();
        verify(trustStore, never()).getTrustManagers();

        _transport = mock(HttpClientTransport.class);
        when(_transport.newRequestBuilder(any(URI.class)))
                .thenAnswer(invocation -> HttpRequest.newBuilder(invocation.getArgument(0)));
        setTransport(_provider, _transport);

        final AuthenticationProvider<?> authenticationProvider = mock(AuthenticationProvider.class);
        _userPrincipal = new OAuth2UserPrincipal("user", ACCESS_TOKEN, authenticationProvider);
    }

    @Test
    public void testMapsTimeoutToExternalServiceTimeoutException() throws Exception
    {
        final SocketTimeoutException cause = new SocketTimeoutException("request timed out");
        when(_transport.send(any(HttpRequest.class))).thenThrow(cause);

        final ExternalServiceTimeoutException exception = assertThrows(
                ExternalServiceTimeoutException.class,
                () -> _provider.getGroupPrincipalsForUser(_userPrincipal));

        assertSame(cause, exception.getCause());
    }

    @Test
    public void testMapsIoFailureToExternalServiceException() throws Exception
    {
        final IOException cause = new IOException("request failed");
        when(_transport.send(any(HttpRequest.class))).thenThrow(cause);

        final ExternalServiceException exception = assertThrows(
                ExternalServiceException.class,
                () -> _provider.getGroupPrincipalsForUser(_userPrincipal));

        assertFalse(exception instanceof ExternalServiceTimeoutException);
        assertSame(cause, exception.getCause());
    }

    @Test
    public void testMapsInvalidRuntimeUriToExternalServiceException()
    {
        final IllegalArgumentException cause = new IllegalArgumentException("invalid URI");
        when(_transport.newRequestBuilder(any(URI.class))).thenThrow(cause);

        final ExternalServiceException exception = assertThrows(
                ExternalServiceException.class,
                () -> _provider.getGroupPrincipalsForUser(_userPrincipal));

        assertSame(cause, exception.getCause());
    }

    @Test
    public void testRejectsEndpointWithoutHost()
    {
        assertThrows(IllegalConfigurationException.class,
                     () -> _provider.setAttributes(
                             Map.of("cloudFoundryEndpointURI", URI.create("https:/cloud-foundry"))));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUsesBearerAuthenticationAndMapsSuccessfulResponse() throws Exception
    {
        final HttpResponse<byte[]> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.body()).thenReturn("{\"manage\":true}".getBytes(UTF_8));
        when(_transport.send(any(HttpRequest.class))).thenReturn(response);

        final Set<Principal> principals = _provider.getGroupPrincipalsForUser(_userPrincipal);

        assertEquals(1, principals.size());
        assertEquals("managers", principals.iterator().next().getName());
        final ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(_transport).send(requestCaptor.capture());
        assertEquals("Bearer " + ACCESS_TOKEN,
                     requestCaptor.getValue().headers().firstValue("Authorization").orElseThrow());
    }

    private static void setTransport(final CloudFoundryDashboardManagementGroupProviderImpl provider,
                                     final HttpClientTransport transport) throws Exception
    {
        final Field field = CloudFoundryDashboardManagementGroupProviderImpl.class
                .getDeclaredField("_httpClientTransport");
        field.setAccessible(true);
        field.set(provider, transport);
    }
}
