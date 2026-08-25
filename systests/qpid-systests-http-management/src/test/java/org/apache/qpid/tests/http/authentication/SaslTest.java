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
package org.apache.qpid.tests.http.authentication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static jakarta.servlet.http.HttpServletResponse.SC_EXPECTATION_FAILED;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static org.apache.qpid.server.security.auth.sasl.SaslUtil.generateCramMD5ClientResponse;
import static org.apache.qpid.server.security.auth.sasl.SaslUtil.generatePlainClientResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.apache.qpid.server.security.auth.manager.ScramSHA1AuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Negotiator;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;
import org.apache.qpid.tests.http.HttpTestBase;

// TestInstance.Lifecycle.PER_METHOD fixes rare race conditions
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class SaslTest extends HttpTestBase
{
    private static final String SASL_SERVICE = "/service/sasl";
    private static final String SET_COOKIE_HEADER = "Set-Cookie";

    private String _userName;
    private String _userPassword;

    @BeforeEach
    public void setUp()
    {
        _userName = getBrokerAdmin().getValidUsername();
        _userPassword = getBrokerAdmin().getValidPassword();
    }

    @Test
    public void requestSASLMechanisms() throws Exception
    {
        final Map<String, Object> saslData = getBrokerHelper().getJsonAsMap(SASL_SERVICE);
        assertNotNull(saslData.get("mechanisms"), "mechanisms attribute is not found");

        @SuppressWarnings("unchecked")
        final List<String> mechanisms = (List<String>) saslData.get("mechanisms");
        final String[] expectedMechanisms = {PlainNegotiator.MECHANISM,
                CramMd5Negotiator.MECHANISM,
                ScramSHA1AuthenticationManager.MECHANISM,
                ScramSHA256AuthenticationManager.MECHANISM};
        for (final String mechanism : expectedMechanisms)
        {
            assertTrue(mechanisms.contains(mechanism), String.format("Mechanism '%s' is not found", mechanism));
        }
        assertNull(saslData.get("user"), String.format("Unexpected user was returned: %s", saslData.get("user")));
    }

    @Test
    public void requestUnsupportedSASLMechanism() throws Exception
    {
        final HttpResponse<byte[]> response = requestSASLAuthentication("UNSUPPORTED");
        assertEquals(SC_EXPECTATION_FAILED, response.statusCode(), "Unexpected response");
    }

    @Test
    public void plainSASLAuthenticationWithoutInitialResponse() throws Exception
    {
        final HttpResponse<byte[]> response = requestSASLAuthentication(PlainNegotiator.MECHANISM);
        assertEquals(SC_OK, response.statusCode(), "Unexpected response");
        handleChallengeAndSendResponse(response, _userName, _userPassword, PlainNegotiator.MECHANISM, SC_OK);
        assertAuthenticatedUser(_userName, getCookies(response));
    }

    @Test
    public void plainSASLAuthenticationWithMalformedInitialResponse() throws Exception
    {
        final String responseData = Base64.getEncoder().encodeToString("null".getBytes(UTF_8));
        final HttpResponse<byte[]> response =
                postForm(Map.of("mechanism", PlainNegotiator.MECHANISM, "response", responseData), List.of());
        assertEquals(SC_UNAUTHORIZED, response.statusCode(), "Unexpected response code");
        assertAuthenticatedUser(null, getCookies(response));
    }

    @Test
    public void plainSASLAuthenticationWithValidCredentials() throws Exception
    {
        final List<String> cookies = plainSASLAuthenticationWithInitialResponse(_userName, _userPassword, SC_OK);
        assertAuthenticatedUser(_userName, cookies);
    }

    @Test
    public void plainSASLAuthenticationWithIncorrectPassword() throws Exception
    {
        final List<String> cookies =
                plainSASLAuthenticationWithInitialResponse(_userName, "incorrect", SC_UNAUTHORIZED);
        assertAuthenticatedUser(null, cookies);
    }

    @Test
    public void plainSASLAuthenticationWithUnknownUser() throws Exception
    {
        final List<String> cookies =
                plainSASLAuthenticationWithInitialResponse("unknown", _userPassword, SC_UNAUTHORIZED);
        assertAuthenticatedUser(null, cookies);
    }

    @Test
    public void cramMD5SASLAuthenticationForValidCredentials() throws Exception
    {
        final List<String> cookies =
                challengeResponseAuthentication(_userName, _userPassword, CramMd5Negotiator.MECHANISM, SC_OK);
        assertAuthenticatedUser(_userName, cookies);
    }

    @Test
    public void cramMD5SASLAuthenticationForIncorrectPassword() throws Exception
    {
        final List<String> cookies =
                challengeResponseAuthentication(_userName, "incorrect",
                                                CramMd5Negotiator.MECHANISM, SC_UNAUTHORIZED);
        assertAuthenticatedUser(null, cookies);
    }

    @Test
    public void cramMD5SASLAuthenticationForNonExistingUser() throws Exception
    {
        final List<String> cookies =
                challengeResponseAuthentication("unknown", _userPassword,
                                                CramMd5Negotiator.MECHANISM, SC_UNAUTHORIZED);
        assertAuthenticatedUser(null, cookies);
    }

    @Test
    public void cramMD5SASLAuthenticationResponseNotProvided() throws Exception
    {
        final HttpResponse<byte[]> response = requestSASLAuthentication(CramMd5Negotiator.MECHANISM);
        final Map<String, Object> responseData = getBrokerHelper().readJsonResponseAsMap(response);
        final String challenge = (String) responseData.get("challenge");
        assertNotNull(challenge, "Challenge is not found");

        final List<String> cookies = getCookies(response);
        postResponse(cookies, Map.of("id", String.valueOf(responseData.get("id"))), SC_UNAUTHORIZED);
        assertAuthenticatedUser(null, cookies);
    }

    @Test
    public void cramMD5SASLAuthenticationWithMalformedResponse() throws Exception
    {
        final HttpResponse<byte[]> response = requestSASLAuthentication(CramMd5Negotiator.MECHANISM);
        final Map<String, Object> responseData = getBrokerHelper().readJsonResponseAsMap(response);
        final String challenge = (String) responseData.get("challenge");
        assertNotNull(challenge, "Challenge is not found");

        final List<String> cookies = getCookies(response);
        final String malformedResponse = Base64.getEncoder().encodeToString("null".getBytes(UTF_8));
        postResponse(cookies,
                     Map.of("id", String.valueOf(responseData.get("id")), "response", malformedResponse),
                     SC_UNAUTHORIZED);
        assertAuthenticatedUser(null, cookies);
    }

    @Test
    public void cramMD5SASLAuthenticationWithInvalidId() throws Exception
    {
        final HttpResponse<byte[]> response = requestSASLAuthentication(CramMd5Negotiator.MECHANISM);
        final Map<String, Object> responseData = getBrokerHelper().readJsonResponseAsMap(response);
        final String challenge = (String) responseData.get("challenge");
        assertNotNull(challenge, "Challenge is not found");

        final byte[] challengeBytes = Base64.getDecoder().decode(challenge);
        final byte[] clientResponse =
                generateClientResponse(CramMd5Negotiator.MECHANISM, _userName, _userPassword, challengeBytes);
        final String encodedResponse = Base64.getEncoder().encodeToString(clientResponse);
        final List<String> cookies = getCookies(response);
        postResponse(cookies,
                     Map.of("id", UUID.randomUUID().toString(), "response", encodedResponse),
                     SC_EXPECTATION_FAILED);
        assertAuthenticatedUser(null, cookies);
    }

    private List<String> plainSASLAuthenticationWithInitialResponse(final String userName,
                                                                    final String userPassword,
                                                                    final int expectedResponseCode) throws Exception
    {
        final byte[] responseBytes = generatePlainClientResponse(userName, userPassword);
        final String responseData = Base64.getEncoder().encodeToString(responseBytes);
        final HttpResponse<byte[]> response =
                postForm(Map.of("mechanism", PlainNegotiator.MECHANISM, "response", responseData), List.of());
        assertEquals(expectedResponseCode, response.statusCode(), "Unexpected response code");
        return getCookies(response);
    }

    private List<String> challengeResponseAuthentication(final String userName,
                                                         final String userPassword,
                                                         final String mechanism,
                                                         final int expectedResponseCode) throws Exception
    {
        final HttpResponse<byte[]> response = requestSASLAuthentication(mechanism);
        handleChallengeAndSendResponse(response, userName, userPassword, mechanism, expectedResponseCode);
        return getCookies(response);
    }

    private void handleChallengeAndSendResponse(final HttpResponse<byte[]> challengeResponse,
                                                final String userName,
                                                final String userPassword,
                                                final String mechanism,
                                                final int expectedResponseCode) throws Exception
    {
        final Map<String, Object> responseData = getBrokerHelper().readJsonResponseAsMap(challengeResponse);
        final String challenge = (String) responseData.get("challenge");
        assertNotNull(challenge, "Challenge is not found");

        final byte[] challengeBytes = Base64.getDecoder().decode(challenge);
        final byte[] clientResponse = generateClientResponse(mechanism, userName, userPassword, challengeBytes);
        final String encodedResponse = Base64.getEncoder().encodeToString(clientResponse);
        postResponse(getCookies(challengeResponse),
                     Map.of("id", String.valueOf(responseData.get("id")), "response", encodedResponse),
                     expectedResponseCode);
    }

    private void postResponse(final List<String> cookies,
                              final Map<String, String> parameters,
                              final int expectedResponseCode) throws IOException
    {
        final HttpResponse<byte[]> response = postForm(parameters, cookies);
        assertEquals(expectedResponseCode, response.statusCode(), "Unexpected response code");
    }

    private HttpResponse<byte[]> postForm(final Map<String, String> parameters, final List<String> cookies)
            throws IOException
    {
        final String formData = parameters.entrySet().stream()
                .map(entry -> encode(entry.getKey()) + "=" + encode(entry.getValue()))
                .collect(Collectors.joining("&"));
        final HttpRequest.Builder request = getBrokerHelper().createRequest(SASL_SERVICE, "POST")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .method("POST", HttpRequest.BodyPublishers.ofString(formData, UTF_8));
        applyCookies(cookies, request);
        return getBrokerHelper().send(request);
    }

    private byte[] generateClientResponse(final String mechanism,
                                          final String userName,
                                          final String userPassword,
                                          final byte[] challengeBytes) throws Exception
    {
        if (PlainNegotiator.MECHANISM.equals(mechanism))
        {
            return generatePlainClientResponse(userName, userPassword);
        }
        else if (CramMd5Negotiator.MECHANISM.equalsIgnoreCase(mechanism))
        {
            return generateCramMD5ClientResponse(userName, userPassword, challengeBytes);
        }
        throw new IllegalArgumentException("Unsupported test mechanism " + mechanism);
    }

    private void applyCookies(final List<String> cookies, final HttpRequest.Builder request)
    {
        final String cookieHeader = cookies.stream()
                .map(cookie -> cookie.split(";", 2)[0])
                .collect(Collectors.joining("; "));
        if (!cookieHeader.isEmpty())
        {
            request.header("Cookie", cookieHeader);
        }
    }

    private List<String> getCookies(final HttpResponse<?> response)
    {
        return response.headers().allValues(SET_COOKIE_HEADER);
    }

    private HttpResponse<byte[]> requestSASLAuthentication(final String mechanism) throws IOException
    {
        return postForm(Map.of("mechanism", mechanism), List.of());
    }

    private void assertAuthenticatedUser(final String userName, final List<String> cookies) throws IOException
    {
        final HttpRequest.Builder request = getBrokerHelper().createRequest(SASL_SERVICE, "GET");
        applyCookies(cookies, request);
        final Map<String, Object> response =
                getBrokerHelper().readJsonResponseAsMap(getBrokerHelper().send(request));
        assertEquals(userName, response.get("user"), "Unexpected user");
    }

    private String encode(final String value)
    {
        return URLEncoder.encode(value, UTF_8);
    }
}
