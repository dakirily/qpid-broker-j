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
package org.apache.qpid.tools;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.tools.RestStressTestClient.RestClient;

public class RestStressTestClientTest extends UnitTestBase
{
    private static final TypeReference<Map<String, Object>> TYPE_MAP = new TypeReference<>()
    {
    };
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private HttpServer _server;
    private String _baseUrl;

    @BeforeEach
    public void setUp() throws IOException
    {
        _server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        _server.start();
        _baseUrl = "http://127.0.0.1:" + _server.getAddress().getPort();
    }

    @AfterEach
    public void tearDown()
    {
        _server.stop(0);
    }

    @Test
    public void basicAuthenticationAndCrudRequestsUseExpectedHttpSemantics() throws Exception
    {
        final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
        _server.createContext("/", exchange ->
        {
            requests.add(capture(exchange));
            if ("GET".equals(exchange.getRequestMethod()))
            {
                respond(exchange, 200, OBJECT_MAPPER.writeValueAsString(Map.of("name", "testBroker")));
            }
            else if ("PUT".equals(exchange.getRequestMethod()))
            {
                respond(exchange, 201, "");
            }
            else
            {
                respond(exchange, 200, "");
            }
        });

        final RestClient client = new RestClient(_baseUrl + "/", "testUser", "testPassword", null);
        assertEquals("testBroker", client.get("/api/latest/broker?depth=0").get("name"));

        final Map<String, Object> queueAttributes = new LinkedHashMap<>();
        queueAttributes.put("name", "testQueue");
        queueAttributes.put("durable", true);
        assertEquals(RestClient.RESPONSE_PUT_CREATE_OK, client.put("/api/latest/queue/testQueue", queueAttributes));

        final Map<String, String> bindingData = new LinkedHashMap<>();
        bindingData.put("bindingKey", "testQueue");
        bindingData.put("destination", "testQueue");
        assertEquals(RestClient.RESPONSE_OK, client.post("/api/latest/exchange/amq.direct/bind", bindingData));
        assertEquals(RestClient.RESPONSE_PUT_UPDATE_OK, client.delete("/api/latest/queue/testQueue"));

        assertEquals(4, requests.size());
        final String expectedAuthorization = "Basic " +
                Base64.getEncoder().encodeToString("testUser:testPassword".getBytes(UTF_8));
        assertTrue(requests.stream().allMatch(request -> expectedAuthorization.equals(request.authorization())));
        assertTrue(requests.stream().allMatch(request -> "HTTP/1.1".equals(request.protocol())));

        assertEquals("GET", requests.get(0).method());
        assertEquals("/api/latest/broker?depth=0", requests.get(0).target());
        assertEquals("PUT", requests.get(1).method());
        assertEquals("application/json", requests.get(1).contentType());
        assertEquals(queueAttributes, readJson(requests.get(1).body()));
        assertEquals("POST", requests.get(2).method());
        assertEquals("application/json", requests.get(2).contentType());
        assertEquals(bindingData, readJson(requests.get(2).body()));
        assertEquals("DELETE", requests.get(3).method());
    }

    @Test
    public void cramMd5AuthenticationPropagatesEncodedFormDataAndSessionCookie() throws Exception
    {
        final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
        final AtomicInteger saslRequestCount = new AtomicInteger();
        final String requestId = "request id+/=";
        final String challenge = Base64.getEncoder().encodeToString("test challenge".getBytes(UTF_8));

        _server.createContext("/service/sasl", exchange ->
        {
            requests.add(capture(exchange));
            if (saslRequestCount.getAndIncrement() == 0)
            {
                exchange.getResponseHeaders().add("Set-Cookie", "JSESSIONID=test-session; Path=/; HttpOnly");
                respond(exchange, 200, OBJECT_MAPPER.writeValueAsString(
                        Map.of("challenge", challenge, "id", requestId)));
            }
            else
            {
                respond(exchange, 200, "{}");
            }
        });
        _server.createContext("/api/latest/broker", exchange ->
        {
            requests.add(capture(exchange));
            respond(exchange, 200, OBJECT_MAPPER.writeValueAsString(Map.of("name", "testBroker")));
        });
        _server.createContext("/service/logout", exchange ->
        {
            requests.add(capture(exchange));
            respond(exchange, 200, "");
        });

        final RestClient client = new RestClient(_baseUrl, "testUser", "testPassword", "CRAM-MD5");
        client.authenticateIfSaslAuthenticationRequested();
        assertEquals("testBroker", client.get("/api/latest/broker").get("name"));
        client.logout();

        assertEquals(4, requests.size());
        assertEquals(Map.of("mechanism", "CRAM-MD5"), decodeForm(requests.get(0).body()));
        assertEquals("application/x-www-form-urlencoded", requests.get(0).contentType());

        final Map<String, String> authenticationResponse = decodeForm(requests.get(1).body());
        assertEquals(requestId, authenticationResponse.get("id"));
        final String decodedResponse =
                new String(Base64.getDecoder().decode(authenticationResponse.get("response")), UTF_8);
        assertTrue(decodedResponse.matches("testUser [0-9a-f]{32}"));
        assertEquals("JSESSIONID=test-session", requests.get(1).cookie());
        assertEquals("JSESSIONID=test-session", requests.get(2).cookie());
        assertEquals("JSESSIONID=test-session", requests.get(3).cookie());
        assertTrue(requests.stream().allMatch(request -> request.authorization() == null));
    }

    @Test
    public void redirectsAreNotFollowedAndAuthenticationFailuresAreMapped() throws Exception
    {
        final AtomicBoolean redirectTargetRequested = new AtomicBoolean();
        _server.createContext("/redirect", exchange ->
        {
            exchange.getResponseHeaders().add("Location", "/redirect-target");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        _server.createContext("/redirect-target", exchange ->
        {
            redirectTargetRequested.set(true);
            respond(exchange, 200, OBJECT_MAPPER.writeValueAsString(Map.of("name", "unexpected")));
        });
        _server.createContext("/unauthorized", exchange -> respond(exchange, 401, ""));

        final RestClient client = new RestClient(_baseUrl, "testUser", "testPassword", null);
        final IOException redirectException = assertThrows(IOException.class, () -> client.get("/redirect"));
        assertTrue(redirectException.getMessage().contains("302"));
        assertFalse(redirectTargetRequested.get());

        final IllegalArgumentException authenticationException =
                assertThrows(IllegalArgumentException.class, () -> client.get("/unauthorized"));
        assertEquals("Authentication is required", authenticationException.getMessage());
    }

    @Test
    public void trustAllConfigurationDoesNotChangeJvmDefaultSslContext() throws Exception
    {
        final SSLContext defaultSslContext = SSLContext.getDefault();
        new RestClient("https://localhost", "testUser", "testPassword", null, true);
        assertSame(defaultSslContext, SSLContext.getDefault());
    }

    private CapturedRequest capture(final HttpExchange exchange) throws IOException
    {
        return new CapturedRequest(exchange.getRequestMethod(),
                                   exchange.getRequestURI().toString(),
                                   exchange.getProtocol(),
                                   exchange.getRequestHeaders().getFirst("Authorization"),
                                   exchange.getRequestHeaders().getFirst("Content-Type"),
                                   exchange.getRequestHeaders().getFirst("Cookie"),
                                   new String(exchange.getRequestBody().readAllBytes(), UTF_8));
    }

    private void respond(final HttpExchange exchange, final int responseCode, final String responseBody)
            throws IOException
    {
        final byte[] body = responseBody.getBytes(UTF_8);
        exchange.sendResponseHeaders(responseCode, body.length);
        try (final OutputStream output = exchange.getResponseBody())
        {
            output.write(body);
        }
        finally
        {
            exchange.close();
        }
    }

    private Map<String, String> decodeForm(final String form)
    {
        final Map<String, String> values = new LinkedHashMap<>();
        if (!form.isEmpty())
        {
            for (final String parameter : form.split("&"))
            {
                final String[] nameAndValue = parameter.split("=", 2);
                final String name = URLDecoder.decode(nameAndValue[0], UTF_8);
                final String value = nameAndValue.length == 1 ? "" : URLDecoder.decode(nameAndValue[1], UTF_8);
                values.put(name, value);
            }
        }
        return values;
    }

    private Map<String, Object> readJson(final String json) throws IOException
    {
        return OBJECT_MAPPER.readValue(json, TYPE_MAP);
    }

    private record CapturedRequest(String method,
                                   String target,
                                   String protocol,
                                   String authorization,
                                   String contentType,
                                   String cookie,
                                   String body)
    {
    }
}
