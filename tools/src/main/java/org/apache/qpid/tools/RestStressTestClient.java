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

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.tools.util.ArgumentsParser;

public class RestStressTestClient
{
    private static final char COMMAND_CONTINUATION = (char) 92;

    public static void main(final String[] args) throws Exception
    {
        final ArgumentsParser parser = new ArgumentsParser();
        final Arguments arguments;
        try
        {
            arguments = parser.parse(args, Arguments.class);
            arguments.validate();
        }
        catch (final IllegalArgumentException e)
        {
            System.out.println("Invalid argument:" + e.getMessage());
            parser.usage(Arguments.class, Arguments.REQUIRED);
            System.out.println("\nRun examples:");
            System.out.println("  Using Basic authentication:");
            printContinuedCommandLine("  java");
            printContinuedCommandLine("    -Djavax.net.ssl.trustStore=java_client_truststore.jks");
            printContinuedCommandLine("    -Djavax.net.ssl.trustStorePassword=password");
            printContinuedCommandLine("    org.apache.qpid.tools.RestStressTestClient");
            printContinuedCommandLine(
                    "      repetitions=10 brokerUrl=https://localhost:8081 username=admin password=admin");
            printContinuedCommandLine(
                    "      virtualHost=default virtualHostNode=default createQueue=true bindQueue=true");
            System.out.println("      deleteQueue=true uniqueQueues=true queueName=boo exchangeName=amq.fanout");
            System.out.println("  Using CRAM-MD5 SASL authentication:");
            printContinuedCommandLine("  java");
            printContinuedCommandLine("    org.apache.qpid.tools.RestStressTestClient saslMechanism=CRAM-MD5");
            printContinuedCommandLine(
                    "      repetitions=10 brokerUrl=http://localhost:8080 username=admin password=admin");
            printContinuedCommandLine(
                    "      virtualHost=default virtualHostNode=default createQueue=true bindQueue=true");
            System.out.println("      deleteQueue=true uniqueQueues=true queueName=boo exchangeName=amq.fanout");
            return;
        }

        final RestStressTestClient client = new RestStressTestClient();
        client.run(arguments);
    }

    private static void printContinuedCommandLine(final String commandLine)
    {
        System.out.println(commandLine + " " + COMMAND_CONTINUATION);
    }

    public void run(final Arguments arguments) throws IOException
    {
        log(arguments.toString());
        final RestClient client = new RestClient(arguments.getBrokerUrl(), arguments.getUsername(),
                                                 arguments.getPassword(), arguments.getSaslMechanism(),
                                                 arguments.isTrustAll());
        for (int i = 0; i < arguments.getRepetitions(); i++)
        {
            runIteration(arguments, i, client);
        }
    }

    private void runIteration(final Arguments arguments, final int iteration, final RestClient client)
            throws IOException
    {
        log("Iteration " + iteration);

        client.authenticateIfSaslAuthenticationRequested();
        try
        {
            final Map<String, Object> brokerData = client.get("/api/latest/broker?depth=0");
            log("    Connected to broker " + brokerData.get("name"));
            createAndBindQueueIfRequired(arguments, client, iteration);
        }
        finally
        {
            if (arguments.isLogout())
            {
                client.logout();
            }
        }
    }

    private void log(final String logMessage)
    {
        System.out.println(logMessage);
    }

    private void createAndBindQueueIfRequired(final Arguments arguments,
                                              final RestClient client,
                                              final int iteration) throws IOException
    {
        if (arguments.isCreateQueue())
        {
            final String virtualHostNode = arguments.getVirtualHostNode();
            final String virtualHost = arguments.getVirtualHost();
            String queueName = arguments.getQueueName();

            if (queueName == null)
            {
                queueName = "temp-queue-" + System.nanoTime();
            }
            else if (arguments.isUniqueQueues())
            {
                queueName = queueName + "-" + iteration;
            }

            createQueue(client, virtualHostNode, virtualHost, queueName);

            if (arguments.isBindQueue())
            {
                bindQueue(client, virtualHostNode, virtualHost, queueName, arguments.getExchangeName());
            }

            if (arguments.isDeleteQueue())
            {
                deleteQueue(client, virtualHostNode, virtualHost, queueName);
            }
        }
    }

    private void createQueue(final RestClient client,
                             final String virtualHostNode,
                             final String virtualHost,
                             final String queueName) throws IOException
    {
        log("    Create queue " + queueName);

        final String queueUrl = getQueueServiceUrl(virtualHostNode, virtualHost, queueName);
        final Map<String, Object> queueData = new HashMap<>();
        queueData.put("name", queueName);
        queueData.put("durable", true);

        final int result = client.put(queueUrl, queueData);

        if (result != RestClient.RESPONSE_PUT_CREATE_OK)
        {
            throw new RuntimeException(String.format("Failure (%d) to create queue '%s'", result, queueName));
        }
    }

    private String getQueueServiceUrl(final String virtualHostNode,
                                      final String virtualHost,
                                      final String queueName)
    {
        return "/api/latest/queue/" + virtualHostNode + "/" + virtualHost + "/" + queueName;
    }

    private void deleteQueue(final RestClient client,
                             final String virtualHostNode,
                             final String virtualHost,
                             final String queueName) throws IOException
    {
        log("    Delete queue " + queueName);
        final int result = client.delete(getQueueServiceUrl(virtualHostNode, virtualHost, queueName));
        if (result != RestClient.RESPONSE_PUT_UPDATE_OK)
        {
            throw new RuntimeException(String.format("Failure (%d) to delete queue '%s'", result, queueName));
        }
    }

    private void bindQueue(final RestClient client,
                           final String virtualHostNode,
                           final String virtualHost,
                           final String queueName,
                           final String exchangeName) throws IOException
    {
        final String resolvedExchangeName = exchangeName == null ? "amq.direct" : exchangeName;

        log("        Bind queue " + queueName + " to " + resolvedExchangeName + " using binding key " + queueName);

        final String path = "/api/latest/exchange/" + virtualHostNode + "/" + virtualHost + "/" +
                resolvedExchangeName + "/bind";

        final Map<String, String> bindingData = new HashMap<>();
        bindingData.put("bindingKey", queueName);
        bindingData.put("destination", queueName);

        final int result = client.post(path, bindingData);

        if (result != RestClient.RESPONSE_OK)
        {
            throw new RuntimeException(String.format("Failure (%d) to bind queue '%s' to exchange '%s'",
                                                     result, queueName, resolvedExchangeName));
        }
    }

    public static class RestClient
    {
        public static final int RESPONSE_PUT_CREATE_OK = 201;
        public static final int RESPONSE_PUT_UPDATE_OK = 200;
        public static final int RESPONSE_OK = 200;
        public static final int RESPONSE_AUTHENTICATION_REQUIRED = 401;

        private static final String CONTENT_TYPE = "Content-Type";
        private static final String JSON_CONTENT_TYPE = "application/json";
        private static final String FORM_CONTENT_TYPE = "application/x-www-form-urlencoded";
        private static final String SET_COOKIE_HEADER = "Set-Cookie";

        private static final TypeReference<LinkedHashMap<String, Object>> TYPE_HASH_MAP =
                new TypeReference<>()
                {
                };
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        private final HttpClient _httpClient;
        private final String _brokerUrl;
        private final String _username;
        private final String _password;
        private final String _saslMechanism;
        private final String _authorizationHeader;

        private List<String> _cookies;

        public RestClient(final String brokerUrl,
                          final String username,
                          final String password,
                          final String saslMechanism)
        {
            this(brokerUrl, username, password, saslMechanism, false);
        }

        public RestClient(final String brokerUrl,
                          final String username,
                          final String password,
                          final String saslMechanism,
                          final boolean trustAll)
        {
            _brokerUrl = normalizeBrokerUrl(brokerUrl);
            _username = Objects.requireNonNull(username, "Username must not be null");
            _password = Objects.requireNonNull(password, "Password must not be null");
            _saslMechanism = saslMechanism;
            _httpClient = createHttpClient(trustAll);

            if (saslMechanism == null)
            {
                final String credentials = _username + ":" + _password;
                _authorizationHeader = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(UTF_8));
            }
            else
            {
                _authorizationHeader = null;
            }
        }

        public Map<String, Object> get(final String restServiceUrl) throws IOException
        {
            final HttpRequest request = createRequest("GET", restServiceUrl, _cookies).build();
            final HttpResponse<byte[]> response = send(request, HttpResponse.BodyHandlers.ofByteArray());
            checkSuccessfulResponse(response);
            return OBJECT_MAPPER.readValue(response.body(), TYPE_HASH_MAP);
        }

        public int put(final String restServiceUrl, final Map<String, Object> attributes) throws IOException
        {
            final HttpRequest.Builder request = createRequest("PUT", restServiceUrl, _cookies);
            if (attributes != null)
            {
                final byte[] requestBody = OBJECT_MAPPER.writeValueAsBytes(attributes);
                request.header(CONTENT_TYPE, JSON_CONTENT_TYPE)
                       .method("PUT", HttpRequest.BodyPublishers.ofByteArray(requestBody));
            }

            final HttpResponse<Void> response = send(request.build(), HttpResponse.BodyHandlers.discarding());
            checkAuthentication(response);
            return response.statusCode();
        }

        public int delete(final String restServiceUrl) throws IOException
        {
            final HttpRequest request = createRequest("DELETE", restServiceUrl, _cookies).build();
            final HttpResponse<Void> response = send(request, HttpResponse.BodyHandlers.discarding());
            checkAuthentication(response);
            return response.statusCode();
        }

        public int post(final String restServiceUrl, final Map<String, String> postData) throws IOException
        {
            final byte[] requestBody = OBJECT_MAPPER.writeValueAsBytes(postData);
            final HttpRequest request = createRequest("POST", restServiceUrl, _cookies)
                    .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
                    .method("POST", HttpRequest.BodyPublishers.ofByteArray(requestBody))
                    .build();
            final HttpResponse<Void> response = send(request, HttpResponse.BodyHandlers.discarding());
            checkAuthentication(response);
            return response.statusCode();
        }

        public void authenticateIfSaslAuthenticationRequested() throws IOException
        {
            if (_saslMechanism == null)
            {
                // Basic authentication is sent with each request.
            }
            else if ("CRAM-MD5".equals(_saslMechanism))
            {
                _cookies = performCramMD5Authentication();
            }
            else
            {
                throw new IllegalArgumentException("Unsupported SASL mechanism: " + _saslMechanism);
            }
        }

        public void logout() throws IOException
        {
            if (_cookies != null)
            {
                final HttpRequest request = createRequest("GET", "/service/logout", _cookies).build();
                send(request, HttpResponse.BodyHandlers.discarding());
                _cookies = null;
            }

            // TODO: track sessions for Basic authentication so they can be logged out.
        }

        private List<String> performCramMD5Authentication() throws IOException
        {
            final HttpResponse<byte[]> challengeResponse =
                    postForm("/service/sasl", Map.of("mechanism", "CRAM-MD5"), null);
            checkSuccessfulResponse(challengeResponse);

            final List<String> cookies = challengeResponse.headers().allValues(SET_COOKIE_HEADER);
            final Map<String, Object> response = OBJECT_MAPPER.readValue(challengeResponse.body(), TYPE_HASH_MAP);
            final String challenge = (String) response.get("challenge");
            final String responseData =
                    generateResponseForChallengeAndCredentials(challenge, _username, _password);

            final Map<String, String> saslResponse = new HashMap<>();
            saslResponse.put("id", (String) response.get("id"));
            saslResponse.put("response", responseData);

            final HttpResponse<byte[]> authenticationResponse = postForm("/service/sasl", saslResponse, cookies);
            if (authenticationResponse.statusCode() != RESPONSE_OK)
            {
                throw new RuntimeException("Authentication failed");
            }
            return cookies;
        }

        private HttpResponse<byte[]> postForm(final String restServiceUrl,
                                              final Map<String, String> postData,
                                              final List<String> cookies) throws IOException
        {
            final String postParameters = getPostDataString(postData);
            final HttpRequest request = createRequest("POST", restServiceUrl, cookies)
                    .header(CONTENT_TYPE, FORM_CONTENT_TYPE)
                    .method("POST", HttpRequest.BodyPublishers.ofString(postParameters, UTF_8))
                    .build();
            return send(request, HttpResponse.BodyHandlers.ofByteArray());
        }

        private HttpRequest.Builder createRequest(final String method,
                                                  final String restServiceUrl,
                                                  final List<String> cookies)
        {
            final HttpRequest.Builder request = HttpRequest.newBuilder(createRequestUri(restServiceUrl))
                    .method(method, HttpRequest.BodyPublishers.noBody());
            applyCookies(request, cookies);
            if (_authorizationHeader != null)
            {
                request.header("Authorization", _authorizationHeader);
            }
            return request;
        }

        private URI createRequestUri(final String restServiceUrl)
        {
            Objects.requireNonNull(restServiceUrl, "REST service URL must not be null");
            final String pathSeparator = restServiceUrl.startsWith("/") ? "" : "/";
            return URI.create(_brokerUrl + pathSeparator + restServiceUrl);
        }

        private <T> HttpResponse<T> send(final HttpRequest request,
                                         final HttpResponse.BodyHandler<T> bodyHandler) throws IOException
        {
            try
            {
                return _httpClient.send(request, bodyHandler);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for HTTP response from " + request.uri(), e);
            }
        }

        private void checkAuthentication(final HttpResponse<?> response)
        {
            if (response.statusCode() == RESPONSE_AUTHENTICATION_REQUIRED)
            {
                _cookies = null;
                throw new IllegalArgumentException("Authentication is required");
            }
        }

        private void checkSuccessfulResponse(final HttpResponse<?> response) throws IOException
        {
            checkAuthentication(response);
            if (response.statusCode() < 200 || response.statusCode() >= 300)
            {
                throw new IOException("HTTP request to " + response.uri() +
                                      " failed with response code " + response.statusCode());
            }
        }

        private String generateResponseForChallengeAndCredentials(final String challenge,
                                                                  final String username,
                                                                  final String password)
        {
            try
            {
                final byte[] challengeBytes = decodeBase64(challenge);
                final String macAlgorithm = "HmacMD5";
                final Mac mac = Mac.getInstance(macAlgorithm);
                mac.init(new SecretKeySpec(password.getBytes(UTF_8), macAlgorithm));
                final byte[] messageAuthenticationCode = mac.doFinal(challengeBytes);
                final String responseAsString = username + " " + toHex(messageAuthenticationCode);
                return Base64.getEncoder().encodeToString(responseAsString.getBytes(UTF_8));
            }
            catch (final GeneralSecurityException | IllegalArgumentException e)
            {
                throw new IllegalArgumentException("Unexpected exception", e);
            }
        }

        public static byte[] decodeBase64(final String base64String)
        {
            final String normalized = base64String.replaceAll("\\s", "");
            if (!normalized.matches("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"))
            {
                throw new IllegalArgumentException("Cannot convert string '" + normalized +
                                                   "' to a byte[] - it does not appear to be base64 data");
            }

            return Base64.getDecoder().decode(normalized);
        }

        private String toHex(final byte[] data)
        {
            final StringBuilder hash = new StringBuilder();
            for (final byte value : data)
            {
                final String hex = Integer.toHexString(0xFF & value);
                if (hex.length() == 1)
                {
                    hash.append('0');
                }
                hash.append(hex);
            }
            return hash.toString();
        }

        private static String getPostDataString(final Map<String, String> postData)
        {
            if (postData == null)
            {
                return "";
            }
            return postData.entrySet().stream()
                    .map(entry -> encodeFormValue(entry.getKey()) + "=" + encodeFormValue(entry.getValue()))
                    .collect(Collectors.joining("&"));
        }

        private static String encodeFormValue(final String value)
        {
            return URLEncoder.encode(value, UTF_8);
        }

        private static void applyCookies(final HttpRequest.Builder request, final List<String> cookies)
        {
            if (cookies != null)
            {
                final String cookieHeader = cookies.stream()
                        .map(cookie -> cookie.split(";", 2)[0])
                        .collect(Collectors.joining("; "));
                if (!cookieHeader.isEmpty())
                {
                    request.header("Cookie", cookieHeader);
                }
            }
        }

        private static String normalizeBrokerUrl(final String brokerUrl)
        {
            Objects.requireNonNull(brokerUrl, "Broker URL must not be null");
            final URI uri = URI.create(brokerUrl);
            final String scheme = uri.getScheme();
            if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))
            {
                throw new IllegalArgumentException("Broker URL must use the HTTP or HTTPS scheme");
            }
            if (!uri.isAbsolute() || uri.getRawAuthority() == null)
            {
                throw new IllegalArgumentException("Broker URL must be absolute and include an authority");
            }
            if (uri.getRawQuery() != null || uri.getRawFragment() != null)
            {
                throw new IllegalArgumentException("Broker URL must not include a query or fragment");
            }
            return brokerUrl.endsWith("/") ? brokerUrl.substring(0, brokerUrl.length() - 1) : brokerUrl;
        }

        private static HttpClient createHttpClient(final boolean trustAll)
        {
            final HttpClient.Builder builder = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NEVER)
                    .version(HttpClient.Version.HTTP_1_1);
            if (trustAll)
            {
                builder.sslContext(createTrustAllSslContext());
            }
            return builder.build();
        }

        @SuppressWarnings("java:S4830")
        private static SSLContext createTrustAllSslContext()
        {
            final TrustManager[] trustAllCerts = new TrustManager[]
            {
                new X509TrustManager()
                {
                    @Override
                    public X509Certificate[] getAcceptedIssuers()
                    {
                        return new X509Certificate[0];
                    }

                    @Override
                    public void checkClientTrusted(final X509Certificate[] certs, final String authType)
                    {
                        // Explicit trust-all mode is intended only for stress-test environments.
                    }

                    @Override
                    public void checkServerTrusted(final X509Certificate[] certs, final String authType)
                    {
                        // Explicit trust-all mode is intended only for stress-test environments.
                    }
                }
            };

            try
            {
                final SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, trustAllCerts, new SecureRandom());
                return sslContext;
            }
            catch (final GeneralSecurityException e)
            {
                throw new IllegalStateException("Failed to configure trust-all trust manager", e);
            }
        }
    }

    public static class Arguments
    {
        private static final Set<String> REQUIRED = new HashSet<>(Arrays.asList("brokerUrl", "username", "password"));

        private final String brokerUrl = null;
        private final String username = null;
        private final String password = null;
        private final String saslMechanism = null;

        private final String virtualHostNode = null;
        private final String virtualHost = null;
        private final String queueName = null;
        private final String exchangeName = null;

        private final int repetitions = 1;

        private final boolean createQueue = false;
        private final boolean deleteQueue = false;
        private final boolean uniqueQueues = false;
        private final boolean bindQueue = false;

        private final boolean logout = true;

        private final boolean trustAll = false;

        public Arguments()
        {
        }

        public void validate()
        {
            if (brokerUrl == null || brokerUrl.isEmpty())
            {
                throw new IllegalArgumentException("Mandatory argument 'brokerUrl' is not specified");
            }

            if (username == null || username.isEmpty())
            {
                throw new IllegalArgumentException("Mandatory argument 'username' is not specified");
            }

            if (password == null || password.isEmpty())
            {
                throw new IllegalArgumentException("Mandatory argument 'password' is not specified");
            }

            if (createQueue)
            {
                if (virtualHostNode == null || virtualHostNode.isEmpty())
                {
                    throw new IllegalArgumentException(
                            "Virtual host node name needs to be specified for queue creation");
                }

                if (virtualHost == null || virtualHost.isEmpty())
                {
                    throw new IllegalArgumentException("Virtual host name needs to be specified for queue creation");
                }
            }
        }

        public String getUsername()
        {
            return username;
        }

        public String getPassword()
        {
            return password;
        }

        public String getVirtualHost()
        {
            return virtualHost;
        }

        public boolean isCreateQueue()
        {
            return createQueue;
        }

        public boolean isDeleteQueue()
        {
            return deleteQueue;
        }

        public boolean isUniqueQueues()
        {
            return uniqueQueues;
        }

        public String getQueueName()
        {
            return queueName;
        }

        public boolean isBindQueue()
        {
            return bindQueue;
        }

        public String getExchangeName()
        {
            return exchangeName;
        }

        public String getVirtualHostNode()
        {
            return virtualHostNode;
        }

        public int getRepetitions()
        {
            return repetitions;
        }

        public String getBrokerUrl()
        {
            return brokerUrl;
        }

        public String getSaslMechanism()
        {
            return saslMechanism;
        }

        public boolean isTrustAll()
        {
            return trustAll;
        }

        public boolean isLogout()
        {
            return logout;
        }

        @Override
        public String toString()
        {
            return "Arguments{" +
                    "brokerUrl='" + brokerUrl + '\'' +
                    ", username='" + username + '\'' +
                    ", password='<redacted>'" +
                    ", saslMechanism='" + saslMechanism + '\'' +
                    ", virtualHostNode='" + virtualHostNode + '\'' +
                    ", virtualHost='" + virtualHost + '\'' +
                    ", queueName='" + queueName + '\'' +
                    ", exchangeName='" + exchangeName + '\'' +
                    ", repetitions=" + repetitions +
                    ", createQueue=" + createQueue +
                    ", deleteQueue=" + deleteQueue +
                    ", uniqueQueues=" + uniqueQueues +
                    ", bindQueue=" + bindQueue +
                    ", trustAll=" + trustAll +
                    ", logout=" + logout +
                    '}';
        }
    }
}
