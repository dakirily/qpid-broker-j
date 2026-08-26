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

package org.apache.qpid.server.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.test.utils.UnitTestBase;

public class HttpClientTransportTest extends UnitTestBase
{
    private static final URI HTTPS_URI = URI.create("https://example.org/resource");

    @Test
    public void testConfiguresSecureDefaults()
    {
        final HttpClientTransport transport = HttpClientTransport.newBuilder()
                .setConnectTimeout(500)
                .setRequestTimeout(1000)
                .build();

        assertEquals(HttpClient.Redirect.NEVER, transport.getHttpClient().followRedirects());
        assertEquals(Duration.ofMillis(500), transport.getHttpClient().connectTimeout().orElseThrow());
        assertEquals("HTTPS",
                     transport.getHttpClient().sslParameters().getEndpointIdentificationAlgorithm());
        assertEquals(Duration.ofMillis(1000),
                     transport.newRequestBuilder(HTTPS_URI).GET().build().timeout().orElseThrow());
    }

    @Test
    public void testRejectsInsecureUri()
    {
        final HttpClientTransport transport = HttpClientTransport.newBuilder().build();

        assertThrows(IllegalArgumentException.class,
                     () -> transport.newRequestBuilder(URI.create("http://example.org/resource")));
    }

    @Test
    public void testRejectsUriContainingCredentials()
    {
        final HttpClientTransport transport = HttpClientTransport.newBuilder().build();

        assertThrows(IllegalArgumentException.class,
                     () -> transport.newRequestBuilder(URI.create("https://user@example.org/resource")));
    }

    @ParameterizedTest
    @ValueSource(strings = { "https:/resource", "https://example.org/resource#fragment",
            "https://example.org:0/resource", "https://example.org:65536/resource" })
    public void testRejectsInvalidUri(final String uri)
    {
        final HttpClientTransport transport = HttpClientTransport.newBuilder().build();

        assertThrows(IllegalArgumentException.class, () -> transport.newRequestBuilder(URI.create(uri)));
    }

    @Test
    public void testRefreshesHttpClientWhenTrustManagersVersionChanges()
    {
        final AtomicLong version = new AtomicLong();
        final AtomicReference<TrustManager[]> trustManagers =
                new AtomicReference<>(new TrustManager[] { mock(X509TrustManager.class) });
        final HttpClientTransport transport = HttpClientTransport.newBuilder()
                .setTrustManagerSource(trustManagers::get, version::get)
                .build();
        final HttpClient initialClient = transport.getHttpClient();

        assertSame(initialClient, transport.getHttpClient());

        trustManagers.set(new TrustManager[] { mock(X509TrustManager.class) });
        version.incrementAndGet();

        final HttpClient refreshedClient = transport.getHttpClient();
        assertNotSame(initialClient, refreshedClient);
        assertNotSame(initialClient.sslContext(), refreshedClient.sslContext());
        assertSame(refreshedClient, transport.getHttpClient());
    }

    @Test
    public void testTrustManagerRefreshIsSharedByConcurrentRequests() throws Exception
    {
        final AtomicLong version = new AtomicLong();
        final AtomicLong supplierCalls = new AtomicLong();
        final CountDownLatch refreshStarted = new CountDownLatch(1);
        final CountDownLatch concurrentRequestsStarted = new CountDownLatch(2);
        final CountDownLatch continueRefresh = new CountDownLatch(1);
        final AtomicBoolean refreshPending = new AtomicBoolean();
        final Set<Thread> refreshThreads = ConcurrentHashMap.newKeySet();
        final TrustManager[] trustManagers = { mock(X509TrustManager.class) };
        final HttpClientTransport transport = HttpClientTransport.newBuilder()
                .setTrustManagerSource(() ->
                {
                    if (supplierCalls.incrementAndGet() == 2L)
                    {
                        refreshStarted.countDown();
                        try
                        {
                            if (!continueRefresh.await(5L, TimeUnit.SECONDS))
                            {
                                throw new AssertionError("Timed out waiting to continue the HTTP client refresh");
                            }
                        }
                        catch (InterruptedException e)
                        {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(
                                    "Interrupted while waiting to continue the HTTP client refresh", e);
                        }
                    }
                    return trustManagers;
                }, () ->
                {
                    if (refreshPending.get() && refreshThreads.add(Thread.currentThread()))
                    {
                        concurrentRequestsStarted.countDown();
                    }
                    return version.get();
                })
                .build();
        version.incrementAndGet();
        refreshPending.set(true);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try
        {
            final Future<HttpClient> firstClient = executor.submit(transport::getHttpClient);
            assertTrue(refreshStarted.await(5L, TimeUnit.SECONDS));
            final Future<HttpClient> secondClient = executor.submit(transport::getHttpClient);
            assertTrue(concurrentRequestsStarted.await(5L, TimeUnit.SECONDS));

            continueRefresh.countDown();

            assertSame(firstClient.get(5L, TimeUnit.SECONDS), secondClient.get(5L, TimeUnit.SECONDS));
            assertEquals(2L, supplierCalls.get());
        }
        finally
        {
            continueRefresh.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testRejectsTlsConfigurationThatDisablesAllProtocols()
    {
        assertThrows(IllegalConfigurationException.class,
                     () -> HttpClientTransport.newBuilder()
                             .setTlsProtocolDenyList(List.of(".*"))
                             .build());
    }

    @Test
    public void testLimitsResponseBodySize()
    {
        final HttpResponse.BodySubscriber<byte[]> subscriber =
                HttpClientTransport.newLimitedBodySubscriber(4);
        final TestSubscription subscription = new TestSubscription();
        subscriber.onSubscribe(subscription);

        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[5])));

        final CompletionException exception =
                assertThrows(CompletionException.class, () -> subscriber.getBody().toCompletableFuture().join());
        assertInstanceOf(IOException.class, exception.getCause());
        assertTrue(subscription.isCancelled());
    }

    @Test
    public void testMapsHttpTimeoutToSocketTimeout() throws Exception
    {
        final HttpTimeoutException cause = new HttpTimeoutException("request timed out");
        final HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.send(any(), any())).thenThrow(cause);
        final HttpClientTransport transport = new HttpClientTransport(httpClient, null);

        final SocketTimeoutException exception = assertThrows(
                SocketTimeoutException.class,
                () -> transport.send(HttpRequest.newBuilder(HTTPS_URI).GET().build()));

        assertSame(cause, exception.getCause());
    }

    @Test
    public void testRestoresInterruptStatus() throws Exception
    {
        assertFalse(Thread.interrupted());
        final InterruptedException cause = new InterruptedException("request interrupted");
        final HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.send(any(), any())).thenThrow(cause);
        final HttpClientTransport transport = new HttpClientTransport(httpClient, null);

        final InterruptedIOException exception = assertThrows(
                InterruptedIOException.class,
                () -> transport.send(HttpRequest.newBuilder(HTTPS_URI).GET().build()));

        assertSame(cause, exception.getCause());
        assertTrue(Thread.interrupted());
    }

    private static final class TestSubscription implements Flow.Subscription
    {
        private boolean _cancelled;

        @Override
        public void request(final long count)
        {
        }

        @Override
        public void cancel()
        {
            _cancelled = true;
        }

        private boolean isCancelled()
        {
            return _cancelled;
        }
    }
}
