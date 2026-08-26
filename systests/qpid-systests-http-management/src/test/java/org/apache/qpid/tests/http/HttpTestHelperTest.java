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
package org.apache.qpid.tests.http;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.http.HttpRequest;
import java.security.KeyStore;
import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class HttpTestHelperTest extends UnitTestBase
{
    private static final String STORE_PASSWORD = "password";
    private static final String REQUEST_TIMEOUT_PROPERTY = "qpid.resttest_request_timeout";

    private BrokerAdmin _brokerAdmin;

    @BeforeEach
    public void setUp()
    {
        _brokerAdmin = mock(BrokerAdmin.class);
        when(_brokerAdmin.getBrokerAddress(BrokerAdmin.PortType.HTTP_BROKER))
                .thenReturn(InetSocketAddress.createUnresolved("localhost", 8080));
    }

    @Test
    public void setTrustStoreFromDataUrl() throws Exception
    {
        final String store = TlsResourceHelper.createKeyStoreAsDataUrl(KeyStore.getDefaultType(),
                                                                       STORE_PASSWORD.toCharArray());
        final HttpTestHelper helper = new HttpTestHelper(_brokerAdmin);

        assertDoesNotThrow(() -> helper.setTrustStore(store, STORE_PASSWORD));
    }

    @Test
    public void requestTimeoutAppliesToRequest()
    {
        setTestSystemProperty(REQUEST_TIMEOUT_PROPERTY, "1234");

        final HttpTestHelper helper = new HttpTestHelper(_brokerAdmin);
        final HttpRequest request = helper.createRequest("broker", "GET").build();

        assertEquals(Duration.ofMillis(1234), request.timeout().orElseThrow());
    }

    @Test
    public void zeroRequestTimeoutDisablesTimeout()
    {
        setTestSystemProperty(REQUEST_TIMEOUT_PROPERTY, "0");

        final HttpTestHelper helper = assertDoesNotThrow(() -> new HttpTestHelper(_brokerAdmin));
        final HttpRequest request = helper.createRequest("broker", "GET").build();

        assertTrue(request.timeout().isEmpty());
    }

    @Test
    public void negativeRequestTimeoutIsRejected()
    {
        setTestSystemProperty(REQUEST_TIMEOUT_PROPERTY, "-1");

        final IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new HttpTestHelper(_brokerAdmin));

        assertTrue(exception.getMessage().contains(REQUEST_TIMEOUT_PROPERTY));
    }
}
