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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.security.KeyStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.tls.TlsResourceHelper;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class HttpTestHelperTest extends UnitTestBase
{
    private static final String STORE_PASSWORD = "password";

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
}
