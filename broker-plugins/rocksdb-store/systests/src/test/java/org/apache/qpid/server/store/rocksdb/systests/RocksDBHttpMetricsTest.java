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

package org.apache.qpid.server.store.rocksdb.systests;

import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.systests.JmsTestBase;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.ConfigItem;

@ConfigItem(name = "qpid.broker.rocksdb.enableStatistics", value = "true")
public class RocksDBHttpMetricsTest extends JmsTestBase
{
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
            new TypeReference<Map<String, Object>>() { };

    @BeforeAll
    public static void verifyStoreType()
    {
        String type = System.getProperty("virtualhostnode.type", "");
        assumeTrue("ROCKSDB".equalsIgnoreCase(type), "Tests require virtualhostnode.type=ROCKSDB");
    }

    @BeforeEach
    public void enableRestrictedHeaders()
    {
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
        assumeTrue(getBrokerAdmin().isManagementSupported(), "HTTP management is not supported");
    }

    @AfterEach
    public void clearRestrictedHeaders()
    {
        System.clearProperty("sun.net.http.allowRestrictedHeaders");
    }

    @Test
    public void rocksdbStatisticsReportedViaHttp() throws Exception
    {
        Map<String, Object> response = postJson("virtualhost/getRocksDBStatistics",
                                                Map.of("reset", Boolean.TRUE));

        assertNotNull(response, "Statistics response is null");
        assertTrue(response.containsKey("rocksdb.stats"), "Expected rocksdb.stats entry");
        assertTrue(response.get("rocksdb.stats") instanceof String, "rocksdb.stats should be a string");
    }

    private Map<String, Object> postJson(final String path, final Map<String, Object> payload) throws Exception
    {
        String urlPath = path.startsWith("/") ? path.substring(1) : path;
        int port = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.HTTP).getPort();
        URL url = new URL("http://localhost:" + port + "/api/latest/" + urlPath);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Host", getVirtualHostName());

        String credentials = getBrokerAdmin().getValidUsername() + ":" + getBrokerAdmin().getValidPassword();
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + encoded);

        ObjectMapper mapper = new ObjectMapper();
        try (OutputStream outputStream = connection.getOutputStream())
        {
            mapper.writeValue(outputStream, payload);
        }

        int responseCode = connection.getResponseCode();
        assertEquals(SC_OK, responseCode, "Unexpected response code from HTTP management");
        try (InputStream inputStream = connection.getInputStream())
        {
            return mapper.readValue(inputStream, MAP_TYPE_REF);
        }
        finally
        {
            connection.disconnect();
        }
    }
}
