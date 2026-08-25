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
package org.apache.qpid.tests.http.compression;

import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.tests.http.HttpTestBase;

public class CompressedResponsesTest extends HttpTestBase
{
    @Test
    public void compressionOffAcceptOff() throws Exception
    {
        doCompressionTest(false, false);
    }

    @Test
    public void compressionOffAcceptOn() throws Exception
    {
        doCompressionTest(false, true);
    }

    @Test
    public void compressionOnAcceptOff() throws Exception
    {
        doCompressionTest(true, false);
    }

    @Test
    public void compressionOnAcceptOn() throws Exception
    {
        doCompressionTest(true, true);

    }

    private void doCompressionTest(final boolean allowCompression,
                                   final boolean acceptCompressed) throws Exception
    {
        final boolean expectCompression = allowCompression && acceptCompressed;

        getBrokerHelper().submitRequest("plugin/httpManagement",
                                        "POST",
                                        Map.of("compressResponses", expectCompression),
                                        SC_OK);


        final HttpRequest.Builder request = getBrokerHelper().createRequest("/service/metadata", "GET");
        if (acceptCompressed)
        {
            request.header("Accept-Encoding", "gzip");
        }
        final HttpResponse<byte[]> response = getBrokerHelper().send(request);
        final String contentEncoding = response.headers().firstValue("Content-Encoding").orElse(null);

        if (expectCompression)
        {
            assertEquals("gzip", contentEncoding);
        }
        else if (contentEncoding != null)
        {
            assertEquals("identity", contentEncoding);
        }

        try (InputStream jsonStream = expectCompression
                ? new GZIPInputStream(new ByteArrayInputStream(response.body()))
                : new ByteArrayInputStream(response.body()))
        {
            final ObjectMapper mapper = new ObjectMapper();
            try
            {
                mapper.readValue(jsonStream, LinkedHashMap.class);
            }
            catch (JacksonException e)
            {
                fail("Message was not in correct format");
            }
        }
    }
}
