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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class OAuth2UtilsTest extends UnitTestBase
{
    @Test
    public void testBuildRequestQueryUsesFormEncoding()
    {
        final Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("space separated", "colon:value");
        parameters.put("reserved", "a/b+c");

        assertEquals("space+separated=colon%3Avalue&reserved=a%2Fb%2Bc",
                     OAuth2Utils.buildRequestQuery(parameters));
    }

    @Test
    public void testBuildBasicAuthorizationFormEncodesCredentials()
    {
        final String encodedCredentials = Base64.getEncoder()
                .encodeToString("client+id:secret%3Avalue%2Fpart".getBytes(UTF_8));

        assertEquals("Basic " + encodedCredentials,
                     OAuth2Utils.buildBasicAuthorization("client id", "secret:value/part"));
    }

    @Test
    public void testBuildBasicAuthorizationTreatsMissingSecretAsEmpty()
    {
        final String encodedCredentials = Base64.getEncoder().encodeToString("client:".getBytes(UTF_8));

        assertEquals("Basic " + encodedCredentials, OAuth2Utils.buildBasicAuthorization("client", null));
    }
}
