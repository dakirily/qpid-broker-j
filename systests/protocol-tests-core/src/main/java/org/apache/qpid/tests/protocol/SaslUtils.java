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

package org.apache.qpid.tests.protocol;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class SaslUtils
{
    private static final HexFormat HEX_LOWER = HexFormat.of();

    public static byte[] generateCramMD5ClientResponse(final String userName,
                                                       final String userPassword,
                                                       final byte[] challengeBytes) throws Exception
    {
        final String macAlgorithm = "HmacMD5";
        final Mac mac = Mac.getInstance(macAlgorithm);
        mac.init(new SecretKeySpec(userPassword.getBytes(StandardCharsets.UTF_8), macAlgorithm));
        final byte[] messageAuthenticationCode = mac.doFinal(challengeBytes);
        final String responseAsString = userName + " " + HEX_LOWER.formatHex(messageAuthenticationCode);
        return responseAsString.getBytes();
    }
}
