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

package org.apache.qpid.systests.jms_3_1.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Enumeration;

import jakarta.jms.ConnectionMetaData;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

/**
 * System tests focusing on ConnectionMetaData semantics.
 *
 * The tests in this class are aligned with:
 * - 6.1.6 "ConnectionMetaData"
 * - 3.5.11 "JMSXDeliveryCount"
 */
@JmsSystemTest
@Tag("connection")
class ConnectionMetaDataTest
{
    /**
     * Verifies that ConnectionMetaData reports a Jakarta Messaging 3.1 version
     * and provides basic provider information (6.1.6).
     */
    @Test
    void metadataProvidesJmsVersionAndProviderDetails(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            final ConnectionMetaData metaData = connection.getMetaData();
            assertNotNull(metaData, "ConnectionMetaData should not be null");

            assertEquals(3, metaData.getJMSMajorVersion(), "Unexpected JMS major version");
            assertEquals(1, metaData.getJMSMinorVersion(), "Unexpected JMS minor version");
            assertTrue(metaData.getJMSVersion().startsWith("3.1"),
                    "JMSVersion should start with 3.1");

            assertNotNull(metaData.getJMSProviderName(), "Provider name should be present");
            assertNotNull(metaData.getProviderVersion(), "Provider version should be present");
        }
    }

    /**
     * Verifies that JMSXDeliveryCount is listed as a supported JMSX property (3.5.11, 6.1.6).
     */
    @Test
    void metadataListsJmsxDeliveryCount(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().create())
        {
            final Enumeration<?> props = connection.getMetaData().getJMSXPropertyNames();
            boolean found = false;
            while (props.hasMoreElements())
            {
                if ("JMSXDeliveryCount".equals(props.nextElement()))
                {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "JMSXDeliveryCount should be listed in JMSX property names");
        }
    }
}
