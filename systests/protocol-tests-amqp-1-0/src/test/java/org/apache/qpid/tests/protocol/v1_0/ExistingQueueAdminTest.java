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
 *
 */

package org.apache.qpid.tests.protocol.v1_0;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Test;

import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminException;
import org.apache.qpid.tests.utils.BrokerAdminExtension;
import org.apache.qpid.tests.utils.QpidTestInfo;
import org.apache.qpid.tests.utils.QpidTestInfoExtension;

@ExtendWith({ BrokerAdminExtension.class, QpidTestInfoExtension.class })
public class ExistingQueueAdminTest
{
    @BeforeEach
    public void before(final BrokerAdmin brokerAdmin,
                       final QpidTestInfo testInfo)
    {
        brokerAdmin.createQueue(testInfo.methodName());
    }

    @Test
    public void createQueue(final BrokerAdmin brokerAdmin,
                            final QpidTestInfo testInfo)
    {
        final ExistingQueueAdmin queueAdmin = new ExistingQueueAdmin(testInfo.virtualHostName());
        queueAdmin.createQueue(brokerAdmin, testInfo.methodName());
    }

    @Test
    public void deleteQueue(final BrokerAdmin brokerAdmin,
                            final QpidTestInfo testInfo) throws Exception
    {
        final String[] messages = Utils.createTestMessageContents(2, testInfo.methodName());
        brokerAdmin.putMessageOnQueue(testInfo.methodName(), messages);

        final ExistingQueueAdmin queueAdmin = new ExistingQueueAdmin(testInfo.virtualHostName());
        queueAdmin.deleteQueue(brokerAdmin, testInfo.methodName());

        final String controlMessage = String.format("controlMessage %s", testInfo.methodName());
        brokerAdmin.putMessageOnQueue(testInfo.methodName(), controlMessage);
        assertEquals(controlMessage, Utils.receiveMessage(brokerAdmin, testInfo));
    }

    @Test
    public void deleteQueueNonExisting(final BrokerAdmin brokerAdmin, final QpidTestInfo testInfo)
    {
        try
        {
            final ExistingQueueAdmin queueAdmin = new ExistingQueueAdmin(testInfo.virtualHostName());
            queueAdmin.deleteQueue(brokerAdmin, testInfo.methodName() + "_NonExisting");
            fail("Exception is expected");
        }
        catch (BrokerAdminException e)
        {
            // pass
        }
    }

    @Test
    public void putMessageOnQueue(final BrokerAdmin brokerAdmin,
                                  final QpidTestInfo testInfo) throws Exception
    {
        final String[] messages = Utils.createTestMessageContents(2, testInfo.methodName());
        final ExistingQueueAdmin queueAdmin = new ExistingQueueAdmin(testInfo.virtualHostName());
        queueAdmin.putMessageOnQueue(brokerAdmin, testInfo.methodName(), messages);
        assertEquals(messages[0], Utils.receiveMessage(brokerAdmin, testInfo));
        assertEquals(messages[1], Utils.receiveMessage(brokerAdmin, testInfo));
    }

    @Test
    public void putMessageOnQueueNonExisting(final BrokerAdmin brokerAdmin,
                                             final QpidTestInfo testInfo)
    {
        final String[] messages = Utils.createTestMessageContents(2, testInfo.methodName());
        try
        {
            final ExistingQueueAdmin queueAdmin = new ExistingQueueAdmin(testInfo.virtualHostName());
            queueAdmin.putMessageOnQueue(brokerAdmin, testInfo.methodName() + "_NonExisting", messages);
            fail("Exception is expected"); }
        catch (BrokerAdminException e)
        {
            // pass
        }
    }

    @Test
    public void isDeleteQueueSupported(final QpidTestInfo testInfo)
    {
        final ExistingQueueAdmin queueAdmin = new ExistingQueueAdmin(testInfo.virtualHostName());
        assertFalse(queueAdmin.isDeleteQueueSupported());
    }

    @Test
    public void isPutMessageOnQueueSupported(final QpidTestInfo testInfo)
    {
        final ExistingQueueAdmin queueAdmin = new ExistingQueueAdmin(testInfo.virtualHostName());
        assertTrue(queueAdmin.isPutMessageOnQueueSupported());
    }
}
