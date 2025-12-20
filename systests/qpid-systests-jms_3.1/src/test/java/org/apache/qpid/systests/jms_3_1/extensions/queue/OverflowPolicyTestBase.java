/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.qpid.systests.jms_3_1.extensions.queue;

import org.apache.qpid.systests.support.JmsSupport;

import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;

import static org.apache.qpid.systests.support.MessagesSupport.INDEX;

class OverflowPolicyTestBase
{
    private final byte[] BYTE_300 = new byte[300];

    protected String getFlowResumeLimit(final double maximumCapacity, final double resumeCapacity)
    {
        double ratio = resumeCapacity / maximumCapacity;
        return "%.2f".formatted(ratio * 100.0);
    }

    protected long evaluateMessageSize(final JmsSupport jms) throws Exception
    {
        final var tmpQueue = jms.builder().queue().nameSuffix("_Tmp").create();
        try (final var connection = jms.builder().connection().create())
        {
            connection.start();
            final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final var tmpQueueProducer = session.createProducer(tmpQueue);
            tmpQueueProducer.send(nextMessage(0, session));
            session.commit();
            return jms.queue(tmpQueue.getQueueName()).depthBytes();
        }
    }

    protected Message nextMessage(int index, Session producerSession) throws JMSException
    {
        final BytesMessage send = producerSession.createBytesMessage();
        send.writeBytes(BYTE_300);
        send.setIntProperty(INDEX, index);
        return send;
    }
}
