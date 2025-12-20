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

package org.apache.qpid.systests.support;

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;

import java.util.ArrayList;
import java.util.List;

public class MessagesSupport
{
    public static final String INDEX = "index";

    private static final int DEFAULT_MESSAGE_SIZE = 1024;
    private static final String DEFAULT_MESSAGE_PAYLOAD = "x".repeat(Math.max(0, DEFAULT_MESSAGE_SIZE));

    public MessagesSupport()
    {
        // ignore
    }

    public void send(final Connection connection,
                     final Destination destination,
                     final String message) throws JMSException
    {
        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(destination))
        {
            producer.send(session.createTextMessage(message));
        }
    }

    public void send(final Connection connection,
                     final Destination destination,
                     final int count) throws JMSException
    {
        try (final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE))
        {
            send(session, destination, count);
        }
    }

    public List<Message> send(final Session session,
                              final Destination destination,
                              final int count) throws JMSException
    {
        final List<Message> messages = new ArrayList<>(count);
        try (final var producer = session.createProducer(destination))
        {
            for (int i = 0; i < (count); i++)
            {
                final var next = createNext(session, i);
                producer.send(next);
                messages.add(next);
            }

            if (session.getTransacted())
            {
                session.commit();
            }

            return messages;
        }
    }

    public Message createNext(final Session session, final int msgCount) throws JMSException
    {
        final var message = create(session, DEFAULT_MESSAGE_SIZE);
        message.setIntProperty(INDEX, msgCount);
        return message;
    }

    public Message create(final Session session, final int messageSize) throws JMSException
    {
        final String payload = messageSize == DEFAULT_MESSAGE_SIZE
                ? DEFAULT_MESSAGE_PAYLOAD
                : "x".repeat(Math.max(0, messageSize));
        return session.createTextMessage(payload);
    }
}
