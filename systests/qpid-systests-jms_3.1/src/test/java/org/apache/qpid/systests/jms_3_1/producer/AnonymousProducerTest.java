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

package org.apache.qpid.systests.jms_3_1.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.JMSException;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.qpid.systests.Timeouts;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.qpid.systests.support.JmsSupport;
import org.apache.qpid.systests.JmsSystemTest;

@JmsSystemTest
@Tag("topic")
class AnonymousProducerTest
{
    @Test
    void publishIntoDestinationBoundWithNotMatchingFilter(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var messageProducer = session.createProducer(null);
             final var consumer = session.createConsumer(topic, "id>1"))
        {
            final var notMatching = session.createTextMessage("notMatching");
            notMatching.setIntProperty("id", 1);
            messageProducer.send(topic, notMatching);

            final var matching = session.createTextMessage("Matching");
            matching.setIntProperty("id", 2);
            messageProducer.send(topic, matching);
            session.commit();

            connection.start();
            final var message = consumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Expected message not received");
            assertEquals("Matching", textMessage.getText(), "Unexpected text");
        }
    }

    @Test
    void publishIntoNonExistingTopic(final JmsSupport jms) throws Exception
    {
        final var topic = jms.builder().topic().create();

        try (final var connection = jms.builder().connection().create();
             final var session = connection.createSession(true, Session.SESSION_TRANSACTED);
             final var messageProducer = session.createProducer(null))
        {
            messageProducer.send(topic, session.createTextMessage("A"));
            session.commit();

            connection.start();
            final var consumer = session.createConsumer(topic);
            messageProducer.send(topic, session.createTextMessage("B"));
            session.commit();

            final var message = consumer.receive(Timeouts.receiveMillis());
            final var textMessage = assertInstanceOf(TextMessage.class, message, "Expected message not received");
            assertEquals("B", textMessage.getText(), "Unexpected text");
        }
    }

    @Test
    void syncPublishIntoNonExistingQueue(final JmsSupport jms) throws Exception
    {
        try (final var connection = jms.builder().connection().syncPublish(true).create();
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             final var producer = session.createProducer(null))
        {
            final var queue = session.createQueue("nonExistingQueue");

            assertThrows(JMSException.class,
                    () -> producer.send(queue, session.createTextMessage("hello")),
                    "Send to unknown destination should result in error");
        }
    }

    @Test
    void unidentifiedDestination(final JmsSupport jms) throws Exception
    {
       try (final var connection =  jms.builder().connection().create();
            final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final var publisher = session.createProducer(null))
       {
           assertThrows(UnsupportedOperationException.class,
                   () -> publisher.send(session.createTextMessage("Test")),
                   "Did not throw UnsupportedOperationException");
       }
   }
}
