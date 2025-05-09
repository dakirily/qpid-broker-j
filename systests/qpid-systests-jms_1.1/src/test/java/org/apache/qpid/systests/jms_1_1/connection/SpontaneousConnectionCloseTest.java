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
package org.apache.qpid.systests.jms_1_1.connection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.systests.JmsTestBase;

public class SpontaneousConnectionCloseTest extends JmsTestBase
{
    private CompletableFuture<JMSException> _connectionCloseFuture;

    @BeforeEach
    public void setUp()
    {
        _connectionCloseFuture = new CompletableFuture<>();
    }

    @Test
    public void explictManagementConnectionClose() throws Exception
    {
        assumeTrue(getBrokerAdmin().isManagementSupported());

        Connection con = getConnection();
        try
        {
            con.setExceptionListener(_connectionCloseFuture::complete);

            final UUID uuid = getConnectionUUID(con);

            closeConnectionUsingAmqpManagement(uuid);

            assertClientConnectionClosed(con);
        }
        finally
        {
            con.close();
        }
    }

    @Test
    public void brokerRestartConnectionClose() throws Exception
    {
        Connection con = getConnectionBuilder().setFailover(false).build();
        try
        {
            con.setExceptionListener(_connectionCloseFuture::complete);

            getBrokerAdmin().restart();

            assertClientConnectionClosed(con);
        }
        finally
        {
            con.close();
        }
    }

    private void assertClientConnectionClosed(final Connection con) throws Exception
    {
        _connectionCloseFuture.get(getReceiveTimeout(), TimeUnit.MILLISECONDS);
        try
        {
            con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Connection close ought to invalidate further use");
        }
        catch (JMSException e)
        {
            // PASS
        }
    }

    private UUID getConnectionUUID(final Connection connection) throws Exception
    {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MapMessage message = session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.VirtualHost");
        message.setStringProperty("operation", "getConnectionMetaData");
        message.setStringProperty("index", "object-path");
        message.setStringProperty("key", "");

        MapMessage response = (MapMessage) sendManagementRequestAndGetResponse(session, message, 200);

        return UUID.fromString(String.valueOf(response.getObject("connectionId")));
    }

    private void closeConnectionUsingAmqpManagement(final UUID targetConnectionId) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MapMessage message = session.createMapMessage();
            message.setStringProperty("operation", "DELETE");
            message.setStringProperty("type", "org.apache.qpid.Connection");
            message.setStringProperty("identity", String.valueOf(targetConnectionId));

            sendManagementRequestAndGetResponse(session, message, 204);
        }
        finally
        {
            connection.close();
        }
    }

    private Message sendManagementRequestAndGetResponse(final Session session,
                                                        final MapMessage request,
                                                        final int expectedResponseCode) throws Exception
    {
        final Queue queue;
        final Queue replyConsumer;
        Queue replyAddress;

        if (getProtocol() == Protocol.AMQP_1_0)
        {
            queue = session.createQueue("$management");
            replyAddress = session.createTemporaryQueue();
            replyConsumer = replyAddress;
        }
        else
        {
            queue = session.createQueue("ADDR:$management");
            replyAddress = session.createQueue("ADDR:!response");
            replyConsumer = session.createQueue(
                    "ADDR:$management ; {assert : never, node: { type: queue }, link:{name: \"!response\"}}");
        }
        request.setJMSReplyTo(replyAddress);

        final MessageConsumer consumer = session.createConsumer(replyConsumer);
        final MessageProducer producer = session.createProducer(queue);

        producer.send(request);

        final Message responseMessage = consumer.receive(getReceiveTimeout());
        assertThat("The response code did not indicate success",
                   responseMessage.getIntProperty("statusCode"), is(equalTo(expectedResponseCode)));


        return responseMessage;
    }
}
