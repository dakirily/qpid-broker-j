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
 *
 *
 */
package org.apache.qpid.server.logging.messages;

import static org.apache.qpid.server.logging.AbstractMessageLogger.DEFAULT_LOG_HIERARCHY_PREFIX;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

import org.slf4j.LoggerFactory;

import org.apache.qpid.server.logging.LogMessage;

/**
 * DO NOT EDIT DIRECTLY, THIS FILE WAS GENERATED.
 *
 * Generated using GenerateLogMessages and LogMessages.vm
 * This file is based on the content of Network_logmessages.properties
 *
 * To regenerate, use Maven lifecycle generates-sources with -Dgenerate=true
 */
public class NetworkMessages
{
    private static final ResourceBundle MESSAGES;
    private static final Locale CURRENT_LOCALE;

    static
    {
        Locale locale = Locale.US;
        String localeSetting = System.getProperty("qpid.broker_locale");
        if (localeSetting != null)
        {
            String[] localeParts = localeSetting.split("_");
            String language = (localeParts.length > 0 ? localeParts[0] : "");
            String country = (localeParts.length > 1 ? localeParts[1] : "");
            String variant = "";
            if (localeParts.length > 2)
            {
                variant = localeSetting.substring(language.length() + 1 + country.length() + 1);
            }
            locale = new Locale(language, country, variant);
        }
        CURRENT_LOCALE = locale;
    }

    public static final String NETWORK_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "network";
    public static final String CLOSED_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "network.closed";
    public static final String OPEN_LOG_HIERARCHY = DEFAULT_LOG_HIERARCHY_PREFIX + "network.open";

    static
    {
        LoggerFactory.getLogger(NETWORK_LOG_HIERARCHY);
        LoggerFactory.getLogger(CLOSED_LOG_HIERARCHY);
        LoggerFactory.getLogger(OPEN_LOG_HIERARCHY);

        MESSAGES = ResourceBundle.getBundle("org.apache.qpid.server.logging.messages.Network_logmessages", CURRENT_LOCALE);
    }

    /**
     * Log a Network message of the Format:
     * <pre>NET-1002 : Close : Destination : {0}({1}) : Source {2}</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage CLOSED(CharSequence param1, CharSequence param2, CharSequence param3)
    {
        String rawMessage = MESSAGES.getString("CLOSED");

        final Object[] messageArguments = {param1, param2, param3};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return CLOSED_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }

    /**
     * Log a Network message of the Format:
     * <pre>NET-1001 : Open : Destination : {0}({1}) : Source {2}</pre>
     * Optional values are contained in [square brackets] and are numbered
     * sequentially in the method call.
     *
     */
    public static LogMessage OPEN(CharSequence param1, CharSequence param2, CharSequence param3)
    {
        String rawMessage = MESSAGES.getString("OPEN");

        final Object[] messageArguments = {param1, param2, param3};
        // Create a new MessageFormat to ensure thread safety.
        // Sharing a MessageFormat and using applyPattern is not thread safe
        MessageFormat formatter = new MessageFormat(rawMessage, CURRENT_LOCALE);

        final String message = formatter.format(messageArguments);

        return new LogMessage()
        {
            @Override
            public String toString()
            {
                return message;
            }

            @Override
            public String getLogHierarchy()
            {
                return OPEN_LOG_HIERARCHY;
            }

            @Override
            public boolean equals(final Object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || getClass() != o.getClass())
                {
                    return false;
                }

                final LogMessage that = (LogMessage) o;

                return getLogHierarchy().equals(that.getLogHierarchy()) && toString().equals(that.toString());

            }

            @Override
            public int hashCode()
            {
                int result = toString().hashCode();
                result = 31 * result + getLogHierarchy().hashCode();
                return result;
            }
        };
    }


    private NetworkMessages()
    {
    }

}
