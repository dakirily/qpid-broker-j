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

package org.apache.qpid.server.prometheus;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.prometheus.metrics.config.EscapingScheme;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.RestContentHeader;
import org.apache.qpid.server.plugin.ContentFactory;
import org.apache.qpid.server.plugin.PluggableService;

/**
 * {@link ContentFactory} implementation that exposes Qpid Broker-J statistics
 * as Prometheus metrics using the client_java 1.x APIs.
 *
 * <p>The factory creates a {@link Content} instance that, when written,
 * collects statistics starting from the supplied root {@link ConfiguredObject}
 * using {@link QpidCollector} and renders them in Prometheus text format
 * via {@link PrometheusTextFormatWriter}.</p>
 *
 * <p>The following request parameters are honored via the {@code filter}
 * argument of {@link #createContent(ConfiguredObject, Map)}:</p>
 * <ul>
 *   <li>{@code includeDisabled} – if set to {@code true}, statistics that are
 *       disabled in the model are still exported. If omitted, the value can be
 *       overridden by the context variable
 *       {@value #INCLUDE_DISABLED_CONTEXT_VARIABLE} on the root object.</li>
 *   <li>{@code name[]} – optional list of metric <em>family</em> names to include.
 *       If omitted or empty, all metric families are exported.</li>
 * </ul>
 */
@PluggableService
public class PrometheusContentFactory implements ContentFactory
{
    static final String INCLUDE_DISABLED = "includeDisabled";
    static final String INCLUDE_METRIC = "name[]";
    static final String INCLUDE_DISABLED_CONTEXT_VARIABLE = "qpid.metrics.includeDisabled";

    /**
     * Creates {@link Content} that produces a Prometheus text exposition of
     * the broker statistics below the given root object.
     *
     * <p>The returned {@link Content} instance is lightweight; actual
     * collection of statistics happens once {@link Content#write(OutputStream)}
     * is invoked by the REST layer.</p>
     *
     * @param object
     *         root {@link ConfiguredObject} that serves as the starting point
     *         for statistics collection
     * @param filter
     *         map of request parameters controlling which metrics are included
     *         in the output; supported keys are described in the class-level
     *         Javadoc
     *
     * @return content that writes Prometheus text format for the configured
     *         object tree to the supplied {@link OutputStream}
     */
    @Override
    public Content createContent(final ConfiguredObject<?> object,
                                 final Map<String, String[]> filter)
    {
        final String[] includeDisabledValues = filter.get(INCLUDE_DISABLED);
        boolean includeDisabled = includeDisabledValues != null
                && includeDisabledValues.length == 1
                && Boolean.parseBoolean(includeDisabledValues[0]);

        // Fall back to the broker context variable if the request parameter is not explicitly set to true
        if (!includeDisabled)
        {
            final Boolean val = object.getContextValue(Boolean.class, INCLUDE_DISABLED_CONTEXT_VARIABLE);
            if (val != null)
            {
                includeDisabled = val;
            }
        }

        final String[] includedMetricNames = filter.get(INCLUDE_METRIC);

        final Set<String> allowedNames = includedMetricNames == null || includedMetricNames.length == 0
                ? Collections.emptySet()
                : new HashSet<>(Set.of(includedMetricNames));

        final IncludeMetricPredicate metricIncludeFilter = new IncludeMetricPredicate(allowedNames);

        final IncludeDisabledStatisticPredicate includeDisabledPredicate = new IncludeDisabledStatisticPredicate(includeDisabled);

        final QpidCollector qpidCollector = new QpidCollector(object, includeDisabledPredicate, metricIncludeFilter);

        return new Content()
        {
            /**
             * Collects the current broker statistics and writes them in
             * Prometheus text format to the provided output stream.
             *
             * @param outputStream
             *         stream to which the metrics are written
             *
             * @throws IOException
             *         if writing to the output stream fails
             */
            @Override
            public void write(final OutputStream outputStream) throws IOException
            {
                final PrometheusTextFormatWriter writer = PrometheusTextFormatWriter.create();
                writer.write(outputStream, qpidCollector.collect(), EscapingScheme.UNDERSCORE_ESCAPING);
            }

            /**
             * Releases any resources associated with this content instance.
             *
             * <p>The current implementation is stateless and therefore does
             * not need to perform any action here.</p>
             */
            @Override
            public void release()
            {
                // no-op
            }

            /**
             * Returns the HTTP {@code Content-Type} header value for the
             * Prometheus text exposition format produced by this content.
             *
             * @return content type string compatible with Prometheus text format
             */
            @RestContentHeader("Content-Type")
            public String getContentType()
            {
                return PrometheusTextFormatWriter.CONTENT_TYPE;
            }
        };
    }

    /**
     * Returns the plugin type identifier used by the Qpid pluggable infrastructure.
     *
     * @return the constant string {@code "metrics"}
     */
    @Override
    public String getType()
    {
        return "metrics";
    }
}
