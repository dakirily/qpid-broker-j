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
 *   <li>{@code includeDisabled} – if set, controls whether statistics that are
 *       disabled in the model are exported. If omitted, the value can be
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
    static final PrometheusTextFormatWriter WRITER = PrometheusTextFormatWriter.create();

    @Override
    public Content createContent(final ConfiguredObject<?> object,
                                 final Map<String, String[]> filter)
    {
        final Boolean includeDisabled = resolveIncludeDisabled(object, filter);

        final String[] includedMetricNames = filter.get(INCLUDE_METRIC);
        final Set<String> allowedNames = toAllowedNames(includedMetricNames);

        final IncludeMetricPredicate metricIncludeFilter = new IncludeMetricPredicate(allowedNames);
        final IncludeDisabledStatisticPredicate includeDisabledPredicate =
                new IncludeDisabledStatisticPredicate(Boolean.TRUE.equals(includeDisabled));

        final QpidCollector qpidCollector = new QpidCollector(object, includeDisabledPredicate, metricIncludeFilter);

        return new Content()
        {
            @Override
            public void write(final OutputStream outputStream) throws IOException
            {
                WRITER.write(outputStream, qpidCollector.collect(), EscapingScheme.UNDERSCORE_ESCAPING);
            }

            @Override
            public void release()
            {
                // no-op
            }

            @RestContentHeader("Content-Type")
            public String getContentType()
            {
                return PrometheusTextFormatWriter.CONTENT_TYPE;
            }
        };
    }

    /**
     * Resolve includeDisabled from request parameters with a fallback to broker context
     * only when the request parameter is not present.
     */
    private static Boolean resolveIncludeDisabled(final ConfiguredObject<?> object,
                                                  final Map<String, String[]> filter)
    {
        final String[] includeDisabledValues = filter.get(INCLUDE_DISABLED);

        // If the request parameter is present, it has priority (even if it's "false").
        if (includeDisabledValues != null && includeDisabledValues.length > 0 && includeDisabledValues[0] != null)
        {
            return Boolean.parseBoolean(includeDisabledValues[0]);
        }

        // Fall back to context variable only when the request parameter is absent.
        final Boolean fromContext = object.getContextValue(Boolean.class, INCLUDE_DISABLED_CONTEXT_VARIABLE);
        return fromContext != null ? fromContext : Boolean.FALSE;
    }

    /**
     * Build a set of allowed metric family names from the request parameter values.
     * Filters out null/blank entries and tolerates duplicates.
     */
    private static Set<String> toAllowedNames(final String[] includedMetricNames)
    {
        if (includedMetricNames == null || includedMetricNames.length == 0)
        {
            return Collections.emptySet();
        }

        final Set<String> names = new HashSet<>();
        for (final String name : includedMetricNames)
        {
            if (name != null && !name.isBlank())
            {
                names.add(name);
            }
        }

        return names.isEmpty() ? Collections.emptySet() : names;
    }

    @Override
    public String getType()
    {
        return "metrics";
    }
}
