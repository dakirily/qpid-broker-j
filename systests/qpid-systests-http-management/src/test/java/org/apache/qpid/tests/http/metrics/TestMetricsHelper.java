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

package org.apache.qpid.tests.http.metrics;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

class TestMetricsHelper
{
    static final String QUEUE_NAME = "foo";

    // Prometheus text format sample value can be a number or special values like NaN, +Inf, -Inf.
    // Timestamp (integer) is optional and can follow the value.
    private static final String SAMPLE_VALUE_REGEX =
            "[-+]?(?:NaN|Inf(?:inity)?|(?:[0-9]+(?:\\.[0-9]+)?|\\.[0-9]+)(?:[eE][-+]?[0-9]+)?)";
    private static final String SAMPLE_WITH_OPTIONAL_TIMESTAMP_REGEX =
            "\\s+" + SAMPLE_VALUE_REGEX + "(?:\\s+[-+]?[0-9]+)?\\s*$";

    static void assertMetricsInclusion(final String metricsString,
                                       final String[] metricNames,
                                       final boolean inclusionFlag)
    {
        for (String metricName : metricNames)
        {
            final boolean present = metricFamilyPresent(metricsString, metricName);
            assertThat("Metric presence did not match expectation for '" + metricName + "'",
                    present,
                    equalTo(inclusionFlag));
        }
    }

    static void assertMetricsInclusion(final String metricsString,
                                       final Pattern[] expectedMetricPattens,
                                       final boolean inclusionFlag)
    {
        for (Pattern expected : expectedMetricPattens)
        {
            final boolean present = expected.matcher(metricsString).find();
            assertThat("Metric pattern presence did not match expectation for '" + expected.pattern() + "'",
                    present,
                    equalTo(inclusionFlag));
        }
    }

    /**
     * Matches a queue metric sample line with queue name label = QUEUE_NAME.
     * Anchors at line start to avoid false positives where metricName is a prefix of another metric name.
     */
    static Pattern createQueueMetricPattern(final String metricName)
    {
        final String quotedMetric = Pattern.quote(metricName);
        final String quotedQueueName = Pattern.quote(QUEUE_NAME);

        // Example: qpid_queue_depth_messages{name="foo",...} 123
        // - anchor to start of line
        // - require '{' right after metric name (labels exist in these systests)
        // - require name="foo" somewhere inside the label set
        // - accept any numeric/special value (and optional timestamp)
        final String regex =
                "(?m)^"
                        + quotedMetric
                        + "\\{[^}]*\\bname\\s*=\\s*\""
                        + quotedQueueName
                        + "\"[^}]*}"
                        + SAMPLE_WITH_OPTIONAL_TIMESTAMP_REGEX;

        return Pattern.compile(regex);
    }

    static void assertVirtualHostHierarchyMetrics(final byte[] metricsBytes) throws IOException
    {
        final Predicate<String> unexpectedMetricPredicate = line -> !(line.startsWith("qpid_virtual_host")
                || line.startsWith("qpid_queue")
                || line.startsWith("qpid_exchange"));
        getMetricLines(metricsBytes).stream().filter(unexpectedMetricPredicate)
                .findFirst().ifPresent(found -> fail("Unexpected metric: " + found));
    }

    static Collection<String> getMetricLines(final byte[] metricsBytes) throws IOException
    {
        final List<String> results = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(metricsBytes))))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (!(line.startsWith("#") || line.isEmpty()))
                {
                    results.add(line);
                }
            }
        }
        return results;
    }

    /**
     * Returns true if a metric family/sample line exists for the given metric name.
     * Anchored at line start and requires either '{' (labels) or whitespace after the metric name.
     */
    private static boolean metricFamilyPresent(final String metricsString, final String metricName)
    {
        final Pattern pattern = Pattern.compile("(?m)^" + Pattern.quote(metricName) + "(?:\\{|\\s)");
        return pattern.matcher(metricsString).find();
    }
}
