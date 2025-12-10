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

import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.tests.http.metrics.TestMetricsHelper.QUEUE_NAME;
import static org.apache.qpid.tests.http.metrics.TestMetricsHelper.assertMetricsInclusion;
import static org.apache.qpid.tests.http.metrics.TestMetricsHelper.assertVirtualHostHierarchyMetrics;
import static org.apache.qpid.tests.http.metrics.TestMetricsHelper.createQueueMetricPattern;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

/**
 * System tests for the Prometheus metrics HTTP endpoint exposed by the broker.
 * These tests validate naming, filtering, and context-variable driven behavior.
 */
@HttpRequestConfig(useVirtualHostAsHost = false)
public class BrokerMetricsTest extends HttpTestBase
{
    private static final String[] EXPECTED_BROKER_METRIC_NAMES =
            {
                    "qpid_broker_inbound_bytes_total", "qpid_broker_outbound_bytes_total",
                    "qpid_broker_inbound_message_size_high_watermark",
                    "qpid_broker_inbound_messages_total", "qpid_broker_inbound_transacted_messages_total",
                    "qpid_broker_number_of_buffers_in_pool", "qpid_broker_number_of_buffers_in_use"
            };

    /**
     * Verifies the default /metrics endpoint exports the expected broker metrics and respects includeDisabled defaults.
     */
    @Test
    public void testBrokerMetrics() throws Exception
    {
        final String[] unexpectedMetricNames =
                {"qpid_broker_live_threads", "qpid_broker_direct_memory_capacity_bytes"};

        final byte[] metricsBytes = getHelper().getBytes("/metrics");
        final String metricsString = new String(metricsBytes, UTF_8);
        assertMetricsInclusion(metricsString, EXPECTED_BROKER_METRIC_NAMES, true);
        assertMetricsInclusion(metricsString, unexpectedMetricNames, false);

        final byte[] metricsBytesIncludingDisabled = getHelper().getBytes("/metrics?includeDisabled=true");
        final String metricsStringIncludingDisabled = new String(metricsBytesIncludingDisabled, UTF_8);
        assertMetricsInclusion(metricsStringIncludingDisabled, unexpectedMetricNames, true);
        assertMetricsInclusion(metricsStringIncludingDisabled, EXPECTED_BROKER_METRIC_NAMES, true);
    }

    /**
     * Verifies that {@code qpid.metrics.prometheus.preserveMetricNameSuffix} matches both raw metric names and
     * fully qualified metric family names (case-insensitive and separator-insensitive), causing the exporter
     * to preserve suffixes such as {@code _total} when exporting disabled broker metrics.
     */
    @Test
    public void testBrokerMetricsWithPreserveMetricNameSuffix() throws Exception
    {
        // Ensure legacy naming mode is not enabled for the baseline assertion.
        getHelper().submitRequest("broker/removeContextVariable", "POST",
                Map.of("name", "qpid.metrics.prometheus.stripMetricNameSuffix"), SC_OK);

        final String[] strippedMetricNames =
                { "qpid_broker_live_threads", "qpid_broker_direct_memory_capacity_bytes" };
        final String[] preservedMetricNames =
                { "qpid_broker_live_threads_total", "qpid_broker_direct_memory_capacity_bytes_total" };

        // Baseline: includeDisabled=true should expose the stripped names by default.
        String metricsString = new String(getHelper().getBytes("/metrics?includeDisabled=true"), UTF_8);
        assertMetricsInclusion(metricsString, strippedMetricNames, true);
        assertMetricsInclusion(metricsString, preservedMetricNames, false);

        try
        {
            final Map<String, String> args = new HashMap<>();
            args.put("name", "qpid.metrics.prometheus.preserveMetricNameSuffix");
            // Mix raw and fully qualified forms using non-alphanumeric separators and mixed case.
            args.put("value", "live-threads-total, QPID_BROKER_DIRECT_MEMORY_CAPACITY_BYTES_TOTAL");
            getHelper().submitRequest("broker/setContextVariable", "POST", args, SC_OK);

            metricsString = new String(getHelper().getBytes("/metrics?includeDisabled=true"), UTF_8);
            assertMetricsInclusion(metricsString, preservedMetricNames, true);
            assertMetricsInclusion(metricsString, strippedMetricNames, false);
        }
        finally
        {
            getHelper().submitRequest("broker/removeContextVariable", "POST",
                    Map.of("name", "qpid.metrics.prometheus.preserveMetricNameSuffix"), SC_OK);
        }
    }

    /**
     * Verifies that {@code qpid.metrics.prometheus.stripMetricNameSuffix=false} enables legacy naming mode so
     * the exporter does not strip suffixes such as {@code _total} from non-counter metric names.
     */
    @Test
    public void testBrokerMetricsWithStripMetricNameSuffixFalseUsesLegacyNames() throws Exception
    {
        // Ensure per-metric preserve override is not set for this test.
        getHelper().submitRequest("broker/removeContextVariable", "POST",
                Map.of("name", "qpid.metrics.prometheus.preserveMetricNameSuffix"), SC_OK);

        final String[] strippedMetricNames =
                { "qpid_broker_live_threads", "qpid_broker_direct_memory_capacity_bytes" };
        final String[] preservedMetricNames =
                { "qpid_broker_live_threads_total", "qpid_broker_direct_memory_capacity_bytes_total" };

        try
        {
            final Map<String, String> args = new HashMap<>();
            args.put("name", "qpid.metrics.prometheus.stripMetricNameSuffix");
            args.put("value", "false");
            getHelper().submitRequest("broker/setContextVariable", "POST", args, SC_OK);

            final String metricsString = new String(getHelper().getBytes("/metrics?includeDisabled=true"), UTF_8);
            assertMetricsInclusion(metricsString, preservedMetricNames, true);
            assertMetricsInclusion(metricsString, strippedMetricNames, false);
        }
        finally
        {
            getHelper().submitRequest("broker/removeContextVariable", "POST",
                    Map.of("name", "qpid.metrics.prometheus.stripMetricNameSuffix"), SC_OK);
        }
    }


    /**
     * Verifies queue metrics are exported and include expected depth metrics for a created queue.
     */
    @Test
    public void testQueueMetrics() throws Exception
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
        final byte[] metricsBytes = getHelper().getBytes("/metrics");
        final String metricsString = new String(metricsBytes, UTF_8);

        final Pattern[] expectedMetricPattens = {createQueueMetricPattern("qpid_queue_consumers"),
                createQueueMetricPattern("qpid_queue_depth_messages")};

        assertMetricsInclusion(metricsString, expectedMetricPattens, true);
    }

    /**
     * Verifies filtering of queue metrics to only include message depth metrics when requested.
     */
    @Test
    public void testQueueMetricsIncludeOnlyMessageDepth() throws Exception
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
        final byte[] metricsBytes = getHelper().getBytes("/metrics?name[]=qpid_queue_depth_messages&name[]=qpid_queue_depth_bytes");
        Collection<String> metricLines = TestMetricsHelper.getMetricLines(metricsBytes);
        assertThat(metricLines.size(), is(equalTo(2)));

        final String metricsString = new String(metricsBytes, UTF_8);
        final Pattern[] expectedMetricPattens = {createQueueMetricPattern("qpid_queue_depth_bytes"),
                createQueueMetricPattern("qpid_queue_depth_messages")};

        assertMetricsInclusion(metricsString, expectedMetricPattens, true);
    }

    /**
     * Verifies virtual host scoped metrics mapping when requesting /metrics/<vhn>/<vh>.
     */
    @Test
    public void testMappingForVirtualHost() throws Exception
    {
        getBrokerAdmin().createQueue(QUEUE_NAME);
        final byte[] metricsBytes =
                getHelper().getBytes(String.format("/metrics/%s/%s", getVirtualHostNode(), getVirtualHost()));

        assertVirtualHostHierarchyMetrics(metricsBytes);
    }

    /**
     * Verifies name[] filters remove all metrics except the explicitly selected ones.
     */
    @Test
    public void testNameFilterCutsAllExceptSelectedMetrics() throws Exception
    {
        // Create some objects so the unfiltered endpoint definitely exposes lots of metrics
        getBrokerAdmin().createQueue(QUEUE_NAME);
        getBrokerAdmin().createQueue("bar");

        final String[] requestedMetricNames = {"qpid_queue_depth_messages", "qpid_queue_depth_bytes"};

        // Allow both the requested name and the Prometheus counter naming convention suffix.
        // This keeps the test focused on the name[] filtering behaviour rather than exact naming policy.
        final Set<String> allowedMetricNames = new HashSet<>();
        for (String name : requestedMetricNames)
        {
            allowedMetricNames.add(name);
            allowedMetricNames.add(name + "_total");
        }

        // Sanity check: unfiltered /metrics should contain at least one metric outside our allow-list
        final Set<String> unfilteredNames = new HashSet<>();
        for (String line : TestMetricsHelper.getMetricLines(getHelper().getBytes("/metrics")))
        {
            unfilteredNames.add(metricNameFromLine(line));
        }

        boolean hasOtherMetrics = false;
        for (String name : unfilteredNames)
        {
            if (!allowedMetricNames.contains(name))
            {
                hasOtherMetrics = true;
                break;
            }
        }
        assertThat("Unfiltered /metrics did not contain any other metrics; this test is not meaningful",
                hasOtherMetrics,
                is(true));

        // Now request only the selected metric families and ensure nothing else leaks through
        final byte[] filteredBytes =
                getHelper().getBytes("/metrics?name[]=qpid_queue_depth_messages&name[]=qpid_queue_depth_bytes");

        final Set<String> filteredNames = new HashSet<>();
        for (String line : TestMetricsHelper.getMetricLines(filteredBytes))
        {
            final String metricName = metricNameFromLine(line);
            filteredNames.add(metricName);

            assertThat("Unexpected metric in filtered response: " + metricName,
                    allowedMetricNames.contains(metricName),
                    is(true));
        }

        for (String requested : requestedMetricNames)
        {
            final boolean present = filteredNames.contains(requested) || filteredNames.contains(requested + "_total");
            assertThat("Expected metric missing in filtered response: " + requested,
                    present,
                    is(true));
        }
    }

    private static String metricNameFromLine(final String line)
    {
        final int brace = line.indexOf('{');
        final int space = line.indexOf(' ');

        final int end;
        if (brace >= 0 && space >= 0)
        {
            end = Math.min(brace, space);
        }
        else if (brace >= 0)
        {
            end = brace;
        }
        else if (space >= 0)
        {
            end = space;
        }
        else
        {
            end = line.length();
        }

        return line.substring(0, end);
    }
}
