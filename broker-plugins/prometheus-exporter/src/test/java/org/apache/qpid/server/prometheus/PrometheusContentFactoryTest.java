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

import static org.apache.qpid.server.prometheus.PrometheusContentFactory.INCLUDE_DISABLED;
import static org.apache.qpid.server.prometheus.PrometheusContentFactory.INCLUDE_DISABLED_CONTEXT_VARIABLE;
import static org.apache.qpid.server.prometheus.PrometheusContentFactory.INCLUDE_METRIC;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.testmodels.hierarchy.TestCar;
import org.apache.qpid.server.model.testmodels.hierarchy.TestKitCarImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestModel;

/**
 * Unit tests for {@link PrometheusContentFactory} verifying that broker
 * statistics are exposed as Prometheus text format via {@link Content}
 * instances.
 *
 * <p>The tests focus on:</p>
 * <ul>
 *   <li>creating non-null {@link Content} instances for a given root object,</li>
 *   <li>honouring the {@code includeDisabled} request parameter and context
 *       variable,</li>
 *   <li>honoring the {@code name[]} metric family filter, and</li>
 *   <li>ensuring that the plugin type identifier is correctly reported.</li>
 * </ul>
 */
/**
 * Unit tests for {@link PrometheusContentFactory} verifying query parameter handling and response formatting.
 */
public class PrometheusContentFactoryTest
{
    public static final String QPID_TEST_CAR_MILEAGE = "qpid_test_car_mileage_total";
    public static final String QPID_TEST_CAR_AGE = "qpid_test_car_age_total";
    public static final String PROMETHEUS_COMMENT = "#";

    private ConfiguredObject<?> _root;
    private PrometheusContentFactory _prometheusContentFactory;

    /**
     * Sets up a small test model consisting of a single {@link TestCar}
     * instance that serves as the root configured object for the content
     * factory.
     */
    @BeforeEach
    public void setUp()
    {
        final Model model = TestModel.getInstance();
        final Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, "MyPrometheusCar");
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        @SuppressWarnings("unchecked")
        final TestCar<?> car =
                model.getObjectFactory().create(TestCar.class, carAttributes, null);
        _root = car;
        _prometheusContentFactory = new PrometheusContentFactory();
    }

    /**
     * Verifies that {@link PrometheusContentFactory#createContent(ConfiguredObject, Map)}
     * returns non-null content and that the default behavior (no filters)
     * exposes only the enabled mileage metric for the car.
     *
     * <p>The test asserts that:</p>
     * <ul>
     *   <li>exactly one non-comment metric line is produced, and</li>
     *   <li>the line starts with the mileage metric family name.</li>
     * </ul>
     */
    /**
     * Verifies that the content factory renders a Prometheus text response for the default request.
     */
    @Test
    public void testCreateContent() throws Exception
    {
        final Content content = _prometheusContentFactory.createContent(_root, Map.of());
        assertThat(content, is(notNullValue()));

        final Collection<String> metrics = writeAndGetMetricLines(content);
        assertThat(metrics, is(notNullValue()));
        assertThat(metrics.size(), is(equalTo(1)));

        final String metric = metrics.iterator().next();
        assertThat(metric, startsWith(QPID_TEST_CAR_MILEAGE));
    }

    /**
     * Verifies that the {@code includeDisabled=true} request parameter causes
     * disabled statistics to be exported in addition to the default metrics.
     *
     * <p>The test asserts that:</p>
     * <ul>
     *   <li>both the mileage metric and the disabled age metric are present,</li>
     *   <li>no additional metric families are exposed.</li>
     * </ul>
     */
    /**
     * Verifies that the includeDisabled query parameter controls whether disabled statistics are exported.
     */
    @Test
    public void testCreateContentIncludeDisabled() throws Exception
    {
        final Content content = _prometheusContentFactory.createContent(
                _root, Map.of(INCLUDE_DISABLED, new String[]{"true"}));
        assertThat(content, is(notNullValue()));

        final Collection<String> metrics = writeAndGetMetricLines(content);
        assertThat(metrics, is(notNullValue()));
        assertThat(metrics.size(), is(equalTo(2)));

        final Map<String, String> metricsMap = convertMetricsToMap(metrics);
        assertThat(metricsMap.containsKey(QPID_TEST_CAR_MILEAGE), is(true));
        assertThat(metricsMap.containsKey(QPID_TEST_CAR_AGE), is(true));
    }

    /**
     * Verifies that the broker context variable
     * {@link PrometheusContentFactory#INCLUDE_DISABLED_CONTEXT_VARIABLE}
     * is honored when the {@code includeDisabled} request parameter is not
     * explicitly set to {@code true}.
     *
     * <p>The test sets the context variable to {@code true} on the root object
     * and asserts that both the mileage and age metrics are exported even
     * without an explicit request parameter.</p>
     */
    /**
     * Verifies that the includeDisabled default can be provided via broker context variable when the query parameter is absent.
     */
    @Test
    public void testCreateContentIncludeDisabledUsingContextVariable() throws Exception
    {
        _root.setContextVariable(INCLUDE_DISABLED_CONTEXT_VARIABLE, "true");
        final Content content = _prometheusContentFactory.createContent(_root, Map.of());
        assertThat(content, is(notNullValue()));

        final Collection<String> metrics = writeAndGetMetricLines(content);
        assertThat(metrics, is(notNullValue()));
        assertThat(metrics.size(), is(equalTo(2)));

        final Map<String, String> metricsMap = convertMetricsToMap(metrics);
        assertThat(metricsMap.containsKey(QPID_TEST_CAR_MILEAGE), is(true));
        assertThat(metricsMap.containsKey(QPID_TEST_CAR_AGE), is(true));
    }

    /**
     * Verifies that the {@code name[]} filter includes only the requested
     * metric family names when used together with {@code includeDisabled=true}.
     *
     * <p>The test requests that only the car age metric family is exported
     * and asserts that:</p>
     * <ul>
     *   <li>exactly one non-comment metric line is produced, and</li>
     *   <li>the line starts with the car age metric family name.</li>
     * </ul>
     */
    /**
     * Verifies that name[] query parameters restrict the exported metrics to the selected family names.
     */
    @Test
    public void testCreateContentIncludeName() throws Exception
    {
        final Map<String, String[]> filter = new HashMap<>();
        filter.put(INCLUDE_DISABLED, new String[]{ "true" });
        filter.put(INCLUDE_METRIC, new String[]{ "qpid_test_car_age" });

        final Content content = _prometheusContentFactory.createContent(_root, filter);
        assertThat(content, is(notNullValue()));

        final Collection<String> metrics = writeAndGetMetricLines(content);
        assertThat(metrics, is(notNullValue()));
        assertThat(metrics.size(), is(equalTo(1)));

        final String metric = metrics.iterator().next();
        assertThat(metric, startsWith(QPID_TEST_CAR_AGE));
    }

    /**
     * Verifies that the {@code name[]} filter can exclude all metrics when
     * it does not match any existing metric family name.
     *
     * <p>The test enables disabled metrics and then filters for a non-existent
     * metric family name; it asserts that no non-comment metric lines are
     * produced.</p>
     */
    /**
     * Verifies that a non-matching name[] filter results in an empty (or minimal) metrics response.
     */
    @Test
    public void testCreateContentWithNonMatchingNameFilter() throws Exception
    {
        final Map<String, String[]> filter = new HashMap<>();
        filter.put(INCLUDE_DISABLED, new String[]{"true"});
        filter.put(INCLUDE_METRIC, new String[]{"non_existing_metric_family"});

        final Content content = _prometheusContentFactory.createContent(_root, filter);
        assertThat(content, is(notNullValue()));

        final Collection<String> metrics = writeAndGetMetricLines(content);
        assertThat(metrics, is(notNullValue()));
        assertThat(metrics.isEmpty(), is(true));
    }

    /**
     * Verifies that {@link PrometheusContentFactory#getType()} returns the
     * expected plugin type identifier used by the Qpid pluggable
     * infrastructure.
     *
     * <p>The method should return the constant string {@code "metrics"}.</p>
     */
    /**
     * Verifies that the content factory exposes the correct content type for Prometheus text format.
     */
    @Test
    public void testGetType()
    {
        assertThat(_prometheusContentFactory.getType(), is(equalTo("metrics")));
    }

    /**
     * Writes the metrics produced by the given {@link Content} instance to an
     * in-memory output stream and returns the list of non-comment, non-empty
     * lines.
     *
     * <p>Lines starting with {@link #PROMETHEUS_COMMENT} or empty lines are
     * discarded, leaving only actual metric samples in the result.</p>
     *
     * @param content content whose output should be captured and parsed
     *
     * @return collection of metric lines (one per time series)
     *
     * @throws IOException if writing the content fails
     */
    private static Collection<String> writeAndGetMetricLines(final Content content) throws IOException
    {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream())
        {
            content.write(output);
            return getMetricLines(output.toByteArray());
        }
    }

    /**
     * Parses a byte array containing Prometheus text exposition format and
     * returns a list of metric sample lines, i.e. lines that are not comments
     * and not empty.
     *
     * <p>Comment lines (starting with {@link #PROMETHEUS_COMMENT}) and empty
     * lines are ignored.</p>
     *
     * @param metricsBytes byte array with Prometheus text formatted metrics
     *
     * @return collection of metric sample lines
     *
     * @throws IOException if reading from the in-memory stream fails
     */
    private static Collection<String> getMetricLines(final byte[] metricsBytes) throws IOException
    {
        final List<String> results = new ArrayList<>();
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(new ByteArrayInputStream(metricsBytes))))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (!(line.startsWith(PROMETHEUS_COMMENT) || line.isEmpty()))
                {
                    results.add(line);
                }
            }
        }
        return results;
    }

    /**
     * Converts a collection of Prometheus metric sample lines into a map
     * keyed by metric family name.
     *
     * <p>Each line is expected to follow the standard pattern
     * {@code <metric_name> <value>}, and the method splits on the first
     * whitespace, using the metric name as key and the value as map value.</p>
     *
     * @param metrics collection of metric sample lines
     *
     * @return map from metric name to its textual value
     */
    private Map<String, String> convertMetricsToMap(final Collection<String> metrics)
    {
        return metrics.stream()
                .map(m -> m.split(" "))
                .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));
    }
}
