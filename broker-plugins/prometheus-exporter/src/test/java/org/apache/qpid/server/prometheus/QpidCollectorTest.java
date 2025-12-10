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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectStatistic;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.StatisticType;
import org.apache.qpid.server.model.StatisticUnit;
import org.apache.qpid.server.model.testmodels.hierarchy.TestAbstractEngineImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestCar;
import org.apache.qpid.server.model.testmodels.hierarchy.TestDigitalInstrumentPanelImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestElecEngineImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestEngine;
import org.apache.qpid.server.model.testmodels.hierarchy.TestInstrumentPanel;
import org.apache.qpid.server.model.testmodels.hierarchy.TestKitCarImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestModel;
import org.apache.qpid.server.model.testmodels.hierarchy.TestPetrolEngineImpl;
import org.apache.qpid.server.model.testmodels.hierarchy.TestSensor;
import org.apache.qpid.server.model.testmodels.hierarchy.TestTemperatureSensorImpl;
import org.apache.qpid.test.utils.UnitTestBase;

/**
 * Unit tests for {@link QpidCollector} verifying that broker statistics are
 * correctly exposed as Prometheus {@link MetricSnapshots} using the client_java 1.x API.
 *
 * <p>The tests exercise different shapes of the
 * {@link org.apache.qpid.server.model.ConfiguredObject} tree (single child,
 * nested children, siblings) and ensure that:</p>
 *
 * <ul>
 *   <li>metric family names are generated as expected,</li>
 *   <li>the correct number of data points is produced for each family,</li>
 *   <li>label names and values reflect the configured object hierarchy,</li>
 *   <li>filters on statistics and metric families are honored, and</li>
 *   <li>metric families use the correct snapshot type
 *       ({@link CounterSnapshot} for cumulative metrics and
 *       {@link GaugeSnapshot} for point-in-time metrics).</li>
 * </ul>
 */
public class QpidCollectorTest extends UnitTestBase
{
    private static final String CAR_NAME = "myCar";
    private static final String ELECTRIC_ENGINE_NAME = "myEngine";
    private static final String INSTRUMENT_PANEL_NAME = "instrumentPanel";
    private static final String PETROL_ENGINE_NAME = "myPetrolModel";
    private static final String SENSOR = "sensor";
    private static final int DESIRED_MILEAGE = 100;
    private static final String QPID_TEST_CAR_MILEAGE = "qpid_test_car_mileage";
    private static final String QPID_TEST_ENGINE_TEMPERATURE = "qpid_test_engine_temperature";
    private static final String QPID_TEST_SENSOR_ALERT_COUNT = "qpid_test_sensor_alert_count";
    private static final String QPID_TEST_CAR_AGE = "qpid_test_car_age";

    private TestCar<?> _root;
    private QpidCollector _qpidCollector;

    private static final StatisticUnit[] UNITS = new StatisticUnit[]{
            StatisticUnit.BYTES,
            StatisticUnit.MESSAGES,
            StatisticUnit.COUNT,
            StatisticUnit.ABSOLUTE_TIME,
            StatisticUnit.TIME_DURATION };

    private static final String[] UNIT_SUFFIXES = new String[]{ "_bytes", "_messages", "", "_timestamp_seconds", "_seconds" };

    /**
     * Creates a small test model tree with a single {@link TestCar} instance
     * and initializes a {@link QpidCollector} for it.
     *
     * <p>The collector is configured to skip disabled statistics and to include
     * all metric families.</p>
     */
    @BeforeEach
    public void setUp()
    {
        final Model model = TestModel.getInstance();
        final Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, CAR_NAME);
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);

        _root = model.getObjectFactory().create(TestCar.class, carAttributes, null);
        _qpidCollector = new QpidCollector(_root, new IncludeDisabledStatisticPredicate(false), s -> true);
    }

    /**
     * Verifies that metrics are collected correctly for a simple hierarchy
     * consisting of the root car and a single engine.
     *
     * <p>The test asserts that:</p>
     * <ul>
     *   <li>both the mileage and engine temperature metric families are exported,</li>
     *   <li>the mileage counter has a single unlabeled sample with the distance
     *       moved by the car, and</li>
     *   <li>the engine temperature gauge has one sample labeled with the engine name.</li>
     * </ul>
     */
    @Test
    public void testCollectForHierarchyOfTwoObjects()
    {
        createTestEngine(ELECTRIC_ENGINE_NAME, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        _root.move(DESIRED_MILEAGE);

        final MetricSnapshots metrics = _qpidCollector.collect();

        final String[] expectedFamilyNames =
                {QPID_TEST_CAR_MILEAGE, QPID_TEST_ENGINE_TEMPERATURE};
        final Map<String, MetricSnapshot> metricsMap =
                convertMetricSnapshotsIntoMapAndAssert(metrics, expectedFamilyNames);

        final MetricSnapshot carMetricSnapshot = metricsMap.get(QPID_TEST_CAR_MILEAGE);
        assertMetricSnapshotDataPointsSize(carMetricSnapshot, 1);
        final DataPointSnapshot carDataPoint = carMetricSnapshot.getDataPoints().get(0);
        assertThat(carDataPoint.getLabels().isEmpty(), is(true));
        assertThat(getDataPointValue(carDataPoint), closeTo(DESIRED_MILEAGE, 0.01));

        final MetricSnapshot engineMetricSnapshot = metricsMap.get(QPID_TEST_ENGINE_TEMPERATURE);
        assertMetricSnapshotDataPointsSize(engineMetricSnapshot, 1);
        final DataPointSnapshot engineDataPoint = engineMetricSnapshot.getDataPoints().get(0);
        final Labels engineLabels = engineDataPoint.getLabels();
        assertThat(extractLabelNames(engineLabels), is(equalTo(List.of("name"))));
        assertThat(extractLabelValues(engineLabels), is(equalTo(List.of(ELECTRIC_ENGINE_NAME))));
        assertThat(getDataPointValue(engineDataPoint),
                closeTo(TestAbstractEngineImpl.TEST_TEMPERATURE, 0.01));
    }

    /**
     * Verifies metric collection for a three-level hierarchy:
     * car → instrument panel → sensor.
     *
     * <p>The test asserts that:</p>
     * <ul>
     *   <li>the car mileage metric family is exported with a single unlabeled
     *       sample with value 0, and</li>
     *   <li>the sensor alert counter is exported with a single sample labeled
     *       by both the sensor name and its parent instrument panel name.</li>
     * </ul>
     */
    @Test
    public void testCollectForHierarchyOfThreeObjects()
    {
        final TestInstrumentPanel<?> instrumentPanel = getTestInstrumentPanel();
        createTestSensor(instrumentPanel);

        final MetricSnapshots metrics = _qpidCollector.collect();

        final String[] expectedFamilyNames =
                {QPID_TEST_CAR_MILEAGE, QPID_TEST_SENSOR_ALERT_COUNT};
        final Map<String, MetricSnapshot> metricsMap =
                convertMetricSnapshotsIntoMapAndAssert(metrics, expectedFamilyNames);

        final MetricSnapshot carMetricSnapshot = metricsMap.get(QPID_TEST_CAR_MILEAGE);
        assertMetricSnapshotDataPointsSize(carMetricSnapshot, 1);
        final DataPointSnapshot carDataPoint = carMetricSnapshot.getDataPoints().get(0);
        final Labels carLabels = carDataPoint.getLabels();
        assertThat(carLabels.isEmpty(), is(true));
        assertThat(getDataPointValue(carDataPoint), closeTo(0.0, 0.01));

        final MetricSnapshot sensorMetricSnapshot =
                metricsMap.get(QPID_TEST_SENSOR_ALERT_COUNT);
        assertMetricSnapshotDataPointsSize(sensorMetricSnapshot, 1);
        final DataPointSnapshot sensorDataPoint = sensorMetricSnapshot.getDataPoints().get(0);
        final Labels sensorLabels = sensorDataPoint.getLabels();
        assertThat(extractLabelNames(sensorLabels),
                is(equalTo(List.of("name", "test_instrument_panel_name"))));
        assertThat(extractLabelValues(sensorLabels),
                is(equalTo(List.of(SENSOR, INSTRUMENT_PANEL_NAME))));
    }

    /**
     * Verifies that metrics are collected correctly for sibling objects,
     * i.e. a car with two different engine children.
     *
     * <p>The test asserts that:</p>
     * <ul>
     *   <li>the car mileage metric family has a single unlabeled sample, and</li>
     *   <li>the engine temperature metric family has two samples, one for each
     *       engine, labeled with the respective engine name and reporting the
     *       correct temperature value.</li>
     * </ul>
     */
    @Test
    public void testCollectForSiblingObjects()
    {
        createTestEngine(ELECTRIC_ENGINE_NAME, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        createTestEngine(PETROL_ENGINE_NAME, TestPetrolEngineImpl.TEST_PETROL_ENGINE_TYPE);

        final MetricSnapshots metrics = _qpidCollector.collect();

        final String[] expectedFamilyNames =
                {QPID_TEST_CAR_MILEAGE, QPID_TEST_ENGINE_TEMPERATURE};
        final Map<String, MetricSnapshot> metricsMap =
                convertMetricSnapshotsIntoMapAndAssert(metrics, expectedFamilyNames);

        final MetricSnapshot carMetricSnapshot = metricsMap.get(QPID_TEST_CAR_MILEAGE);
        assertMetricSnapshotDataPointsSize(carMetricSnapshot, 1);
        final DataPointSnapshot carDataPoint = carMetricSnapshot.getDataPoints().get(0);
        assertThat(carDataPoint.getLabels().isEmpty(), is(true));

        final MetricSnapshot engineMetricSnapshot = metricsMap.get(QPID_TEST_ENGINE_TEMPERATURE);
        assertMetricSnapshotDataPointsSize(engineMetricSnapshot, 2);
        final String[] engineNames = {PETROL_ENGINE_NAME, ELECTRIC_ENGINE_NAME};
        for (String engineName : engineNames)
        {
            final DataPointSnapshot dataPoint =
                    findDataPointByLabelValue(engineMetricSnapshot, engineName);
            final Labels labels = dataPoint.getLabels();
            assertThat(extractLabelNames(labels), is(equalTo(List.of("name"))));
            assertThat(extractLabelValues(labels), is(equalTo(List.of(engineName))));
            assertThat(getDataPointValue(dataPoint),
                    closeTo(TestAbstractEngineImpl.TEST_TEMPERATURE, 0.01));
        }
    }

    /**
     * Verifies that the collector honors the metric family filter and only
     * exports metric families whose names match the configured predicate.
     *
     * <p>The test enables disabled statistics and restricts the collector to
     * export only the car age metric family.</p>
     */
    @Test
    public void testCollectWithFilter()
    {
        createTestEngine(ELECTRIC_ENGINE_NAME, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);
        _root.move(DESIRED_MILEAGE);

        _qpidCollector = new QpidCollector(_root,
                new IncludeDisabledStatisticPredicate(true),
                new IncludeMetricPredicate(Set.of(QPID_TEST_CAR_AGE)));
        final MetricSnapshots metrics = _qpidCollector.collect();

        final String[] expectedFamilyNames = { QPID_TEST_CAR_AGE };
        final Map<String, MetricSnapshot> metricsMap =
                convertMetricSnapshotsIntoMapAndAssert(metrics, expectedFamilyNames);

        final MetricSnapshot ageMetricSnapshot = metricsMap.get(QPID_TEST_CAR_AGE);
        assertMetricSnapshotDataPointsSize(ageMetricSnapshot, 1);
        final DataPointSnapshot ageDataPoint = ageMetricSnapshot.getDataPoints().get(0);
        assertThat(ageDataPoint.getLabels().isEmpty(), is(true));
        assertThat(getDataPointValue(ageDataPoint), closeTo(0.0, 0.01));
    }

    /**
     * Verifies that cumulative statistics in the test model are exported as
     * {@link CounterSnapshot} metric families.
     *
     * <p>The test uses the car mileage and sensor alert count statistics,
     * both of which are defined as cumulative in the test model, and asserts
     * that their metric families are instances of {@link CounterSnapshot}.</p>
     */
    @Test
    public void testCumulativeMetricsAreExposedAsCounterSnapshots()
    {
        final TestInstrumentPanel<?> instrumentPanel = getTestInstrumentPanel();
        createTestSensor(instrumentPanel);
        _root.move(DESIRED_MILEAGE);

        final MetricSnapshots metrics = _qpidCollector.collect();

        final String[] expectedFamilyNames =
                {QPID_TEST_CAR_MILEAGE, QPID_TEST_SENSOR_ALERT_COUNT};
        final Map<String, MetricSnapshot> metricsMap =
                convertMetricSnapshotsIntoMapAndAssert(metrics, expectedFamilyNames);

        final MetricSnapshot mileageSnapshot = metricsMap.get(QPID_TEST_CAR_MILEAGE);
        final MetricSnapshot alertSnapshot = metricsMap.get(QPID_TEST_SENSOR_ALERT_COUNT);

        assertThat(mileageSnapshot, instanceOf(CounterSnapshot.class));
        assertThat(alertSnapshot, instanceOf(CounterSnapshot.class));
    }

    /**
     * Verifies that point-in-time statistics in the test model are exported as
     * {@link GaugeSnapshot} metric families.
     *
     * <p>The test uses the engine temperature statistic, which is modeled
     * as a point-in-time measurement, and asserts that its metric family is a
     * {@link GaugeSnapshot}. For completeness, it also asserts that the car
     * mileage metric remains a {@link CounterSnapshot}.</p>
     */
    @Test
    public void testPointInTimeMetricsAreExposedAsGaugeSnapshots()
    {
        createTestEngine(ELECTRIC_ENGINE_NAME, TestElecEngineImpl.TEST_ELEC_ENGINE_TYPE);

        final MetricSnapshots metrics = _qpidCollector.collect();

        final String[] expectedFamilyNames =
                {QPID_TEST_CAR_MILEAGE, QPID_TEST_ENGINE_TEMPERATURE};
        final Map<String, MetricSnapshot> metricsMap =
                convertMetricSnapshotsIntoMapAndAssert(metrics, expectedFamilyNames);

        final MetricSnapshot mileageSnapshot = metricsMap.get(QPID_TEST_CAR_MILEAGE);
        final MetricSnapshot temperatureSnapshot = metricsMap.get(QPID_TEST_ENGINE_TEMPERATURE);

        assertThat(mileageSnapshot, instanceOf(CounterSnapshot.class));
        assertThat(temperatureSnapshot, instanceOf(GaugeSnapshot.class));
    }

    /**
     * Finds a single data point in the given metric snapshot by matching a
     * specific label name and value.
     *
     * @param metricSnapshot metric family snapshot to search
     * @param labelValue     expected value of the label
     * @return the single {@link DataPointSnapshot} that matches the given label
     * @throws AssertionError if no or multiple data points match the predicate
     */
    private DataPointSnapshot findDataPointByLabelValue(final MetricSnapshot metricSnapshot,
                                                        final String labelValue)
    {
        final List<DataPointSnapshot> found = metricSnapshot.getDataPoints()
                .stream()
                .filter(dp -> labelValue.equals(dp.getLabels().get("name")))
                .collect(Collectors.toList());
        assertThat(found.size(), is(equalTo(1)));
        return found.get(0);
    }

    /**
     * Creates a {@link TestEngine} child of the root {@link TestCar} with the
     * given name and type.
     *
     * @param engineName name of the test engine
     * @param engineType type identifier of the test engine implementation
     */
    private void createTestEngine(final String engineName, final String engineType)
    {
        final Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, engineName);
        engineAttributes.put(ConfiguredObject.TYPE, engineType);
        _root.createChild(TestEngine.class, engineAttributes);
    }

    /**
     * Creates a {@link TestSensor} child under the given test instrument panel.
     *
     * @param instrumentPanel parent instrument panel for the sensor
     */
    private void createTestSensor(final TestInstrumentPanel<?> instrumentPanel)
    {
        final Map<String, Object> sensorAttributes = new HashMap<>();
        sensorAttributes.put(ConfiguredObject.NAME, SENSOR);
        sensorAttributes.put(ConfiguredObject.TYPE, TestTemperatureSensorImpl.TEST_TEMPERATURE_SENSOR_TYPE);
        instrumentPanel.createChild(TestSensor.class, sensorAttributes);
    }

    /**
     * Creates and returns a {@link TestInstrumentPanel} child for the root
     * test car, using the digital instrument panel implementation.
     *
     * @return newly created test instrument panel
     */
    private TestInstrumentPanel<?> getTestInstrumentPanel()
    {
        final Map<String, Object> instrumentPanelAttributes = new HashMap<>();
        instrumentPanelAttributes.put(ConfiguredObject.NAME, INSTRUMENT_PANEL_NAME);
        instrumentPanelAttributes.put(ConfiguredObject.TYPE,
                TestDigitalInstrumentPanelImpl.TEST_DIGITAL_INSTRUMENT_PANEL_TYPE);
        return _root.createChild(TestInstrumentPanel.class, instrumentPanelAttributes);
    }

    /**
     * Converts the given metric snapshots into a map keyed by metric family
     * name as used in Prometheus exposition format.
     *
     * <p>The method also asserts that no duplicate family names are present.</p>
     *
     * @param metricSnapshots snapshots to convert
     *
     * @return map from metric family name to {@link MetricSnapshot}
     */
    private Map<String, MetricSnapshot> convertMetricSnapshotsIntoMap(final MetricSnapshots metricSnapshots)
    {
        final Map<String, MetricSnapshot> result = new HashMap<>();
        for (MetricSnapshot metricSnapshot : metricSnapshots)
        {
            final String name = metricSnapshot.getMetadata().getPrometheusName();

            if (result.put(name, metricSnapshot) != null)
            {
                fail(String.format("Duplicate family name : %s", name));
            }
        }
        return result;
    }

    /**
     * Performs basic sanity assertions on the given metric family snapshot.
     *
     * <p>The method validates that metadata and data points are non-null.</p>
     *
     * @param metricSnapshot snapshot to validate
     */
    private void assertMetricSnapshot(final MetricSnapshot metricSnapshot)
    {
        assertThat(metricSnapshot, is(notNullValue()));
        assertThat(metricSnapshot.getMetadata(), is(notNullValue()));
        assertThat(metricSnapshot.getDataPoints(), is(notNullValue()));
    }

    /**
     * Asserts that the given metric family snapshot contains the expected
     * number of data points.
     *
     * @param metricSnapshot        metric family snapshot to inspect
     * @param expectedDataPointSize expected number of samples
     */
    private void assertMetricSnapshotDataPointsSize(final MetricSnapshot metricSnapshot,
                                                    final int expectedDataPointSize)
    {
        assertThat(metricSnapshot.getDataPoints().size(), equalTo(expectedDataPointSize));
    }

    /**
     * Converts the given {@link MetricSnapshots} instance into a map keyed by
     * metric family name and asserts that all expected families are present.
     *
     * @param metrics             collected metric snapshots
     * @param expectedFamilyNames array of expected metric family names
     *
     * @return map from metric family name to {@link MetricSnapshot}
     */
    private Map<String, MetricSnapshot> convertMetricSnapshotsIntoMapAndAssert(final MetricSnapshots metrics,
                                                                               final String[] expectedFamilyNames)
    {
        final Map<String, MetricSnapshot> metricsMap = convertMetricSnapshotsIntoMap(metrics);
        assertThat(metricsMap.size(), equalTo(expectedFamilyNames.length));

        for (String expectedFamily : expectedFamilyNames)
        {
            assertMetricSnapshot(metricsMap.get(expectedFamily));
        }
        return metricsMap;
    }

    /**
     * Verifies that {@link QpidCollector#toSnakeCase(String)} converts Java
     * style camelCase identifiers into Prometheus-compatible snake_case names.
     */
    @Test
    public void testToSnakeCase()
    {
        assertThat(QpidCollector.toSnakeCase("carEngineOilChanges"), is(equalTo("car_engine_oil_changes")));
    }

    /**
     * Verifies that {@link QpidCollector#getFamilyName(Class, ConfiguredObjectStatistic)}
     * generates the correct metric family name for cumulative statistics with
     * different units.
     *
     * <p>The test checks that unit-specific suffixes such as {@code _bytes} or
     * {@code _messages} are appended correctly together with the counter suffix.</p>
     */
    @Test
    public void getFamilyNameForCumulativeStatistic()
    {
        for (int i = 0; i < UNITS.length; i++)
        {
            final ConfiguredObjectStatistic<?, ?> statistics = mock(ConfiguredObjectStatistic.class);
            when(statistics.getUnits()).thenReturn(UNITS[i]);
            when(statistics.getStatisticType()).thenReturn(StatisticType.CUMULATIVE);
            when(statistics.getName()).thenReturn("diagnosticData");
            final String familyName = QpidCollector.getFamilyName(TestCar.class, statistics);
            final String expectedName = String.format("qpid_test_car_diagnostic_data%s", UNIT_SUFFIXES[i]);
            assertThat(String.format("unexpected metric name for units %s", UNITS[i]),
                    familyName,
                    is(equalTo(expectedName)));
        }
    }

    /**
     * Verifies that no additional {@code _count} suffix is added when the
     * statistic name for a cumulative metric already contains the word
     * "count", even if the units would otherwise trigger a suffix.
     */
    @Test
    public void getFamilyNameForCumulativeStatisticContainingCountInName()
    {
        final ConfiguredObjectStatistic<?, ?> statistics = mock(ConfiguredObjectStatistic.class);
        when(statistics.getUnits()).thenReturn(StatisticUnit.BYTES);
        when(statistics.getStatisticType()).thenReturn(StatisticType.CUMULATIVE);
        when(statistics.getName()).thenReturn("CountOfDiagnosticData");
        final String familyName = QpidCollector.getFamilyName(TestCar.class, statistics);
        assertThat(familyName, is(equalTo("qpid_test_car_count_of_diagnostic_data_bytes")));
    }

    /**
     * Verifies that {@link QpidCollector#getFamilyName(Class, ConfiguredObjectStatistic)}
     * generates the correct metric family name for point-in-time statistics
     * with different units.
     *
     * <p>The test checks that unit-specific suffixes such as {@code _bytes} or
     * {@code _messages} are appended correctly together with the {@code _total}
     * suffix for non-time based units.</p>
     */
    @Test
    public void getFamilyNameForPointInTimeStatistic()
    {
        for (int i = 0; i < UNITS.length; i++)
        {
            final ConfiguredObjectStatistic<?, ?> statistics = mock(ConfiguredObjectStatistic.class);
            when(statistics.getUnits()).thenReturn(UNITS[i]);
            when(statistics.getStatisticType()).thenReturn(StatisticType.POINT_IN_TIME);
            when(statistics.getName()).thenReturn("diagnosticData");
            final String familyName = QpidCollector.getFamilyName(TestCar.class, statistics);

            final String expectedName =
                    String.format("qpid_test_car_diagnostic_data%s", UNIT_SUFFIXES[i]);
            assertThat(String.format("unexpected metric name for units %s", UNITS[i]),
                    familyName,
                    is(equalTo(expectedName)));
        }
    }

    /**
     * Extracts label names from the given {@link Labels} instance as a list of
     * Prometheus label names.
     *
     * @param labels source labels
     *
     * @return list of label names in deterministic order
     */
    private List<String> extractLabelNames(final Labels labels)
    {
        final List<String> names = new java.util.ArrayList<>();
        for (int i = 0; i < labels.size(); i++)
        {
            names.add(labels.getPrometheusName(i));
        }
        return names;
    }

    /**
     * Extracts label values from the given {@link Labels} instance.
     *
     * @param labels source labels
     *
     * @return list of label values in deterministic order
     */
    private List<String> extractLabelValues(final Labels labels)
    {
        final List<String> values = new java.util.ArrayList<>();
        for (int i = 0; i < labels.size(); i++)
        {
            values.add(labels.getValue(i));
        }
        return values;
    }

    /**
     * Retrieves the numeric value from the given {@link DataPointSnapshot}
     * assuming it belongs to a counter or gauge metric.
     *
     * @param dataPoint data point whose value should be read
     *
     * @return the numeric value of the data point
     *
     * @throws AssertionError if the data point type is not supported
     */
    private double getDataPointValue(final DataPointSnapshot dataPoint)
    {
        if (dataPoint instanceof CounterSnapshot.CounterDataPointSnapshot)
        {
            return ((CounterSnapshot.CounterDataPointSnapshot) dataPoint).getValue();
        }
        else if (dataPoint instanceof GaugeSnapshot.GaugeDataPointSnapshot)
        {
            return ((GaugeSnapshot.GaugeDataPointSnapshot) dataPoint).getValue();
        }
        else
        {
            fail("Unsupported data point type: " + dataPoint.getClass());
            return 0.0; // unreachable
        }
    }
}