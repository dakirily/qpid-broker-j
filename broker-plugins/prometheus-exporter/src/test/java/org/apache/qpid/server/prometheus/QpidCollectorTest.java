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
 * Unit tests for {@link QpidCollector} covering metric collection from a simple
 * {@link org.apache.qpid.server.model.ConfiguredObject} hierarchy and verification of
 * naming, filtering, and label-mapping rules.
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
    private static final String QPID_TEST_SENSOR_ALERT_COUNT = "qpid_test_sensor_alert";
    private static final String QPID_TEST_SENSOR_ALERT_COUNT_RAW = "qpid_test_sensor_alert_count";
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
     * Verifies collection for a two-level object hierarchy and checks that the expected metric families and labels are produced.
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
     * Verifies collection for a three-level object hierarchy and checks that label ordering matches the configured object ancestry.
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

        // label order should correspond to the value order
        assertThat(extractLabelNames(sensorLabels),
                is(equalTo(List.of("name", "test_instrument_panel_name"))));
        assertThat(extractLabelValues(sensorLabels),
                is(equalTo(List.of(SENSOR, INSTRUMENT_PANEL_NAME))));
    }

    /**
     * Verifies collection for sibling objects of the same category and checks that samples are emitted with the correct distinguishing labels.
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
     * Verifies that metric name filtering prevents excluded metrics from being emitted.
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
     * Verifies that counter snapshots use a base name without the Prometheus '_total' suffix in metadata (the suffix is added only in exposition).
     */
    @Test
    public void testCountersDoNotEndWithTotalSuffixInMetadataName()
    {
        final TestInstrumentPanel<?> instrumentPanel = getTestInstrumentPanel();
        createTestSensor(instrumentPanel);
        _root.move(DESIRED_MILEAGE);

        final MetricSnapshots metrics = _qpidCollector.collect();
        final Map<String, MetricSnapshot> map = convertMetricSnapshotsIntoMap(metrics);

        final MetricSnapshot mileage = map.get(QPID_TEST_CAR_MILEAGE);
        final MetricSnapshot alerts = map.get(QPID_TEST_SENSOR_ALERT_COUNT);

        assertThat(mileage, instanceOf(CounterSnapshot.class));
        assertThat(alerts, instanceOf(CounterSnapshot.class));

        assertThat(mileage.getMetadata().getPrometheusName().endsWith("_total"), is(false));
        assertThat(alerts.getMetadata().getPrometheusName().endsWith("_total"), is(false));
    }

    /**
     * Verifies that the name filter accepts both the base counter name and its '_total' exposed form for convenience.
     */
    @Test
    public void testCounterMetricFilterAcceptsTotalSuffix()
    {
        _root.move(DESIRED_MILEAGE);

        _qpidCollector = new QpidCollector(_root,
                new IncludeDisabledStatisticPredicate(true),
                new IncludeMetricPredicate(Set.of(QPID_TEST_CAR_MILEAGE + "_total")));

        final MetricSnapshots metrics = _qpidCollector.collect();
        final Map<String, MetricSnapshot> map = convertMetricSnapshotsIntoMap(metrics);

        // despite to filter with _total, metric should be present under the base name
        assertMetricSnapshot(map.get(QPID_TEST_CAR_MILEAGE));
    }

    /**
     * Verifies that cumulative statistics are mapped to {@link io.prometheus.metrics.model.snapshots.CounterSnapshot}.
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
     * Verifies that point-in-time statistics are mapped to {@link io.prometheus.metrics.model.snapshots.GaugeSnapshot}.
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
     * Verifies snake_case conversion for typical CamelCase identifiers.
     */
    @Test
    public void testToSnakeCase()
    {
        assertThat(QpidCollector.toSnakeCase("carEngineOilChanges"), is(equalTo("car_engine_oil_changes")));
    }

    /**
     * Verifies snake_case conversion preserves acronyms as a single token (e.g. AMQPConnection -> amqp_connection).
     */
    @Test
    public void testToSnakeCaseWithAcronyms()
    {
        assertThat(QpidCollector.toSnakeCase("AMQPConnection"), is(equalTo("amqp_connection")));
    }

    /**
     * Verifies that {@code qpid.metrics.prometheus.preserveMetricNameSuffix} matches a fully qualified family name
     * regardless of case and separators, preserving the raw suffix (e.g. {@code _count}) in the emitted metric name.
     */
    @Test
    public void testPreserveMetricNameSuffixMatchesFullyQualifiedFamilyNameRegardlessOfSeparatorsAndCase()
    {
        final Model model = TestModel.getInstance();

        final Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, CAR_NAME);
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        carAttributes.put(ConfiguredObject.CONTEXT,
                Map.of("qpid.metrics.prometheus.preserveMetricNameSuffix", "QPID-TEST-SENSOR-ALERT-COUNT"));

        final TestCar<?> root = model.getObjectFactory().create(TestCar.class, carAttributes, null);
        final QpidCollector qpidCollector =
                new QpidCollector(root, new IncludeDisabledStatisticPredicate(false), s -> true);

        final Map<String, Object> instrumentPanelAttributes = new HashMap<>();
        instrumentPanelAttributes.put(ConfiguredObject.NAME, INSTRUMENT_PANEL_NAME);
        instrumentPanelAttributes.put(ConfiguredObject.TYPE,
                TestDigitalInstrumentPanelImpl.TEST_DIGITAL_INSTRUMENT_PANEL_TYPE);
        final TestInstrumentPanel<?> instrumentPanel =
                root.createChild(TestInstrumentPanel.class, instrumentPanelAttributes);

        createTestSensor(instrumentPanel);

        final MetricSnapshots metrics = qpidCollector.collect();

        final String[] expectedFamilyNames =
                {QPID_TEST_CAR_MILEAGE, QPID_TEST_SENSOR_ALERT_COUNT_RAW};
        final Map<String, MetricSnapshot> metricsMap =
                convertMetricSnapshotsIntoMapAndAssert(metrics, expectedFamilyNames);

        assertThat("Stripped family name should not be present when suffix preservation is configured",
                metricsMap.containsKey(QPID_TEST_SENSOR_ALERT_COUNT), is(false));
    }

    /**
     * Verifies that {@code qpid.metrics.prometheus.stripMetricNameSuffix=false} enables legacy naming mode, keeping
     * suffixes such as {@code _count} in the emitted metric family names.
     */
    @Test
    public void testStripMetricNameSuffixFalseKeepsRawCounterSuffixes()
    {
        final Model model = TestModel.getInstance();

        final Map<String, Object> carAttributes = new HashMap<>();
        carAttributes.put(ConfiguredObject.NAME, CAR_NAME);
        carAttributes.put(ConfiguredObject.TYPE, TestKitCarImpl.TEST_KITCAR_TYPE);
        carAttributes.put(ConfiguredObject.CONTEXT,
                Map.of("qpid.metrics.prometheus.stripMetricNameSuffix", "false"));

        final TestCar<?> root = model.getObjectFactory().create(TestCar.class, carAttributes, null);
        final QpidCollector qpidCollector =
                new QpidCollector(root, new IncludeDisabledStatisticPredicate(false), s -> true);

        final Map<String, Object> instrumentPanelAttributes = new HashMap<>();
        instrumentPanelAttributes.put(ConfiguredObject.NAME, INSTRUMENT_PANEL_NAME);
        instrumentPanelAttributes.put(ConfiguredObject.TYPE,
                TestDigitalInstrumentPanelImpl.TEST_DIGITAL_INSTRUMENT_PANEL_TYPE);
        final TestInstrumentPanel<?> instrumentPanel =
                root.createChild(TestInstrumentPanel.class, instrumentPanelAttributes);

        createTestSensor(instrumentPanel);

        final MetricSnapshots metrics = qpidCollector.collect();

        final String[] expectedFamilyNames =
                {QPID_TEST_CAR_MILEAGE, QPID_TEST_SENSOR_ALERT_COUNT_RAW};
        final Map<String, MetricSnapshot> metricsMap =
                convertMetricSnapshotsIntoMapAndAssert(metrics, expectedFamilyNames);

        assertThat("Default stripped family name should not be present in legacy naming mode",
                metricsMap.containsKey(QPID_TEST_SENSOR_ALERT_COUNT), is(false));
    }

    /**
     * Verifies family name generation for cumulative statistics across supported units.
     */
    @Test
    public void getFamilyNameForCumulativeStatistic() throws Exception
    {
        for (int i = 0; i < UNITS.length; i++)
        {
            final ConfiguredObjectStatistic<?, ?> statistics = mock(ConfiguredObjectStatistic.class);
            when(statistics.getUnits()).thenReturn(UNITS[i]);
            when(statistics.getStatisticType()).thenReturn(StatisticType.CUMULATIVE);
            when(statistics.getName()).thenReturn("diagnosticData");
            when(statistics.getMetricName()).thenReturn(null);

            final String familyName = _qpidCollector.getFamilyName(TestCar.class, statistics, true);

            final String expectedName = String.format("qpid_test_car_diagnostic_data%s", UNIT_SUFFIXES[i]);
            assertThat(String.format("unexpected metric name for units %s", UNITS[i]),
                    familyName,
                    is(equalTo(expectedName)));
        }
    }

    /**
     * Verifies that the token 'Count' appearing in a statistic name does not incorrectly trigger counter suffix stripping.
     */
    @Test
    public void getFamilyNameForCumulativeStatisticContainingCountInName() throws Exception
    {
        final ConfiguredObjectStatistic<?, ?> statistics = mock(ConfiguredObjectStatistic.class);
        when(statistics.getUnits()).thenReturn(StatisticUnit.BYTES);
        when(statistics.getStatisticType()).thenReturn(StatisticType.CUMULATIVE);
        when(statistics.getName()).thenReturn("CountOfDiagnosticData");
        when(statistics.getMetricName()).thenReturn(null);

        final String familyName = _qpidCollector.getFamilyName(TestCar.class, statistics, true);
        assertThat(familyName, is(equalTo("qpid_test_car_count_of_diagnostic_data_bytes")));
    }

    /**
     * Verifies family name generation for point-in-time statistics across supported units.
     */
    @Test
    public void getFamilyNameForPointInTimeStatistic() throws Exception
    {
        for (int i = 0; i < UNITS.length; i++)
        {
            final ConfiguredObjectStatistic<?, ?> statistics = mock(ConfiguredObjectStatistic.class);
            when(statistics.getUnits()).thenReturn(UNITS[i]);
            when(statistics.getStatisticType()).thenReturn(StatisticType.POINT_IN_TIME);
            when(statistics.getName()).thenReturn("diagnosticData");
            when(statistics.getMetricName()).thenReturn(null);

            final String familyName = _qpidCollector.getFamilyName(TestCar.class, statistics, false);

            final String expectedName =
                    String.format("qpid_test_car_diagnostic_data%s", UNIT_SUFFIXES[i]);
            assertThat(String.format("unexpected metric name for units %s", UNITS[i]),
                    familyName,
                    is(equalTo(expectedName)));
        }
    }

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

    private void createTestEngine(final String engineName, final String engineType)
    {
        final Map<String, Object> engineAttributes = new HashMap<>();
        engineAttributes.put(ConfiguredObject.NAME, engineName);
        engineAttributes.put(ConfiguredObject.TYPE, engineType);
        _root.createChild(TestEngine.class, engineAttributes);
    }

    private void createTestSensor(final TestInstrumentPanel<?> instrumentPanel)
    {
        final Map<String, Object> sensorAttributes = new HashMap<>();
        sensorAttributes.put(ConfiguredObject.NAME, SENSOR);
        sensorAttributes.put(ConfiguredObject.TYPE, TestTemperatureSensorImpl.TEST_TEMPERATURE_SENSOR_TYPE);
        instrumentPanel.createChild(TestSensor.class, sensorAttributes);
    }

    private TestInstrumentPanel<?> getTestInstrumentPanel()
    {
        final Map<String, Object> instrumentPanelAttributes = new HashMap<>();
        instrumentPanelAttributes.put(ConfiguredObject.NAME, INSTRUMENT_PANEL_NAME);
        instrumentPanelAttributes.put(ConfiguredObject.TYPE,
                TestDigitalInstrumentPanelImpl.TEST_DIGITAL_INSTRUMENT_PANEL_TYPE);
        return _root.createChild(TestInstrumentPanel.class, instrumentPanelAttributes);
    }

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

    private void assertMetricSnapshot(final MetricSnapshot metricSnapshot)
    {
        assertThat(metricSnapshot, is(notNullValue()));
        assertThat(metricSnapshot.getMetadata(), is(notNullValue()));
        assertThat(metricSnapshot.getDataPoints(), is(notNullValue()));
    }

    private void assertMetricSnapshotDataPointsSize(final MetricSnapshot metricSnapshot,
                                                    final int expectedDataPointSize)
    {
        assertThat(metricSnapshot.getDataPoints().size(), equalTo(expectedDataPointSize));
    }

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

    private List<String> extractLabelNames(final Labels labels)
    {
        final List<String> names = new java.util.ArrayList<>();
        for (int i = 0; i < labels.size(); i++)
        {
            names.add(labels.getPrometheusName(i));
        }
        return names;
    }

    private List<String> extractLabelValues(final Labels labels)
    {
        final List<String> values = new java.util.ArrayList<>();
        for (int i = 0; i < labels.size(); i++)
        {
            values.add(labels.getValue(i));
        }
        return values;
    }

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
