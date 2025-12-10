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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

import io.prometheus.metrics.model.snapshots.PrometheusNaming;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectStatistic;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.StatisticType;
import org.apache.qpid.server.model.StatisticUnit;

/**
 * {@link MultiCollector} implementation that exposes Qpid Broker-J statistics
 * as Prometheus metrics using the client_java 1.x MetricSnapshots API.
 *
 * <p>The collector walks the {@link ConfiguredObject} tree starting at the
 * supplied root object and converts all statistics into counter and gauge
 * metric snapshots.</p>
 */
public class QpidCollector implements MultiCollector
{
    private final Predicate<ConfiguredObjectStatistic<?, ?>> _includeStatisticFilter;
    private final Predicate<String> _includeMetricFilter;
    private final ConfiguredObject<?> _root;
    private final Model _model;

    /**
     * Creates a new collector for the given broker model tree.
     *
     * @param root                   root {@link ConfiguredObject} from which statistics are collected
     * @param includeStatisticFilter predicate that decides whether a statistic definition is exposed
     * @param includeMetricFilter    predicate that decides whether a generated Prometheus metric name is exposed
     */
    QpidCollector(final ConfiguredObject<?> root,
                  final Predicate<ConfiguredObjectStatistic<?, ?>> includeStatisticFilter,
                  final Predicate<String> includeMetricFilter)
    {
        _root = root;
        _model = _root.getModel();
        _includeStatisticFilter = includeStatisticFilter;
        _includeMetricFilter = includeMetricFilter;
    }

    /**
     * Collects all statistics from the broker model and converts them into
     * Prometheus {@link MetricSnapshots} for export.
     *
     * @return immutable snapshot of all metrics at the time of the scrape
     */
    @Override
    public MetricSnapshots collect()
    {
        final List<MetricFamilyAccumulator> metricFamilyAccumulators = new ArrayList<>();

        addObjectMetrics(_root, List.of(), new HashMap<>(), metricFamilyAccumulators);
        addChildrenMetrics(metricFamilyAccumulators, _root, List.of("name"));

        final List<MetricSnapshot> snapshots = new ArrayList<>(metricFamilyAccumulators.size());
        for (MetricFamilyAccumulator accumulator : metricFamilyAccumulators)
        {
            snapshots.add(accumulator.build());
        }
        return new MetricSnapshots(snapshots);
    }

    /**
     * Collects statistics for a single configured object and adds them to the
     * provided metric family accumulators.
     *
     * @param object                   configured object whose statistics are exported
     * @param labelNames               label names describing the object hierarchy for this level
     * @param metricFamilyMap          cache of metric families already created for this hierarchy level
     * @param metricFamilyAccumulators list of accumulators representing all metric families in the tree
     */
    private void addObjectMetrics(final ConfiguredObject<?> object,
                                  final List<String> labelNames,
                                  final Map<String, MetricFamilyAccumulator> metricFamilyMap,
                                  final List<MetricFamilyAccumulator> metricFamilyAccumulators)
    {
        final Map<String, Object> statsMap = object.getStatistics();
        for (final Map.Entry<String, Object> entry : statsMap.entrySet())
        {
            final String statisticName = entry.getKey();
            MetricFamilyAccumulator family = metricFamilyMap.get(statisticName);
            if (family == null)
            {
                family = createMetricFamilyAccumulator(statisticName, object, labelNames);
                if (family != null)
                {
                    metricFamilyMap.put(statisticName, family);
                    metricFamilyAccumulators.add(family);
                }
            }
            if (family != null)
            {
                final List<String> labelsValues = buildLabelValues(object);
                final double doubleValue = toDoubleValue(entry.getValue());
                family.addSample(labelsValues, doubleValue);
            }
        }
    }

    /**
     * Recursively walks the configured object tree and collects metrics from
     * all children of the given object.
     *
     * @param metricFamilyAccumulators list of accumulators representing all metric families in the tree
     * @param object                    current configured object whose children are processed
     * @param childLabelNames           label names that should be used for children of the current object
     */
    private void addChildrenMetrics(final List<MetricFamilyAccumulator> metricFamilyAccumulators,
                                    final ConfiguredObject<?> object,
                                    final List<String> childLabelNames)
    {
        final Class<? extends ConfiguredObject> category = object.getCategoryClass();
        for (final Class<? extends ConfiguredObject> childClass : _model.getChildTypes(category))
        {
            final Collection<? extends ConfiguredObject> children = object.getChildren(childClass);
            if (children != null && !children.isEmpty())
            {
                final Map<String, MetricFamilyAccumulator> childrenMetricFamilyMap = new HashMap<>();
                for (final ConfiguredObject<?> child : children)
                {
                    addObjectMetrics(child, childLabelNames, childrenMetricFamilyMap, metricFamilyAccumulators);
                    final List<String> labelNames = new ArrayList<>(childLabelNames);
                    final String label = String.format("%s_name", toSnakeCase(childClass.getSimpleName()));
                    labelNames.add(label);
                    addChildrenMetrics(metricFamilyAccumulators, child, labelNames);
                }
            }
        }
    }

    /**
     * Creates a new metric family accumulator for the given statistic if it
     * should be exposed as a Prometheus metric.
     *
     * @param statisticName name of the statistic as returned from {@link ConfiguredObject#getStatistics()}
     * @param object        configured object that owns the statistic
     * @param labelNames    label names that will be used for samples of this metric family
     *
     * @return a new {@link MetricFamilyAccumulator} or {@code null} if the statistic is not exported
     */
    private MetricFamilyAccumulator createMetricFamilyAccumulator(final String statisticName,
                                                                  final ConfiguredObject<?> object,
                                                                  final List<String> labelNames)
    {
        final ConfiguredObjectStatistic<?, ?> configuredObjectStatistic =
                findConfiguredObjectStatistic(statisticName, object.getTypeClass());
        if (configuredObjectStatistic == null || !_includeStatisticFilter.test(configuredObjectStatistic))
        {
            return null;
        }

        final StatisticType type = configuredObjectStatistic.getStatisticType();
        final String familyName = getFamilyName(object.getCategoryClass(), configuredObjectStatistic);

        if (!_includeMetricFilter.test(familyName))
        {
            return null;
        }

        final String description = configuredObjectStatistic.getDescription();
        return type == StatisticType.CUMULATIVE
                ? new CounterMetricFamilyAccumulator(familyName, description, labelNames)
                : new GaugeMetricFamilyAccumulator(familyName, description, labelNames);
    }

    /**
     * Looks up the statistic definition for the given statistic name and
     * configured object type in the broker model.
     *
     * @param statisticName name of the statistic to resolve
     * @param typeClass     configured object type that owns the statistic
     *
     * @return the matching {@link ConfiguredObjectStatistic} or {@code null} if none is found
     */
    private ConfiguredObjectStatistic<?, ?> findConfiguredObjectStatistic(final String statisticName,
                                                                          final Class<? extends ConfiguredObject> typeClass)
    {
        final Collection<ConfiguredObjectStatistic<?, ?>> statisticsDefinitions =
                _model.getTypeRegistry().getStatistics(typeClass);
        return statisticsDefinitions.stream()
                .filter(s -> statisticName.equals(s.getName()))
                .findFirst()
                .orElse(null);
    }

    /**
     * Builds the list of label values for the given configured object.
     *
     * <p>The values correspond to the label names generated for the object
     * hierarchy, starting with the object itself and then walking up the
     * parents until the configured root object is reached.</p>
     *
     * @param object configured object for which label values should be created
     *
     * @return list of label values in the same order as the label names
     */
    private List<String> buildLabelValues(final ConfiguredObject<?> object)
    {
        final List<String> labelsValues = new ArrayList<>();
        ConfiguredObject<?> o = object;
        while (o != null && o != _root)
        {
            labelsValues.add(o.getName());
            o = o.getParent();
        }
        return labelsValues;
    }

    /**
     * Converts the given simple Java class name into a Prometheus-compatible
     * snake_case identifier.
     *
     * @param simpleName Java simple class name
     *
     * @return the name converted to snake_case
     */
    static String toSnakeCase(final String simpleName)
    {
        final StringBuilder sb = new StringBuilder();
        final char[] chars = simpleName.toCharArray();
        for (int i = 0; i < chars.length; i++)
        {
            final char ch = chars[i];
            if (Character.isUpperCase(ch))
            {
                if (i > 0)
                {
                    sb.append('_');
                }
                sb.append(Character.toLowerCase(ch));
            }
            else
            {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    /**
     * Converts an arbitrary value returned from {@link ConfiguredObject#getStatistics()} to a double.
     *
     * @param value numeric value to convert
     *
     * @return double representation of the value or {@code 0.0} if the value is not numeric
     */
    private double toDoubleValue(final Object value)
    {
        if (value instanceof Number)
        {
            return ((Number) value).doubleValue();
        }
        return 0;
    }

    /**
     * Computes the Prometheus metric family name for the given statistic.
     *
     * @param categoryClass configured object category the statistic belongs to
     * @param statistics    statistic definition
     *
     * @return metric family name in the form {@code qpid_<category>_<metric>}
     */
    static String getFamilyName(final Class<? extends ConfiguredObject> categoryClass,
                                final ConfiguredObjectStatistic<?, ?> statistics)
    {
        String metricName = statistics.getMetricName();
        if (metricName == null || metricName.isEmpty())
        {
            metricName = generateMetricName(statistics);
        }
        else
        {
            metricName = PrometheusNaming.sanitizeMetricName(metricName);
        }
        return "qpid_%s_%s".formatted(toSnakeCase(categoryClass.getSimpleName()), metricName);
    }

    /**
     * Generates a metric name for the given statistic using the original
     * statistic name plus a suffix derived from the statistic type and units.
     *
     * @param statistics statistic definition for which to generate a metric name
     *
     * @return generated metric name in snake_case
     */
    private static String generateMetricName(final ConfiguredObjectStatistic<?, ?> statistics)
    {
        final String baseName = toSnakeCase(statistics.getName());
        final String suffix = generateMetricSuffix(statistics, baseName);
        return PrometheusNaming.sanitizeMetricName(baseName + suffix);
    }

    /**
     * Generates the metric suffix (e.g. unit and type modifiers) for the given statistic.
     *
     * @param statistics statistic definition
     * @param metricName base metric name (without suffix)
     *
     * @return suffix to append to the base metric name, possibly empty
     */
    private static String generateMetricSuffix(final ConfiguredObjectStatistic<?, ?> statistics,
                                               final String metricName)
    {
        final StatisticUnit statisticUnit = statistics.getUnits();
        return switch (statisticUnit)
        {
            case ABSOLUTE_TIME -> metricName.endsWith("_timestamp_seconds") ? "" : "_timestamp_seconds";
            case COUNT -> "";
            case BYTES -> metricName.endsWith("_bytes") ? "" : "_bytes";
            case MESSAGES -> metricName.endsWith("_messages") ? "" : "_messages";
            case TIME_DURATION -> metricName.endsWith("_seconds") ? "" : "_seconds";
        };
    }

    /**
     * Creates a {@link Labels} instance from the given label names and values.
     *
     * @param labelNames  ordered list of label names
     * @param labelValues ordered list of label values
     *
     * @return {@link Labels} with the provided name/value pairs, or {@link Labels#EMPTY} if there are no labels
     */
    private static Labels createLabels(final List<String> labelNames, final List<String> labelValues)
    {
        if (labelNames == null || labelNames.isEmpty())
        {
            return Labels.EMPTY;
        }
        if (labelNames.size() != labelValues.size())
        {
            throw new IllegalArgumentException("List of label names size doesn't match list of label values size");
        }
        return Labels.of(labelNames, labelValues);
    }

    /**
     * Base class for metric family accumulators that build
     * {@link GaugeSnapshot} or {@link CounterSnapshot} instances from
     * individual samples.
     */
    private abstract static class MetricFamilyAccumulator
    {
        private final String _name;
        private final String _help;
        private final List<String> _labelNames;

        MetricFamilyAccumulator(final String name, final String help, final List<String> labelNames)
        {
            _name = name;
            _help = help;
            _labelNames = List.copyOf(labelNames);
        }

        String getName()
        {
            return _name;
        }

        String getHelp()
        {
            return _help;
        }

        List<String> getLabelNames()
        {
            return _labelNames;
        }

        /**
         * Adds a single sample (data point) to the metric family.
         *
         * @param labelValues label values for this sample
         * @param value       numeric value of the sample
         */
        abstract void addSample(List<String> labelValues, double value);

        /**
         * Builds the immutable metric snapshot for this family.
         *
         * @return immutable {@link MetricSnapshot} representing the family
         */
        abstract MetricSnapshot build();
    }

    /**
     * Accumulates counter samples and builds a {@link CounterSnapshot} snapshot.
     */
    private static final class CounterMetricFamilyAccumulator extends MetricFamilyAccumulator
    {
        private final CounterSnapshot.Builder _builder;

        CounterMetricFamilyAccumulator(final String name, final String help, final List<String> labelNames)
        {
            super(name, help, labelNames);
            _builder = CounterSnapshot.builder().name(name).help(help);
        }

        @Override
        void addSample(final List<String> labelValues, final double value)
        {
            final Labels labels = createLabels(getLabelNames(), labelValues);
            final CounterSnapshot.CounterDataPointSnapshot.Builder dataPointBuilder =
                    CounterSnapshot.CounterDataPointSnapshot.builder();
            dataPointBuilder.labels(labels);
            dataPointBuilder.value(value);
            _builder.dataPoint(dataPointBuilder.build());
        }

        @Override
        MetricSnapshot build()
        {
            return _builder.build();
        }
    }

    /**
     * Accumulates gauge samples and builds a {@link GaugeSnapshot} snapshot.
     */
    private static final class GaugeMetricFamilyAccumulator extends MetricFamilyAccumulator
    {
        private final GaugeSnapshot.Builder _builder;

        GaugeMetricFamilyAccumulator(final String name, final String help, final List<String> labelNames)
        {
            super(name, help, labelNames);
            _builder = GaugeSnapshot.builder().name(name).help(help);
        }

        @Override
        void addSample(final List<String> labelValues, final double value)
        {
            final Labels labels = createLabels(getLabelNames(), labelValues);
            final GaugeSnapshot.GaugeDataPointSnapshot.Builder dataPointBuilder =
                    GaugeSnapshot.GaugeDataPointSnapshot.builder();
            dataPointBuilder.labels(labels);
            dataPointBuilder.value(value);
            _builder.dataPoint(dataPointBuilder.build());
        }

        @Override
        MetricSnapshot build()
        {
            return _builder.build();
        }
    }
}
