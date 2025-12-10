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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidCollector.class);

    static final String STRICT_NAMING_CONTEXT_VARIABLE = "qpid.metrics.strictNaming";
    static final String PRESERVE_METRIC_NAME_SUFFIX_CONTEXT_VARIABLE = "qpid.metrics.prometheus.preserveMetricNameSuffix";
    static final String STRIP_METRIC_NAME_SUFFIX_CONTEXT_VARIABLE = "qpid.metrics.prometheus.stripMetricNameSuffix";

    private static final String COUNTER_COUNT_SUFFIX = "_count";
    private static final String COUNTER_TOTAL_SUFFIX = "_total";

    // suppress repetitive WARNs
    private static final Set<String> WARNED_SANITIZATIONS = ConcurrentHashMap.newKeySet();
    private static final Set<String> WARNED_INVALID_VALUES = ConcurrentHashMap.newKeySet();
    private static final Set<String> WARNED_LABEL_MISMATCH = ConcurrentHashMap.newKeySet();
    private static final Set<String> WARNED_AUTO_EXTENDED_NAMES = ConcurrentHashMap.newKeySet();

    private final Predicate<ConfiguredObjectStatistic<?, ?>> _includeStatisticFilter;
    private final Predicate<String> _includeMetricFilter;
    private final ConfiguredObject<?> _root;
    private final Model _model;
    private final boolean _strictNaming;
    private final boolean _stripMetricNameSuffix;
    private final Set<String> _preserveSuffixMetricNameKeys = new HashSet<>();

    // cache: typeClass -> (statisticName -> statisticDefinition)
    private final Map<Class<? extends ConfiguredObject>, Map<String, ConfiguredObjectStatistic<?, ?>>> _statisticsCache =
            new HashMap<>();

    QpidCollector(final ConfiguredObject<?> root,
                  final Predicate<ConfiguredObjectStatistic<?, ?>> includeStatisticFilter,
                  final Predicate<String> includeMetricFilter)
    {
        _root = root;
        _model = _root.getModel();
        _includeStatisticFilter = includeStatisticFilter;
        _includeMetricFilter = includeMetricFilter;

        final Boolean strict = _root.getContextValue(Boolean.class, STRICT_NAMING_CONTEXT_VARIABLE);
        _strictNaming = Boolean.TRUE.equals(strict);

        final Boolean stripSuffix = _root.getContextValue(Boolean.class, STRIP_METRIC_NAME_SUFFIX_CONTEXT_VARIABLE);
        _stripMetricNameSuffix = stripSuffix == null || stripSuffix;
        if (!_stripMetricNameSuffix)
        {
            LOGGER.warn("Prometheus exporter legacy naming enabled: metric name suffix stripping is disabled ({}=false).",
                    STRIP_METRIC_NAME_SUFFIX_CONTEXT_VARIABLE);
        }

        final String preserveSuffixMetricNamesVariable = _root.getContextValue(String.class, PRESERVE_METRIC_NAME_SUFFIX_CONTEXT_VARIABLE);
        if (preserveSuffixMetricNamesVariable != null)
        {
            final String[] preserveSuffixMetricNames = preserveSuffixMetricNamesVariable.split(",");
            for (final String name : preserveSuffixMetricNames)
            {
                final String trimmed = name == null ? "" : name.trim();
                if (!trimmed.isEmpty())
                {
                    _preserveSuffixMetricNameKeys.add(normalizeForPreserveMatch(trimmed));
                }
            }
        }
    }

    @Override
    public MetricSnapshots collect()
    {
        // insertion order needed for deterministic tests/diffs
        final Map<String, MetricFamilyAccumulator> familiesByName = new LinkedHashMap<>();

        // root object has no labels
        collectRecursively(_root, List.of(), familiesByName);

        final List<MetricSnapshot> snapshots = new ArrayList<>(familiesByName.size());
        for (MetricFamilyAccumulator accumulator : familiesByName.values())
        {
            snapshots.add(accumulator.build());
        }
        return new MetricSnapshots(snapshots);
    }

    private void collectRecursively(final ConfiguredObject<?> object,
                                    final List<String> labelNamesForObject,
                                    final Map<String, MetricFamilyAccumulator> familiesByName)
    {
        addObjectMetrics(object, labelNamesForObject, familiesByName);

        final Class<? extends ConfiguredObject> category = object.getCategoryClass();
        for (final Class<? extends ConfiguredObject> childClass : _model.getChildTypes(category))
        {
            final Collection<? extends ConfiguredObject> children = object.getChildren(childClass);
            if (children == null || children.isEmpty())
            {
                continue;
            }

            // child label names depend on the parent object (not on the child class),
            // because we want the label ordering: name (self), parent_name, grandparent_name, ...
            final List<String> childLabelNames = computeChildLabelNames(object, labelNamesForObject);

            for (final ConfiguredObject<?> child : children)
            {
                collectRecursively(child, childLabelNames, familiesByName);
            }
        }
    }

    /**
     * Label naming scheme (for any object except root):
     * - first label is always "name" (object's own name),
     * - next labels are "<parentType>_name", "<grandparentType>_name", ... (closest ancestor first).
     * <br>
     * This fixes the old depth>2 shift where ancestor labels got swapped.
     */
    private List<String> computeChildLabelNames(final ConfiguredObject<?> parent,
                                                final List<String> parentLabelNames)
    {
        // For direct children of root, keep historical behavior: only "name" label.
        if (parent == _root)
        {
            return List.of("name");
        }

        final String parentTypeLabel = "%s_name".formatted(toSnakeCase(parent.getCategoryClass().getSimpleName()));

        // parentLabelNames is expected to be ["name", ...ancestor labels]
        if (parentLabelNames.isEmpty())
        {
            // defensive fallback (should not normally happen)
            return List.of("name", parentTypeLabel);
        }

        // Insert parent's type label right after the "name" label
        final List<String> result = new ArrayList<>(parentLabelNames.size() + 1);
        result.add(parentLabelNames.get(0));   // "name"
        result.add(parentTypeLabel);          // closest ancestor label
        for (int i = 1; i < parentLabelNames.size(); i++)
        {
            result.add(parentLabelNames.get(i));
        }
        return List.copyOf(result);
    }

    private void addObjectMetrics(final ConfiguredObject<?> object,
                                  final List<String> labelNamesForObject,
                                  final Map<String, MetricFamilyAccumulator> familiesByName)
    {
        final Map<String, Object> statsMap = object.getStatistics();
        if (statsMap == null || statsMap.isEmpty())
        {
            return;
        }

        final List<String> labelValuesForObject = buildLabelValues(object);

        // Label sanity (avoid throwing from Labels.of with unclear error context)
        if (labelNamesForObject.size() != labelValuesForObject.size())
        {
            final String msg = "Label names/values size mismatch for object category=%s type=%s: names=%s values=%s"
                    .formatted(object.getCategoryClass().getSimpleName(), object.getType(), labelNamesForObject, labelValuesForObject);
            if (_strictNaming)
            {
                throw new IllegalArgumentException(msg);
            }
            warnOnce(WARNED_LABEL_MISMATCH, msg, LOGGER);
            return;
        }

        for (final Map.Entry<String, Object> statEntry : statsMap.entrySet())
        {
            final String statisticName = statEntry.getKey();
            final Object rawValue = statEntry.getValue();

            final ConfiguredObjectStatistic<?, ?> statisticDef = findConfiguredObjectStatistic(statisticName, object.getCategoryClass());
            if (statisticDef == null)
            {
                continue;
            }

            if (!_includeStatisticFilter.test(statisticDef))
            {
                continue;
            }

            final StatisticType type = statisticDef.getStatisticType();
            final boolean isCounter = type == StatisticType.CUMULATIVE;

            final String rawMetricName = getRawMetricName(statisticDef);

            final List<String> strippedFamilyNameCandidates = _stripMetricNameSuffix
                    ? computeAutoExtendFamilyNameCandidates(object, statisticDef, isCounter, true)
                    : List.of();
            final List<String> unstrippedFamilyNameCandidates =
                    computeAutoExtendFamilyNameCandidates(object, statisticDef, isCounter, false);

            // Apply include filter to any candidate so users can filter by either legacy or extended names,
            // and regardless of whether suffix stripping is applied.
            if (!isAnyIncludedByNameFilter(strippedFamilyNameCandidates, isCounter)
                    && !isAnyIncludedByNameFilter(unstrippedFamilyNameCandidates, isCounter))
            {
                continue;
            }

            final Double value = toDoubleValueOrNull(rawValue, statisticDef, object);
            if (value == null)
            {
                continue;
            }

            final String description = statisticDef.getDescription() == null ? "" : statisticDef.getDescription();

            final MetricFamilyAccumulator family = getOrCreateMetricFamilyAccumulatorAutoExtend(
                    strippedFamilyNameCandidates,
                    unstrippedFamilyNameCandidates,
                    rawMetricName,
                    isCounter,
                    description,
                    labelNamesForObject,
                    familiesByName,
                    object,
                    statisticDef);

            if (family == null)
            {
                continue; // warned already
            }

            family.addSample(labelValuesForObject, value);
        }
    }

    private boolean isAnyIncludedByNameFilter(final List<String> familyNameCandidates, final boolean isCounter)
    {
        if (familyNameCandidates == null || familyNameCandidates.isEmpty())
        {
            return false;
        }
        for (final String familyName : familyNameCandidates)
        {
            if (isIncludedByNameFilter(familyName, isCounter))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * AUTO_EXTEND naming strategy:
     * - Try leaf-only family name first: qpid_<leafCategory>_<metric>
     * - If there's a label/type clash, prepend parent categories: qpid_<parent>_<leaf>_<metric>
     * - Continue prepending upwards until a compatible family is found (or we run out of candidates).
     */
    List<String> computeAutoExtendFamilyNameCandidates(final ConfiguredObject<?> object,
                                                       final ConfiguredObjectStatistic<?, ?> statisticDef,
                                                       final boolean isCounter,
                                                       final boolean stripSuffix)
    {
        final List<Class<? extends ConfiguredObject>> categoryPath = buildCategoryPath(object);
        final List<String> candidates = new ArrayList<>(categoryPath.size());

        // Try the historical leaf-only name first, then prepend parent categories as needed.
        for (int start = categoryPath.size() - 1; start >= 0; start--)
        {
            final List<Class<? extends ConfiguredObject>> suffix = categoryPath.subList(start, categoryPath.size());
            candidates.add(buildFamilyNameForCategoryPathSuffix(suffix, statisticDef, isCounter, stripSuffix));
        }

        return List.copyOf(candidates);
    }

    private List<Class<? extends ConfiguredObject>> buildCategoryPath(final ConfiguredObject<?> object)
    {
        final List<Class<? extends ConfiguredObject>> path = new ArrayList<>();
        ConfiguredObject<?> o = object;
        while (o != null)
        {
            path.add(o.getCategoryClass());
            if (o == _root)
            {
                break;
            }
            o = o.getParent();
        }

        Collections.reverse(path);
        return List.copyOf(path);
    }

    private String buildFamilyNameForCategoryPathSuffix(final List<Class<? extends ConfiguredObject>> categorySuffix,
                                                        final ConfiguredObjectStatistic<?, ?> statisticDef,
                                                        final boolean isCounter,
                                                        final boolean stripSuffix)
    {
        final Class<? extends ConfiguredObject> leafCategory = categorySuffix.get(categorySuffix.size() - 1);
        final String metricName = getSanitizedMetricName(leafCategory, statisticDef, isCounter, stripSuffix);

        final StringBuilder categoryPart = new StringBuilder();
        for (int i = 0; i < categorySuffix.size(); i++)
        {
            if (i > 0)
            {
                categoryPart.append('_');
            }
            categoryPart.append(toSnakeCase(categorySuffix.get(i).getSimpleName()));
        }

        final String familyName = "qpid_%s_%s".formatted(categoryPart, metricName);

        // ensure full family name isn't silently modified either
        return sanitizeWithPolicy(familyName, buildContext(leafCategory, statisticDef, "family"));
    }

    private String getSanitizedMetricName(final Class<? extends ConfiguredObject> categoryClass,
                                          final ConfiguredObjectStatistic<?, ?> statistics,
                                          final boolean isCounter,
                                          final boolean stripSuffix)
    {
        String metricName = getRawMetricName(statistics);

        if (stripSuffix)
        {
            if (isCounter)
            {
                metricName = stripCounterTotalSuffix(metricName);
            }
            else
            {
                metricName = stripNonCounterTotalSuffix(metricName);
            }
        }

        // prevent silent rename from sanitizeMetricName()
        return sanitizeWithPolicy(metricName, buildContext(categoryClass, statistics, "metric"));
    }

    private String getRawMetricName(final ConfiguredObjectStatistic<?, ?> statistics)
    {
        String metricName = statistics.getMetricName();
        if (metricName == null || metricName.isEmpty())
        {
            metricName = generateMetricName(statistics);
        }
        return metricName == null ? "" : metricName.trim();
    }

    private static final class ResolvedFamilyName
    {
        final String resolvedName;
        final int resolvedIndex;

        ResolvedFamilyName(final String resolvedName, final int resolvedIndex)
        {
            this.resolvedName = resolvedName;
            this.resolvedIndex = resolvedIndex;
        }
    }

    private ResolvedFamilyName resolveFamilyNameCandidate(final List<String> familyNameCandidates,
                                                          final boolean isCounter,
                                                          final List<String> labelNamesForObject,
                                                          final Map<String, MetricFamilyAccumulator> familiesByName)
    {
        if (familyNameCandidates == null || familyNameCandidates.isEmpty())
        {
            return null;
        }

        for (int i = 0; i < familyNameCandidates.size(); i++)
        {
            final String candidateName = familyNameCandidates.get(i);
            final MetricFamilyAccumulator existing = familiesByName.get(candidateName);
            if (existing == null)
            {
                return new ResolvedFamilyName(candidateName, i);
            }

            if (isFamilyCompatible(existing, isCounter, labelNamesForObject))
            {
                return new ResolvedFamilyName(candidateName, i);
            }
        }

        return null;
    }

    private boolean isFamilyCompatible(final MetricFamilyAccumulator existing,
                                       final boolean isCounter,
                                       final List<String> labelNamesForObject)
    {
        final boolean existingIsCounter = existing instanceof CounterMetricFamilyAccumulator;
        if (existingIsCounter != isCounter)
        {
            return false;
        }
        return existing.getLabelNames().equals(labelNamesForObject);
    }

    MetricFamilyAccumulator getOrCreateMetricFamilyAccumulatorAutoExtend(
            final List<String> strippedFamilyNameCandidates,
            final List<String> unstrippedFamilyNameCandidates,
            final String rawMetricName,
            final boolean isCounter,
            final String description,
            final List<String> labelNamesForObject,
            final Map<String, MetricFamilyAccumulator> familiesByName,
            final ConfiguredObject<?> object,
            final ConfiguredObjectStatistic<?, ?> statisticDef)
    {
        final ResolvedFamilyName unstrippedResolved =
                resolveFamilyNameCandidate(unstrippedFamilyNameCandidates, isCounter, labelNamesForObject, familiesByName);
        final ResolvedFamilyName strippedResolved =
                resolveFamilyNameCandidate(strippedFamilyNameCandidates, isCounter, labelNamesForObject, familiesByName);

        // Decide whether suffix stripping is applied based on:
        // - raw metric name from the model (as in README)
        // - fully qualified resolved family name after AUTO_EXTEND (as in systest)
        final boolean preserveSuffixRequested = !_stripMetricNameSuffix || shouldPreserveMetricNameSuffix(
                rawMetricName,
                unstrippedResolved == null ? null : unstrippedResolved.resolvedName);

        final ResolvedFamilyName chosen;
        if (preserveSuffixRequested)
        {
            chosen = unstrippedResolved != null ? unstrippedResolved : strippedResolved;
        }
        else
        {
            chosen = strippedResolved != null ? strippedResolved : unstrippedResolved;
        }

        if (chosen == null)
        {
            final String msg = buildAutoExtendFailureMessage(
                    strippedFamilyNameCandidates,
                    unstrippedFamilyNameCandidates,
                    preserveSuffixRequested,
                    isCounter,
                    labelNamesForObject,
                    familiesByName,
                    object,
                    statisticDef,
                    rawMetricName);

            if (_strictNaming)
            {
                throw new IllegalArgumentException(msg);
            }

            warnOnce(WARNED_LABEL_MISMATCH, msg, LOGGER);
            return null;
        }

        if (chosen.resolvedIndex > 0)
        {
            final String warnKey = "%s|%s|%s".formatted(chosen.resolvedName, statisticDef.getName(), object.getCategoryClass().getSimpleName());
            if (WARNED_AUTO_EXTENDED_NAMES.add(warnKey))
            {
                LOGGER.debug("AUTO_EXTEND resolved metric family name to '{}'", chosen.resolvedName);
            }
        }

        MetricFamilyAccumulator family = familiesByName.get(chosen.resolvedName);
        if (family == null)
        {
            family = isCounter ? new CounterMetricFamilyAccumulator(chosen.resolvedName, description, labelNamesForObject)
                    : new GaugeMetricFamilyAccumulator(chosen.resolvedName, description, labelNamesForObject);
            familiesByName.put(chosen.resolvedName, family);
        }

        // Defensive: resolution should guarantee compatibility.
        if (!isFamilyCompatible(family, isCounter, labelNamesForObject))
        {
            final String msg = "Metric family '%s' is incompatible with labels/type after resolution: existingLabels=%s newLabels=%s"
                    .formatted(chosen.resolvedName, family.getLabelNames(), labelNamesForObject);

            if (_strictNaming)
            {
                throw new IllegalArgumentException(msg);
            }

            warnOnce(WARNED_LABEL_MISMATCH, msg, LOGGER);
            return null;
        }

        return family;
    }

    private String buildAutoExtendFailureMessage(final List<String> strippedFamilyNameCandidates,
                                                 final List<String> unstrippedFamilyNameCandidates,
                                                 final boolean preserveSuffixRequested,
                                                 final boolean isCounter,
                                                 final List<String> labelNamesForObject,
                                                 final Map<String, MetricFamilyAccumulator> familiesByName,
                                                 final ConfiguredObject<?> object,
                                                 final ConfiguredObjectStatistic<?, ?> statisticDef,
                                                 final String rawMetricName)
    {
        return "Unable to resolve metric family name (AUTO_EXTEND) for category=%s statistic=%s metricName=%s type=%s. "
                .formatted(object.getCategoryClass().getSimpleName(),
                        statisticDef.getName(),
                        rawMetricName,
                        isCounter ? "counter" : "gauge")
                + " preserveSuffixRequested=" + preserveSuffixRequested
                + " labels=" + labelNamesForObject
                + " unstrippedCandidates=" + unstrippedFamilyNameCandidates
                + " strippedCandidates=" + strippedFamilyNameCandidates;
    }

    private boolean shouldPreserveMetricNameSuffix(final String rawMetricName,
                                                   final String resolvedUnstrippedFamilyName)
    {
        if (_preserveSuffixMetricNameKeys.isEmpty())
        {
            return false;
        }

        if (rawMetricName != null && _preserveSuffixMetricNameKeys.contains(normalizeForPreserveMatch(rawMetricName)))
        {
            return true;
        }

        if (resolvedUnstrippedFamilyName != null
                && _preserveSuffixMetricNameKeys.contains(normalizeForPreserveMatch(resolvedUnstrippedFamilyName)))
        {
            return true;
        }

        return false;
    }

    String normalizeForPreserveMatch(final String name)
    {
        if (name == null)
        {
            return "";
        }

        final String s = name.trim().toLowerCase(Locale.ROOT);
        if (s.isEmpty())
        {
            return "";
        }

        final StringBuilder b = new StringBuilder(s.length());
        boolean prevUnderscore = true; // trim leading separators
        for (int i = 0; i < s.length(); i++)
        {
            final char c = s.charAt(i);
            final boolean isAlphaNum = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
            if (isAlphaNum)
            {
                b.append(c);
                prevUnderscore = false;
            }
            else
            {
                if (!prevUnderscore)
                {
                    b.append('_');
                    prevUnderscore = true;
                }
            }
        }

        final int len = b.length();
        if (len > 0 && b.charAt(len - 1) == '_')
        {
            b.setLength(len - 1);
        }

        return b.toString();
    }

    private boolean isIncludedByNameFilter(final String familyName, final boolean isCounter)
    {
        if (_includeMetricFilter.test(familyName))
        {
            return true;
        }

        if (familyName.endsWith(COUNTER_TOTAL_SUFFIX))
        {
            final String base = familyName.substring(0, familyName.length() - COUNTER_TOTAL_SUFFIX.length());
            if (_includeMetricFilter.test(base))
            {
                return true;
            }
        }
        else
        {
            if (_includeMetricFilter.test(familyName + COUNTER_TOTAL_SUFFIX))
            {
                return true;
            }
        }

        if (isCounter)
        {
            if (familyName.endsWith(COUNTER_COUNT_SUFFIX))
            {
                final String base = familyName.substring(0, familyName.length() - COUNTER_COUNT_SUFFIX.length());
                return _includeMetricFilter.test(base) || _includeMetricFilter.test(base + COUNTER_TOTAL_SUFFIX);
            }
            else
            {
                return _includeMetricFilter.test(familyName + COUNTER_COUNT_SUFFIX);
            }
        }

        return false;
    }

    private ConfiguredObjectStatistic<?, ?> findConfiguredObjectStatistic(final String statisticName,
                                                                          final Class<? extends ConfiguredObject> typeClass)
    {
        final Map<String, ConfiguredObjectStatistic<?, ?>> byName =
                _statisticsCache.computeIfAbsent(typeClass, tc ->
                {
                    final Map<String, ConfiguredObjectStatistic<?, ?>> m = new HashMap<>();
                    final Collection<ConfiguredObjectStatistic<?, ?>> defs =
                            _model.getTypeRegistry().getStatistics(tc);
                    for (ConfiguredObjectStatistic<?, ?> def : defs)
                    {
                        m.put(def.getName(), def);
                    }
                    return m;
                });

        return byName.get(statisticName);
    }

    /**
     * Builds label values in the order:
     *   [objectName, parentName, grandparentName, ...] stopping before root.
     * <br>
     * This order matches our label name scheme where "name" is always first
     * (self), then closest ancestor labels.
     */
    private List<String> buildLabelValues(final ConfiguredObject<?> object)
    {
        final List<String> values = new ArrayList<>();
        ConfiguredObject<?> o = object;
        while (o != null && o != _root)
        {
            values.add(o.getName());
            o = o.getParent();
        }
        return List.copyOf(values);
    }

    /**
     * Better acronym handling than naive char-by-char underscore insertion:
     * - AMQPConnection -> amqp_connection
     * - VirtualHostNode -> virtual_host_node
     */
    static String toSnakeCase(final String simpleName)
    {
        // Split "ABCd" -> "AB_Cd", then "aB" -> "a_B"
        final String s = simpleName
                .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
                .replaceAll("([a-z0-9])([A-Z])", "$1_$2");
        return s.toLowerCase(Locale.ROOT);
    }

    /**
     * Converts raw statistic values to a Double.
     * Returns null to skip a sample (e.g. null input, NaN/Infinity, unsupported type).
     */
    private Double toDoubleValueOrNull(final Object value,
                                       final ConfiguredObjectStatistic<?, ?> statisticDef,
                                       final ConfiguredObject<?> object)
    {
        if (value == null)
        {
            return null;
        }

        final StatisticUnit unit = statisticDef.getUnits();
        Double result;

        if (value instanceof Number number)
        {
            result = number.doubleValue();
        }
        else if (value instanceof Date date)
        {
            // Export in seconds (Prometheus convention) if it looks like time
            if (unit == StatisticUnit.ABSOLUTE_TIME)
            {
                result = date.getTime() / 1000.0d;
            }
            else
            {
                // warn about unexpected unit
                final String msg = "Date statistic value isn't ABSOLUTE_TIME: category=%s statistic=%s type=%s javaType=%s"
                        .formatted(object.getCategoryClass().getSimpleName(),
                                statisticDef.getName(),
                                statisticDef.getStatisticType(),
                                value.getClass().getName());
                warnOnce(WARNED_INVALID_VALUES, msg, LOGGER);
                return null;
            }
        }
        else if (value instanceof Duration duration)
        {
            // Prometheus convention: seconds
            result = duration.toNanos() / 1_000_000_000.0d;
        }
        else if (value instanceof Boolean booleanValue)
        {
            result = booleanValue ? 1.0d : 0.0d;
        }
        else
        {
            // unsupported type -> skip
            final String msg = "Skipping non-numeric statistic value: category=%s statistic=%s type=%s javaType=%s"
                    .formatted(object.getCategoryClass().getSimpleName(),
                            statisticDef.getName(),
                            statisticDef.getStatisticType(),
                            value.getClass().getName());
            warnOnce(WARNED_INVALID_VALUES, msg, LOGGER);
            return null;
        }

        if (!Double.isFinite(result))
        {
            final String msg = "Skipping non-finite statistic value: category=%s statistic=%s value=%s"
                    .formatted(object.getCategoryClass().getSimpleName(),
                            statisticDef.getName(),
                            String.valueOf(result));
            warnOnce(WARNED_INVALID_VALUES, msg, LOGGER);
            return null;
        }

        return result;
    }

    String getFamilyName(final Class<? extends ConfiguredObject> categoryClass,
                         final ConfiguredObjectStatistic<?, ?> statistics,
                         final boolean isCounter)
    {
        final String rawMetricName = getRawMetricName(statistics);

        final String strippedMetricName = getSanitizedMetricName(categoryClass, statistics, isCounter, true);
        final String unstrippedMetricName = getSanitizedMetricName(categoryClass, statistics, isCounter, false);

        final String categoryPart = toSnakeCase(categoryClass.getSimpleName());

        final String strippedFamilyName = "qpid_%s_%s".formatted(categoryPart, strippedMetricName);
        final String unstrippedFamilyName = "qpid_%s_%s".formatted(categoryPart, unstrippedMetricName);

        final String familyName = !_stripMetricNameSuffix || shouldPreserveMetricNameSuffix(rawMetricName, unstrippedFamilyName)
                ? unstrippedFamilyName
                : strippedFamilyName;

        // ensure full family name isn't silently modified either
        return sanitizeWithPolicy(familyName, buildContext(categoryClass, statistics, "family"));
    }

    private static String buildContext(final Class<? extends ConfiguredObject> categoryClass,
                                       final ConfiguredObjectStatistic<?, ?> statistics,
                                       final String kind)

    {
            return "%s name (category=%s, statistic=%s)".formatted( kind, categoryClass.getSimpleName(), statistics.getName());
    }

    private static String stripCounterTotalSuffix(final String name)
    {
        if (name == null)
        {
            return null;
        }
        if (name.endsWith(COUNTER_COUNT_SUFFIX))
        {
            return name.substring(0, name.length() - COUNTER_COUNT_SUFFIX.length());
        }
        if (name.endsWith(COUNTER_TOTAL_SUFFIX))
        {
            return name.substring(0, name.length() - COUNTER_TOTAL_SUFFIX.length());
        }
        return name;
    }

    private static String stripNonCounterTotalSuffix(final String name)
    {
        if (name == null)
        {
            return null;
        }
        if (name.endsWith(COUNTER_TOTAL_SUFFIX))
        {
            return name.substring(0, name.length() - COUNTER_TOTAL_SUFFIX.length());
        }
        return name;
    }

    /**
     * Applies "no silent rename" policy around PrometheusNaming.sanitizeMetricName():
     * - if sanitize changes the name:
     *   - strictNaming=true  -> throw IllegalArgumentException
     *   - strictNaming=false -> WARN once and continue with sanitized name
     */
    private String sanitizeWithPolicy(final String name, final String context)
    {
        if (name == null)
        {
            return null;
        }

        final String sanitized = PrometheusNaming.sanitizeMetricName(name);

        if (!sanitized.equals(name))
        {
            final String msg = "Prometheus " + context + " was sanitized: '" + name + "' -> '" + sanitized + "'";
            if (_strictNaming)
            {
                throw new IllegalArgumentException(msg);
            }
            warnOnce(WARNED_SANITIZATIONS, msg, LOGGER);
        }

        // If sanitize produced something invalid (shouldn't), fail fast regardless of strict mode.
        if (!PrometheusNaming.isValidMetricName(sanitized))
        {
            throw new IllegalArgumentException(
                    "Prometheus " + context + " is invalid even after sanitization: '" + sanitized + "'");
        }

        return sanitized;
    }

    private static void warnOnce(final Set<String> sink, final String msg, final Logger logger)
    {
        if (sink.add(msg))
        {
            logger.warn(msg);
        }
    }

    private static String generateMetricName(final ConfiguredObjectStatistic<?, ?> statistics)
    {
        final String baseName = toSnakeCase(statistics.getName());
        final String suffix = generateMetricSuffix(statistics, baseName);
        return baseName + suffix;
    }

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

        abstract void addSample(List<String> labelValues, double value);

        abstract MetricSnapshot build();
    }

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
