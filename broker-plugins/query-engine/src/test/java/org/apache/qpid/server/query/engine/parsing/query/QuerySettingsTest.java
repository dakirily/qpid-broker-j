/*
 *
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
package org.apache.qpid.server.query.engine.parsing.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.query.engine.QueryEngine;
import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.DateFormat;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.DefaultQuerySettings;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.utils.QuerySettingsBuilder;

/**
 * Tests designed to verify the {@link QuerySettings} functionality
 */
public class QuerySettingsTest
{
    private final Broker<?> _broker = TestBroker.createBroker();

    @ParameterizedTest
    @MethodSource("dateFormatQueries")
    public void customizeDateFormat(final DateFormat format, final Class<?> expectedType)
    {
        QueryEvaluator queryEvaluator = new QueryEvaluator(_broker);

        QuerySettings querySettings = new QuerySettingsBuilder().dateTimeFormat(format).build();

        String query = "select current_timestamp() as result";
        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertTrue(expectedType.isInstance(result.get(0).get("result")));
    }

    @Test()
    public void customizeDatetimePattern()
    {
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy/MM/dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter().withZone(ZoneId.systemDefault());

        QueryEvaluator queryEvaluator = new QueryEvaluator(_broker);
        QuerySettings querySettings = new QuerySettingsBuilder().dateTimePattern("yyyy/MM/dd HH:mm:ss").build();

        String query = "select current_timestamp() as result";
        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        formatter.parse((String)result.get(0).get("result"));
    }

    @Test()
    public void customizeMaxQueryDepth()
    {
        QueryEngine queryEngine = new QueryEngine(_broker);
        queryEngine.setMaxQueryDepth(10);
        QueryEvaluator queryEvaluator = queryEngine.createEvaluator();

        String query = "select 1, 2, 3";
        List<Map<String, Object>> result = queryEvaluator.execute(query).getResults();
        assertEquals(3, result.size());

        Stream.of(
                "select 1, 2, 3, 4, 5, 6, 7, 8, 9, 0",
                "select (1 + 2) * (2 - 3)"
        ).forEach(q -> {
            QueryParsingException exception =
                    assertThrows(QueryParsingException.class, () -> queryEvaluator.execute(q));
            assertEquals("Max query depth reached: 10", exception.getMessage());
        });
    }

    @ParameterizedTest
    @MethodSource("maxBigDecimalQueries")
    public void customizeMaxBigDecimalValue(final String queryToValidate, final String expectedMessage)
    {
        QueryEngine queryEngine = new QueryEngine(_broker);
        queryEngine.setMaxBigDecimalValue(BigDecimal.valueOf(100L));
        queryEngine.setMaxQueryDepth(DefaultQuerySettings.MAX_QUERY_DEPTH);
        QueryEvaluator queryEvaluator = queryEngine.createEvaluator();

        String baselineQuery = "select 2 * 2 as result";
        List<Map<String, Object>> result = queryEvaluator.execute(baselineQuery).getResults();
        assertEquals(1, result.size());
        assertEquals(4, result.get(0).get("result"));

        QueryParsingException exception = assertThrows(QueryParsingException.class,
                () -> queryEvaluator.execute(queryToValidate));
        assertEquals(expectedMessage, exception.getMessage());
    }

    private static Stream<Arguments> maxBigDecimalQueries()
    {
        return Stream.of(
                Arguments.of("select 10 * 10 as result", "Reached maximal allowed big decimal value: 100"),
                Arguments.of("select -10 * 10 as result", "Reached maximal allowed big decimal value: -100")
        );
    }

    @ParameterizedTest
    @MethodSource("decimalDigitQueries")
    public void customizeDecimalDigits(final int decimalDigits, final RoundingMode roundingMode, final String query, final double expected)
    {
        QueryEvaluator queryEvaluator = new QueryEvaluator(_broker);

        QuerySettings querySettings = new QuerySettingsBuilder()
            .decimalDigits(decimalDigits)
            .roundingMode(roundingMode)
            .build();

        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("roundingModeQueries")
    public void customizeRoundingMode(final RoundingMode roundingMode, final String query, final double expected)
    {
        QueryEvaluator queryEvaluator = new QueryEvaluator(_broker);
        QuerySettings querySettings = new QuerySettingsBuilder().decimalDigits(2).roundingMode(roundingMode).build();

        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "GMT+1", "GMT+2", "GMT+3", "GMT+4", "GMT+5", "GMT+6", "GMT+7", "GMT+8", "GMT+9", "GMT+10", "GMT+11", "GMT+12"})
    public void customizeZoneIdViaQuerySettings(final String zoneIdString)
    {
        ZoneId zoneId = ZoneId.of(zoneIdString);
        DateTimeFormatter formatter =
                new DateTimeFormatterBuilder().appendPattern(DefaultQuerySettings.DATE_TIME_PATTERN)
                        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
                        .toFormatter().withZone(zoneId);

        QueryEvaluator queryEvaluator = new QueryEvaluator(_broker);
        QuerySettings querySettings = new QuerySettingsBuilder().zoneId(zoneId).build();

        String query = "select current_timestamp() as result";
        List<Map<String, Object>> result = queryEvaluator.execute(query, querySettings).getResults();

        Instant expected = Instant.now();
        Instant actual = LocalDateTime.parse((String) result.get(0).get("result"), formatter)
                .atZone(zoneId)
                .toInstant();

        assertEquals(1, result.size());
        assertTrue(expected.toEpochMilli() - actual.toEpochMilli() < 1000);
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "GMT+1", "GMT+2", "GMT+3", "GMT+4", "GMT+5", "GMT+6", "GMT+7", "GMT+8", "GMT+9", "GMT+10", "GMT+11", "GMT+12"})
    public void customizeZoneIdViaQueryEngine(final String zoneIdString)
    {
        ZoneId zoneId = ZoneId.of(zoneIdString);
        QueryEngine queryEngine = new QueryEngine(_broker);
        queryEngine.setZoneId(zoneId);
        queryEngine.setMaxQueryDepth(DefaultQuerySettings.MAX_QUERY_DEPTH);
        QueryEvaluator queryEvaluator = queryEngine.createEvaluator();

        Instant expected = Instant.now();
        DateTimeFormatter formatter =
                new DateTimeFormatterBuilder().appendPattern(DefaultQuerySettings.DATE_TIME_PATTERN)
                        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
                        .toFormatter()
                        .withResolverStyle(ResolverStyle.STRICT);

        String query = "select current_timestamp() as result";
        List<Map<String, Object>> result = queryEvaluator.execute(query).getResults();
        Instant actual = LocalDateTime.parse((String) result.get(0).get("result"), formatter)
                .atZone(zoneId)
                .toInstant();

        assertEquals(1, result.size());
        assertTrue(expected.toEpochMilli() - actual.toEpochMilli() < 1000);
    }

    @Test()
    public void customizeBroker()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> new QueryEngine(null));
        assertEquals("Broker instance not provided for querying", exception.getMessage());

        exception = assertThrows(NullPointerException.class, () -> new QueryEvaluator(null));
        assertEquals("Broker instance not provided for querying", exception.getMessage());
    }

    @Test()
    public void customizeMaxQueryCacheSize()
    {
        QueryEngine queryEngine = new QueryEngine(_broker);
        queryEngine.setMaxQueryCacheSize(10);
        queryEngine.setMaxQueryDepth(DefaultQuerySettings.MAX_QUERY_DEPTH);
        queryEngine.initQueryCache();
        QueryEvaluator queryEvaluator = queryEngine.createEvaluator();

        QuerySettings querySettings = new QuerySettings();

        String query = "select current_timestamp() as result";
        queryEvaluator.execute(query, querySettings);
        assertEquals(1, queryEngine.getCacheSize());

        query = "select current_timestamp() as result";
        queryEvaluator.execute(query, querySettings);
        assertEquals(1, queryEngine.getCacheSize());

        for (int i = 0; i < 100; i++)
        {
            query = "select current_timestamp() as result" + i;
            queryEvaluator.execute(query, querySettings);
        }
        assertEquals(10, queryEngine.getCacheSize());
    }

    private static Stream<Arguments> dateFormatQueries()
    {
        return Stream.of(
                Arguments.of(DateFormat.LONG, Long.class),
                Arguments.of(DateFormat.STRING, String.class)
        );
    }

    private static Stream<Arguments> decimalDigitQueries()
    {
        return Stream.of(
                Arguments.of(2, RoundingMode.DOWN, "select 1.999 as result", 1.99),
                Arguments.of(2, RoundingMode.DOWN, "select 1/3 as result", 0.33),
                Arguments.of(4, RoundingMode.DOWN, "select 1.9999 as result", 1.9999),
                Arguments.of(4, RoundingMode.DOWN, "select 1/3 as result", 0.3333)
        );
    }

    private static Stream<Arguments> roundingModeQueries()
    {
        return Stream.of(
                Arguments.of(RoundingMode.HALF_UP, "select 1.977 as result", 1.98),
                Arguments.of(RoundingMode.HALF_UP, "select 1/3 as result", 0.33),
                Arguments.of(RoundingMode.HALF_UP, "select 2/3 as result", 0.67),
                Arguments.of(RoundingMode.DOWN, "select 1.977 as result", 1.97),
                Arguments.of(RoundingMode.DOWN, "select 1/3 as result", 0.33),
                Arguments.of(RoundingMode.DOWN, "select 2/3 as result", 0.66)
        );
    }
}
