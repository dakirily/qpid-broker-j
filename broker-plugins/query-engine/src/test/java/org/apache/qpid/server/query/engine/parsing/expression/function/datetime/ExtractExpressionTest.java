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
package org.apache.qpid.server.query.engine.parsing.expression.function.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the public class {@link ExtractExpression} functionality
 */
public class ExtractExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    private final QuerySettings _querySettings = new QuerySettings();

    @ParameterizedTest
    @MethodSource("yearQueries")
    public void extractYear(final String query, final int expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("monthQueries")
    public void extractMonth(final String query, final int expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("weekQueries")
    public void extractWeek(final String query, final int expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("dayQueries")
    public void extractDay(final String query, final int expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("hourQueries")
    public void extractHour(final String query, final int expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("minuteQueries")
    public void extractMinute(final String query, final int expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("secondQueries")
    public void extractSecond(final String query, final int expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("millisecondQueries")
    public void extractMillisecond(final String query, final int expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @Test()
    public void extractFromCreatedTime()
    {
        int year = LocalDateTime.now().getYear();
        String query = "select * from queue where extract(year from createdTime) = " + year;
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(70, result.size());
    }

    @Test()
    public void noArguments()
    {
        String query = "select extract() as result";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query, _querySettings));
        assertEquals("Function 'EXTRACT' requires 2 parameters", exception.getMessage());
    }

    @Test()
    public void threeArguments()
    {
        String query = "select extract(year, from, current_timestamp()) as result";
        QueryParsingException exception =
                assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query, _querySettings));
        assertEquals("Encountered \" \",\" \", \"\" at line 1, column 20. Was expecting: \"FROM\" ...", exception.getMessage());
    }

    private static Stream<Arguments> yearQueries()
    {
        return Stream.of(
                Arguments.of("select extract(YEAR from '2000-01-01 00:00:00') as result", 2000),
                Arguments.of("select extract(YEAR from '3000-01-01 00:00:00') as result", 3000),
                Arguments.of("select extract(YEAR from '1000-01-01 00:00:00') as result", 1000)
        );
    }

    private static Stream<Arguments> monthQueries()
    {
        return Stream.of(
                Arguments.of("select extract(month from '2000-01-01 00:00:00') as result", 1),
                Arguments.of("select extract(month from '2000-02-01 00:00:00') as result", 2),
                Arguments.of("select extract(month from '2000-03-01 00:00:00') as result", 3),
                Arguments.of("select extract(month from '2000-04-01 00:00:00') as result", 4),
                Arguments.of("select extract(month from '2000-05-01 00:00:00') as result", 5),
                Arguments.of("select extract(month from '2000-06-01 00:00:00') as result", 6),
                Arguments.of("select extract(month from '2000-07-01 00:00:00') as result", 7),
                Arguments.of("select extract(month from '2000-08-01 00:00:00') as result", 8),
                Arguments.of("select extract(month from '2000-09-01 00:00:00') as result", 9),
                Arguments.of("select extract(month from '2000-10-01 00:00:00') as result", 10),
                Arguments.of("select extract(month from '2000-11-01 00:00:00') as result", 11),
                Arguments.of("select extract(month from '2000-12-01 00:00:00') as result", 12)
        );
    }

    private static Stream<Arguments> weekQueries()
    {
        return Stream.of(
                Arguments.of("select extract(week from '2000-01-01 00:00:00') as result", 1),
                Arguments.of("select extract(week from '2000-01-08 00:00:00') as result", 2),
                Arguments.of("select extract(week from '2000-01-15 00:00:00') as result", 3),
                Arguments.of("select extract(week from '2000-01-22 00:00:00') as result", 4),
                Arguments.of("select extract(week from '2000-01-29 00:00:00') as result", 5),
                Arguments.of("select extract(week from '2000-02-05 00:00:00') as result", 6),
                Arguments.of("select extract(week from '2000-02-12 00:00:00') as result", 7),
                Arguments.of("select extract(week from '2000-02-19 00:00:00') as result", 8),
                Arguments.of("select extract(week from '2000-02-26 00:00:00') as result", 9),
                Arguments.of("select extract(week from '2000-03-04 00:00:00') as result", 10)
        );
    }

    private static Stream<Arguments> dayQueries()
    {
        return Stream.of(
                Arguments.of("select extract(day from '2000-01-01 00:00:00') as result", 1),
                Arguments.of("select extract(day from '2000-02-29 00:00:00') as result", 29),
                Arguments.of("select extract(day from '2000-12-31 00:00:00') as result", 31)
        );
    }

    private static Stream<Arguments> hourQueries()
    {
        return Stream.of(
                Arguments.of("select extract(hour from '2000-01-01 00:00:00') as result", 0),
                Arguments.of("select extract(hour from '2000-01-01 12:00:00') as result", 12),
                Arguments.of("select extract(hour from '2000-01-01 23:00:00') as result", 23)
        );
    }

    private static Stream<Arguments> minuteQueries()
    {
        return Stream.of(
                Arguments.of("select extract(minute from '2000-01-01 00:00:00') as result", 0),
                Arguments.of("select extract(minute from '2000-01-01 12:30:00') as result", 30),
                Arguments.of("select extract(minute from '2000-01-01 23:59:00') as result", 59)
        );
    }

    private static Stream<Arguments> secondQueries()
    {
        return Stream.of(
                Arguments.of("select extract(second from '2000-01-01 00:01:00') as result", 0),
                Arguments.of("select extract(second from '2000-01-01 12:32:30') as result", 30),
                Arguments.of("select extract(second from '2000-01-01 23:57:59') as result", 59)
        );
    }

    private static Stream<Arguments> millisecondQueries()
    {
        return Stream.of(
                Arguments.of("select extract(millisecond from '2000-01-01 00:00:00') as result", 0),
                Arguments.of("select extract(millisecond from '2000-01-01 12:30:30.500') as result", 500),
                Arguments.of("select extract(millisecond from '2000-01-01 23:59:59.999') as result", 999)
        );
    }
}
