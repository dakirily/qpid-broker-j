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
package org.apache.qpid.server.query.engine.parsing.expression.comparison;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the public class {@link InExpression} functionality
 */
public class InExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @ParameterizedTest
    @MethodSource("integerQueries")
    public void inIntegers(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("longQueries")
    public void inLongs(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("doubleQueries")
    public void inDoubles(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("bigDecimalQueries")
    public void inBigDecimals(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("stringQueries")
    public void inStrings(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("mixedQueries")
    public void inMixedTypes(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("inSelectQueries")
    public void inSelect(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("notInSelectQueries")
    public void notInSelect(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("inListQueries")
    public void inList(final String query, final int expectedSize, final List<String> expectedValues)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(expectedSize, result.size());
        if (expectedValues != null)
        {
            for (int i = 0; i < expectedValues.size(); i++)
            {
                assertEquals(expectedValues.get(i), result.get(i).values().iterator().next());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("notInListQueries")
    public void notInList(final String query, final int expectedSize, final List<String> expectedValues)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(expectedSize, result.size());
        if (expectedValues != null)
        {
            for (int i = 0; i < expectedValues.size(); i++)
            {
                assertEquals(expectedValues.get(i), result.get(i).values().iterator().next());
            }
        }
    }

    private static Stream<Arguments> integerQueries()
    {
        return Stream.of(
                Arguments.of("select 1 in (1, 2, 3) as result", true),
                Arguments.of("select 1 in (2, 3, 4) as result", false),
                Arguments.of("select 1 in (1) as result", true)
        );
    }

    private static Stream<Arguments> longQueries()
    {
        return Stream.of(
                Arguments.of("select 1L in (1L, 2L, 3L) as result", true),
                Arguments.of("select 1L in (2L, 3L, 4L) as result", false),
                Arguments.of("select 1L in (1L) as result", true)
        );
    }

    private static Stream<Arguments> doubleQueries()
    {
        return Stream.of(
                Arguments.of("select 1.0 in (1.0, 2.0, 3.0) as result", true),
                Arguments.of("select 1.0 in (2.0, 3.0, 4.0) as result", false),
                Arguments.of("select 1.0 in (1.0) as result", true)
        );
    }

    private static Stream<Arguments> bigDecimalQueries()
    {
        return Stream.of(
                Arguments.of("select " + BigDecimal.ONE + " in (" + BigDecimal.ONE + ", " + BigDecimal.valueOf(2L) + ", " + BigDecimal.valueOf(3L) + ") as result", true),
                Arguments.of("select " + BigDecimal.ONE + " in (" + BigDecimal.valueOf(2L) + ", " + BigDecimal.valueOf(3L) + ", " + BigDecimal.valueOf(4L) + ") as result", false),
                Arguments.of("select " + BigDecimal.ONE + " in (" + BigDecimal.valueOf(1L) + ") as result", true)
        );
    }

    private static Stream<Arguments> stringQueries()
    {
        return Stream.of(
                Arguments.of("select 'test' in ('test123', 'test234', 'test') as result", true),
                Arguments.of("select 'test' not in ('test123', 'test234', 'test') as result", false),
                Arguments.of("select 'test' in ('test123', 'test234', 'test345') as result", false),
                Arguments.of("select 'test' not in ('test123', 'test234', 'test345') as result", true),
                Arguments.of("select 'test' in ('test') as result", true),
                Arguments.of("select 'test' not in ('test') as result", false)
        );
    }

    private static Stream<Arguments> mixedQueries()
    {
        return Stream.of(
                Arguments.of("select 1 in ('1', 'test', 1.0) as result", true),
                Arguments.of("select 1 not in ('1', 'test', 1.0) as result", false),
                Arguments.of("select 'test' in (100L, 5/6, 'test') as result", true),
                Arguments.of("select 'test' not in (100L, 5/6, 'test') as result", false),
                Arguments.of("select 1/3 in ('test', '1/3', 0) as result", false),
                Arguments.of("select 1/3 not in ('test', '1/3', 0) as result", true)
        );
    }

    private static Stream<Arguments> inSelectQueries()
    {
        return Stream.of(
                Arguments.of("select 'FLOW_TO_DISK' in (select distinct overflowPolicy from queue) as result", true),
                Arguments.of("select 'RING' in (select distinct overflowPolicy from queue) as result", true),
                Arguments.of("select 'REJECT' in (select distinct overflowPolicy from queue) as result", true),
                Arguments.of("select 'PRODUCER_FLOW_CONTROL' in (select distinct overflowPolicy from queue) as result", true),
                Arguments.of("select 'NONE' in (select distinct overflowPolicy from queue) as result", true),
                Arguments.of("select 'UNKNOWN' in (select distinct overflowPolicy from queue) as result", false)
        );
    }

    private static Stream<Arguments> notInSelectQueries()
    {
        return Stream.of(
                Arguments.of("select 'FLOW_TO_DISK' not in (select distinct overflowPolicy from queue) as result", false),
                Arguments.of("select 'RING' not in (select distinct overflowPolicy from queue) as result", false),
                Arguments.of("select 'REJECT' not  in (select distinct overflowPolicy from queue) as result", false),
                Arguments.of("select 'PRODUCER_FLOW_CONTROL' not in (select distinct overflowPolicy from queue) as result", false),
                Arguments.of("select 'NONE' not in (select distinct overflowPolicy from queue) as result", false),
                Arguments.of("select 'UNKNOWN' not in (select distinct overflowPolicy from queue) as result", true)
        );
    }

    private static Stream<Arguments> inListQueries()
    {
        return Stream.of(
                Arguments.of("select lower(name) as name from queue where lower(name) in ('queue_10', 'queue_20', 'queue_30')", 3, List.of("queue_10", "queue_20", "queue_30")),
                Arguments.of("select distinct overflowPolicy from queue where overflowPolicy in ('RING', 'REJECT', 'NONE')", 3, List.of("NONE", "REJECT", "RING"))
        );
    }

    private static Stream<Arguments> notInListQueries()
    {
        return Stream.of(
                Arguments.of("select lower(name) as name from queue where lower(name) not in ('queue_10', 'queue_20', 'queue_30')", 67, null),
                Arguments.of("select distinct overflowPolicy from queue where overflowPolicy not in ('RING', 'REJECT', 'NONE')", 2, List.of("FLOW_TO_DISK", "PRODUCER_FLOW_CONTROL"))
        );
    }
}
