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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the {@link GreaterThanExpression} functionality
 */
public class GreaterThanExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @ParameterizedTest
    @MethodSource("integerQueries")
    public void comparingIntegers(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("longQueries")
    public void comparingLongs(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("doubleQueries")
    public void comparingDoubles(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("bigDecimalQueries")
    public void comparingBigDecimals(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @Test()
    public void comparingBooleans()
    {
        String query = "select true > false as result";
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Objects of types 'Boolean' and 'Boolean' can not be compared", exception.getMessage());
    }

    @Test()
    public void comparingStrings()
    {
        String query = "select 'b' > 'a' as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));

        query = "select 'b' > 'c' as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select '1' > '2' as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(false, result.get(0).get("result"));

        query = "select 'test124' > 'test123' as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    @Test()
    public void comparingInvalidTypes()
    {
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () ->
                _queryEvaluator.execute("select statistics > statistics as result from queue"));
        assertEquals("Objects of types 'HashMap' and 'HashMap' can not be compared", exception.getMessage());

        exception = assertThrows(QueryEvaluationException.class, () ->
                _queryEvaluator.execute("select bindings > statistics as result from exchange"));
        assertEquals("Objects of types 'List12' and 'HashMap' can not be compared", exception.getMessage());
    }

    @Test()
    public void comparingNulls()
    {
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () ->
                _queryEvaluator.execute("select 1 > null as result from queue"));
        assertEquals("Objects of types 'Integer' and 'null' can not be compared", exception.getMessage());

        exception = assertThrows(QueryEvaluationException.class, () ->
                _queryEvaluator.execute("select null > 'test' as result from queue"));
        assertEquals("Objects of types 'null' and 'String' can not be compared", exception.getMessage());
    }

    private static Stream<Arguments> integerQueries()
    {
        return Stream.of(
                Arguments.of("select 2 > 1 as result", true),
                Arguments.of("select 1 > 2 as result", false),
                Arguments.of("select 0 > -1 as result", true),
                Arguments.of("select -1 > -2 as result", true)
        );
    }

    private static Stream<Arguments> longQueries()
    {
        return Stream.of(
                Arguments.of("select 2L > 1 as result", true),
                Arguments.of("select 1L > 2 as result", false),
                Arguments.of("select 0 > -1L as result", true),
                Arguments.of("select -1L > -2 as result", true)
        );
    }

    private static Stream<Arguments> doubleQueries()
    {
        return Stream.of(
                Arguments.of("select 2/3 > 1/3 as result", true),
                Arguments.of("select 1/4 > 2/4 as result", false),
                Arguments.of("select 0 > -1/2 as result", true),
                Arguments.of("select -1/3 > -2/3 as result", true)
        );
    }

    private static Stream<Arguments> bigDecimalQueries()
    {
        return Stream.of(
                Arguments.of("select " + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN) + " > " + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE) + " as result", true),
                Arguments.of("select " + BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.TEN) + " > " + BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE) + " as result", false)
        );
    }
}
