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
package org.apache.qpid.server.query.engine.parsing.expression.function.numeric;

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
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the public class {@link TruncExpression} functionality
 */
public class TruncExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @ParameterizedTest
    @MethodSource("validQueries")
    public void truncValues(final String query, final Object expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @Test()
    public void noArguments()
    {
        String query = "select trunc() as result";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Function 'TRUNC' requires at least 1 parameter", exception.getMessage());
    }

    @Test()
    public void nullArgument()
    {
        String query = "select trunc(null) as result";
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Parameter of function 'TRUNC' invalid (parameter type: null)", exception.getMessage());
    }

    @Test()
    public void integerArgument()
    {
        String query = "select trunc(1) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));
    }

    @Test()
    public void longArgument()
    {
        String query = "select trunc(1L) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("result"));
    }

    @Test()
    public void doubleArgument()
    {
        String query = "select trunc(2/3) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(0.66, result.get(0).get("result"));
    }

    @Test()
    public void bigDecimalArgument()
    {
        String query = "select trunc(" + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE) + ", 0) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE), result.get(0).get("result"));
    }

    @Test()
    public void dateArgument()
    {
        String query = "select trunc(lastUpdatedTime) as result from queue where name='QUEUE_0'";
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Parameter of function 'TRUNC' invalid (parameter type: Date)", exception.getMessage());
    }

    @Test()
    public void booleanArgument()
    {
        String query = "select trunc(true) as result";
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Parameter of function 'TRUNC' invalid (parameter type: Boolean)", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("invalidTypeQueries")
    public void invalidArgumentType(final String query, final String expectedMessage)
    {
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals(expectedMessage, exception.getMessage());
    }

    private static Stream<Arguments> validQueries()
    {
        return Stream.of(
                Arguments.of("select trunc(2/3) as result", 0.66),
                Arguments.of("select trunc(2/3, 1) as result", 0.6),
                Arguments.of("select trunc(2/3, 2) as result", 0.66),
                Arguments.of("select trunc(2/3, 3) as result", 0.666),
                Arguments.of("select trunc(2/3, 4) as result", 0.6666),
                Arguments.of("select trunc(1) as result", 1),
                Arguments.of("select trunc(1L) as result", 1),
                Arguments.of("select trunc(" + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE) + ", 0) as result",
                        BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE))
        );
    }

    private static Stream<Arguments> invalidTypeQueries()
    {
        return Stream.of(
                Arguments.of("select trunc(statistics) from queue", "Parameter of function 'TRUNC' invalid (parameter type: HashMap)")
        );
    }
}
