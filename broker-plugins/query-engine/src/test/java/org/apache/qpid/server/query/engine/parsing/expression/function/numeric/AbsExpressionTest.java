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
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the public class {@link AbsExpression} functionality
 */
public class AbsExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @ParameterizedTest
    @MethodSource("numericQueries")
    public void numericArguments(final String query, final Object expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @Test()
    public void noArguments()
    {
        String query = "select abs() as result";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Function 'ABS' requires 1 parameter", exception.getMessage());
    }

    @Test()
    public void nullArgument()
    {
        String query = "select abs(null) as result";
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Parameter of function 'ABS' invalid (parameter type: null)", exception.getMessage());
    }

    @Test()
    public void dateArgument()
    {
        String query = "select abs(lastUpdatedTime) as result from queue where name='QUEUE_0'";
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Parameter of function 'ABS' invalid (parameter type: Date)", exception.getMessage());
    }

    @Test()
    public void booleanArgument()
    {
        String query = "select abs(true) as result";
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Parameter of function 'ABS' invalid (parameter type: Boolean)", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("invalidTypeQueries")
    public void invalidArgumentType(final String query, final String expectedMessage)
    {
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals(expectedMessage, exception.getMessage());
    }

    private static Stream<Arguments> numericQueries()
    {
        return Stream.of(
                Arguments.of("select abs(1) as result", 1),
                Arguments.of("select abs(-1) as result", 1),
                Arguments.of("select abs(" + Integer.MIN_VALUE + ") as result", (long) Integer.MAX_VALUE + 1),
                Arguments.of("select abs(1L) as result", 1),
                Arguments.of("select abs(-1L) as result", 1),
                Arguments.of("select abs(" + Long.MIN_VALUE + ") as result", BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE)),
                Arguments.of("select abs(1.0) as result", 1),
                Arguments.of("select abs(-1.0) as result", 1),
                Arguments.of("select abs(" + -Double.MAX_VALUE + ") as result", new BigDecimal(BigDecimal.valueOf(-Double.MAX_VALUE).abs().toPlainString())),
                Arguments.of("select abs(" + BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.TEN) + ") as result",
                        BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN).add(BigDecimal.ONE))
        );
    }

    private static Stream<Arguments> invalidTypeQueries()
    {
        return Stream.of(
                Arguments.of("select abs(statistics) from queue", "Parameter of function 'ABS' invalid (parameter type: HashMap)")
        );
    }
}
