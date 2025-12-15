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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the public class {@link IsNullExpression} functionality
 */
public class IsNullExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @ParameterizedTest
    @MethodSource("valueQueries")
    public void comparingValues(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("countQueries")
    public void countNulls(final String query, final int expectedCount)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expectedCount, result.get(0).values().iterator().next());
    }

    private static Stream<Arguments> valueQueries()
    {
        return Stream.of(
                Arguments.of("select 1 is null as result", false),
                Arguments.of("select 1 is not null as result", true),
                Arguments.of("select 1L is null as result", false),
                Arguments.of("select 1L is not null as result", true),
                Arguments.of("select 1.0 is null as result", false),
                Arguments.of("select 1.0 is not null as result", true),
                Arguments.of("select " + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN) + " is null as result", false),
                Arguments.of("select " + BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN) + " is not null as result", true),
                Arguments.of("select '' is null as result", false),
                Arguments.of("select '' is not null as result", true),
                Arguments.of("select null is null as result", true),
                Arguments.of("select null is not null as result", false)
        );
    }

    private static Stream<Arguments> countQueries()
    {
        return Stream.of(
                Arguments.of("select count(*) as cnt from queue where description is null", 40),
                Arguments.of("select count(*) as cnt from queue where description is not null", 30)
        );
    }
}
