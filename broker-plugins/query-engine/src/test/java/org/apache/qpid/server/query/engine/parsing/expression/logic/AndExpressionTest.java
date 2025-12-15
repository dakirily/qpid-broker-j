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
package org.apache.qpid.server.query.engine.parsing.expression.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;

/**
 * Tests designed to verify the {@link AndExpression} functionality
 */
public class AndExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @ParameterizedTest
    @MethodSource("andQueries")
    public void and(final String query, final String expectedKey, final boolean expectedValue)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expectedValue, result.get(0).get(expectedKey));
    }

    private static Stream<Arguments> andQueries()
    {
        return Stream.of(
                Arguments.of("select true and true", "true and true", true),
                Arguments.of("select true and false", "true and false", false),
                Arguments.of("select false and true", "false and true", false),
                Arguments.of("select false and false", "false and false", false),
                Arguments.of("select (true and true) and false", "(true and true) and false", false),
                Arguments.of("select (true and true) and (false and true)", "(true and true) and (false and true)", false),
                Arguments.of("select 2 > 1 and 2 < 3", "2>1 and 2<3", true),
                Arguments.of("select (2 >= 1 and 2 <= 3) as expr", "expr", true)
        );
    }
}
