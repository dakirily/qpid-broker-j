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
 * Tests designed to verify the public class {@link LikeExpression} functionality
 */
public class LikeExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @ParameterizedTest
    @MethodSource("percentWildcardQueries")
    public void percentWildcard(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("notLikePercentWildcardQueries")
    public void notLikeUsingPercentWildcard(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("questionMarkWildcardQueries")
    public void questionMarkWildcard(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("notLikeQuestionMarkWildcardQueries")
    public void notLikeUsingQuestionMarkWildcard(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("escapeQueries")
    public void escape(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("patternMatchingQueries")
    public void patternMatching(final String query, final int expectedSize, final String expectedKey, final List<Object> expectedValues)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(expectedSize, result.size());
        if (expectedValues != null && expectedKey != null)
        {
            for (int i = 0; i < expectedValues.size(); i++)
            {
                assertEquals(expectedValues.get(i), result.get(i).get(expectedKey));
            }
        }
        if (expectedValues != null && expectedKey == null)
        {
            for (int i = 0; i < expectedValues.size(); i++)
            {
                assertEquals(expectedValues.get(i), result.get(i).values().iterator().next());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("optionalBracketQueries")
    public void optionalBrackets(final String query, final boolean expected)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expected, result.get(0).get("result"));
    }

    @Test()
    public void repeatedWildcards()
    {
        String query = "select 'test' like 't%%%' as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(true, result.get(0).get("result"));
    }

    private static Stream<Arguments> percentWildcardQueries()
    {
        return Stream.of(
                Arguments.of("select 'test' like 't%' as result", true),
                Arguments.of("select 'test' like 'te%' as result", true),
                Arguments.of("select 'test' like 'tes%' as result", true),
                Arguments.of("select 'test' like 'test%' as result", true),
                Arguments.of("select 'test' like '%t' as result", true),
                Arguments.of("select 'test' like '%st' as result", true),
                Arguments.of("select 'test' like '%est' as result", true),
                Arguments.of("select 'test' like '%test' as result", true),
                Arguments.of("select 'test' like '%test%' as result", true),
                Arguments.of("select 'test' like '%es%' as result", true),
                Arguments.of("select 'test' like '%e%' as result", true),
                Arguments.of("select 'test' like '%s%' as result", true),
                Arguments.of("select 'test' like '%' as result", true)
        );
    }

    private static Stream<Arguments> notLikePercentWildcardQueries()
    {
        return Stream.of(
                Arguments.of("select 'test' like 'T%' as result", false),
                Arguments.of("select 'test' like 'et%' as result", false),
                Arguments.of("select 'test' like '%T' as result", false),
                Arguments.of("select '123test123' like '3%`T`' as result", false)
        );
    }

    private static Stream<Arguments> questionMarkWildcardQueries()
    {
        return Stream.of(
                Arguments.of("select 'test' like 'tes?' as result", true),
                Arguments.of("select 'test' like 'te?t' as result", true),
                Arguments.of("select 'test' like 't?st' as result", true),
                Arguments.of("select 'test' like '?est' as result", true),
                Arguments.of("select 'test' like '?es?' as result", true),
                Arguments.of("select 'test' like '????' as result", true)
        );
    }

    private static Stream<Arguments> notLikeQuestionMarkWildcardQueries()
    {
        return Stream.of(
                Arguments.of("select 'test' like 'Tes?' as result", false),
                Arguments.of("select 'tesT' like 'te?t' as result", false),
                Arguments.of("select 'test_123' like 'test_124' as result", false)
        );
    }

    private static Stream<Arguments> escapeQueries()
    {
        return Stream.of(
                Arguments.of("select 'test 100% test' like '% 100#% test' escape '#' as result", true),
                Arguments.of("select 'test? really?' like 'test#? %' escape '#' as result", true)
        );
    }

    private static Stream<Arguments> patternMatchingQueries()
    {
        return Stream.of(
                Arguments.of("select name from queue where lower(name) like 'queue_1?' ", 10, null, null),
                Arguments.of("select name from queue where lower(name) like 'queue_2?' ", 10, null, null),
                Arguments.of("select name from queue where lower(name) like 'queue_3?' ", 10, null, null),
                Arguments.of("select name from queue where lower(name) like 'queue_%' ", 70, null, null),
                Arguments.of("select count(*) from queue where lower(overflowPolicy) like 'r%' ", 1, "count(*)", List.of(20)),
                Arguments.of("select distinct overflowPolicy from queue where lower(overflowPolicy) like '%flow%' ", 2, "overflowPolicy", List.of("FLOW_TO_DISK", "PRODUCER_FLOW_CONTROL"))
        );
    }

    private static Stream<Arguments> optionalBracketQueries()
    {
        return Stream.of(
                Arguments.of("select 'test 100% test' like ('% 100#% test' escape '#') as result", true),
                Arguments.of("select 'test 100% test' not like ('% 100#% test' escape '#') as result", false),
                Arguments.of("select 'test? really?' like ('test#? %' escape '#') as result", true),
                Arguments.of("select 'test? really?' not like ('test#? %' escape '#') as result", false)
        );
    }
}
