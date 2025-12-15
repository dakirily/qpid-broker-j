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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

/**
 * Tests designed to verify the public class {@link DateExpression} functionality
 */
public class DateExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    private final QuerySettings _querySettings = new QuerySettings();

    @Test()
    public void extractDateFromString()
    {
        String query = "select date('2000-01-01 00:00:00') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals("2000-01-01", result.get(0).get("result"));
    }

    @Test()
    public void extractDateFromField()
    {
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("uuuu-MM-dd"));
        String query = "select date(lastUpdatedTime) as result from queue where name = 'QUEUE_0'";
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(1, result.size());
        assertEquals(date, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("comparisonQueries")
    public void compareDates(final String query, final int expectedSize, final List<String> expectedAliases)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query, _querySettings).getResults();
        assertEquals(expectedSize, result.size());
        if (expectedAliases != null)
        {
            for (int i = 0; i < expectedAliases.size(); i++)
            {
                assertEquals(expectedAliases.get(i), result.get(i).get("alias"));
            }
        }
    }

    private static Stream<Arguments> comparisonQueries()
    {
        return Stream.of(
                Arguments.of("SELECT * FROM certificate WHERE DATE(validFrom) = '2020-01-01'", 1, List.of("aaa_mock")),
                Arguments.of("SELECT * FROM certificate WHERE DATE(validFrom) <> ('2020-01-01')", 9, List.of("bbb_mock", "ccc_mock", "ddd_mock", "eee_mock", "fff_mock", "ggg_mock", "hhh_mock", "iii_mock", "jjj_mock")),
                Arguments.of("SELECT * FROM certificate WHERE DATE(validFrom) > '2020-01-09'", 1, List.of("jjj_mock")),
                Arguments.of("SELECT * FROM certificate WHERE DATE(validFrom) >= '2020-01-09'", 2, List.of("iii_mock", "jjj_mock")),
                Arguments.of("SELECT * FROM certificate WHERE DATE(validFrom) < '2020-01-02'", 1, List.of("aaa_mock")),
                Arguments.of("SELECT * FROM certificate WHERE DATE(validFrom) <= '2020-01-02'", 2, List.of("aaa_mock", "bbb_mock")),
                Arguments.of("SELECT * FROM certificate WHERE DATE(validFrom) BETWEEN ('2020-01-01', '2020-01-03')", 3, List.of("aaa_mock", "bbb_mock", "ccc_mock")),
                Arguments.of("SELECT * FROM certificate WHERE DATE(validFrom) NOT BETWEEN ('2020-01-02', '2020-01-09')", 2, List.of("aaa_mock", "jjj_mock")),
                Arguments.of("SELECT * FROM certificate WHERE DATE(validFrom) IN ('2020-01-01', '2020-01-03', '2020-01-05', '2020-01-07', '2020-01-09')", 5, List.of("aaa_mock", "ccc_mock", "eee_mock", "ggg_mock", "iii_mock")),
                Arguments.of("SELECT * FROM certificate WHERE DATE(validFrom) NOT IN ('2020-01-01', '2020-01-03', '2020-01-05', '2020-01-07', '2020-01-09')", 5, List.of("bbb_mock", "ddd_mock", "fff_mock", "hhh_mock", "jjj_mock"))
        );
    }
}
