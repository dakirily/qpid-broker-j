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

import java.util.ArrayList;
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
 * Tests designed to verify the aliases functionality
 */
public class AliasTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @ParameterizedTest
    @MethodSource("fieldQueries")
    public void fields(final String query, final List<String> expectedKeys)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(70, result.size());
        assertEquals(expectedKeys, new ArrayList<>(result.get(0).keySet()));
    }

    @ParameterizedTest
    @MethodSource("aliasQueries")
    public void aliases(final String query, final String expectedKey, final Integer expectedSize)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        if (expectedSize != null)
        {
            assertEquals(expectedSize.intValue(), result.size());
        }
        assertEquals(expectedKey, result.get(0).keySet().iterator().next());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void caseExpression()
    {
        String query = "select case when 1 > 2 then 1 else 2 end";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals("case when 1>2 then 1 else 2 end", result.get(0).keySet().iterator().next());

        query = "select (case when 1 > 2 then 1 else 2 end)";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("(case when 1>2 then 1 else 2 end)", result.get(0).keySet().iterator().next());

        query = "select "
            + "case "
            + "when (maximumQueueDepthMessages = -1 and maximumQueueDepthBytes = -1) or queueDepthMessages < maximumQueueDepthMessages * 0.6 then 'good' "
            + "when queueDepthMessages < maximumQueueDepthMessages * 0.9 then 'bad' "
            + "else 'critical' "
            + "end "
            + "from queue";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("case "
            + "when (maximumQueueDepthMessages=-1 and maximumQueueDepthBytes=-1) or queueDepthMessages<maximumQueueDepthMessages*0.6 then 'good' "
            + "when queueDepthMessages<maximumQueueDepthMessages*0.9 then 'bad' "
            + "else 'critical' "
            + "end", result.get(0).keySet().iterator().next());

        query = "select "
                + "case "
                + "when (maximumQueueDepthMessages = -1 and maximumQueueDepthBytes = -1) or queueDepthMessages < maximumQueueDepthMessages * 0.6 then 'good' "
                + "when queueDepthMessages < maximumQueueDepthMessages * 0.9 then 'bad' "
                + "else 'critical' "
                + "end, "
                + "count(*) "
                + "from queue group by 1";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals("bad", ((Map<String,Object>)result.get(0).get("count(*)")).keySet().stream().skip(0).findFirst().orElse(null));
        assertEquals("critical", ((Map<String,Object>)result.get(0).get("count(*)")).keySet().stream().skip(1).findFirst().orElse(null));
        assertEquals("good", ((Map<String,Object>)result.get(0).get("count(*)")).keySet().stream().skip(2).findFirst().orElse(null));
    }

    private static Stream<Arguments> fieldQueries()
    {
        return Stream.of(
                Arguments.of("select id, name, description from queue", List.of("id", "name", "description")),
                Arguments.of("select id as ID, name as NAME, description as DSC from queue", List.of("ID", "NAME", "DSC")),
                Arguments.of("select id as \"ID\", name as \"NAME\", description as \"DSC\" from queue", List.of("ID", "NAME", "DSC")),
                Arguments.of("select id ID, name NAME, description DSC from queue", List.of("ID", "NAME", "DSC")),
                Arguments.of("select id \"ID\", name \"NAME\", description \"DSC\" from queue", List.of("ID", "NAME", "DSC"))
        );
    }

    private static Stream<Arguments> aliasQueries()
    {
        return Stream.of(
                Arguments.of("select abs(queueDepthMessages) from queue", "abs(queueDepthMessages)", null),
                Arguments.of("select abs(1/3 - 12)", "abs(1/3-12)", null),
                Arguments.of("select true and false", "true and false", null),
                Arguments.of("select 1 > 0 and 2 < 5", "1>0 and 2<5", null),
                Arguments.of("select avg(queueDepthMessages) from queue", "avg(queueDepthMessages)", null),
                Arguments.of("select avg(queueDepthMessages) + 1 from queue", "avg(queueDepthMessages)+1", null),
                Arguments.of("select 1 + avg(queueDepthMessages) + 1 from queue", "1+avg(queueDepthMessages)+1", null),
                Arguments.of("select 'aba' between 'aaa' and 'bbb'", "'aba' between 'aaa' and 'bbb'", 1),
                Arguments.of("select 'aba' BETWEEN 'aaa' AND 'bbb'", "'aba' BETWEEN 'aaa' AND 'bbb'", 1),
                Arguments.of("select 1 between -111 and +111", "1 between -111 and +111", 1),
                Arguments.of("select coalesce(null, 1)", "coalesce(null, 1)", 1),
                Arguments.of("select coalesce(null, null + 'test', 3)", "coalesce(null, null+'test', 3)", 1),
                Arguments.of("select count(coalesce(description, 'empty')) from queue having coalesce(description, 'empty') <> 'empty'", "count(coalesce(description, 'empty'))", 1),
                Arguments.of("select concat('hello', ' ',  'world')", "concat('hello', ' ', 'world')", 1),
                Arguments.of("select e.name from exchange e", "e.name", 10),
                Arguments.of("select count(*) from queue", "count(*)", null),
                Arguments.of("select count(distinct overflowPolicy) from queue", "count(distinct overflowPolicy)", null),
                Arguments.of("select current_timestamp()", "current_timestamp()", 1),
                Arguments.of("select date(validUntil) from certificate", "date(validUntil)", 10),
                Arguments.of("select dateadd(day, -30, validUntil) from certificate", "dateadd(day, -30, validUntil)", 10),
                Arguments.of("select datediff(day, current_timestamp(), validUntil) from certificate", "datediff(day, current_timestamp(), validUntil)", 10),
                Arguments.of("select extract(year from validUntil) from certificate", "extract(year from validUntil)", 10),
                Arguments.of("select extract(month from validUntil) from certificate", "extract(month from validUntil)", 10),
                Arguments.of("select extract(week from validUntil) from certificate", "extract(week from validUntil)", 10),
                Arguments.of("select extract(day from validUntil) from certificate", "extract(day from validUntil)", 10),
                Arguments.of("select extract(hour from validUntil) from certificate", "extract(hour from validUntil)", 10),
                Arguments.of("select extract(minute from validUntil) from certificate", "extract(minute from validUntil)", 10),
                Arguments.of("select extract(second from validUntil) from certificate", "extract(second from validUntil)", 10),
                Arguments.of("select extract(millisecond from validUntil) from certificate", "extract(millisecond from validUntil)", 10),
                Arguments.of("select 1 in ('1', 'test', 1.0)", "1 in ('1','test',1)", null),
                Arguments.of("select 1 not in ('1', 'test', 1.0)", "1 not in ('1','test',1)", null),
                Arguments.of("select len(name) from queue", "len(name)", null),
                Arguments.of("select length(name) from queue", "length(name)", null),
                Arguments.of("select lower(name) from queue", "lower(name)", null),
                Arguments.of("select ltrim(name) from queue", "ltrim(name)", null),
                Arguments.of("select max(queueDepthMessages) from queue", "max(queueDepthMessages)", null),
                Arguments.of("select min(queueDepthMessages) from queue", "min(queueDepthMessages)", null),
                Arguments.of("select position('_' in name) from queue", "position('_' in name)", null),
                Arguments.of("select position('_' in name, 1) from queue", "position('_' in name,1)", null),
                Arguments.of("select position('_', name) from queue", "position('_',name)", null),
                Arguments.of("select position('_', name, 1) from queue", "position('_',name,1)", null),
                Arguments.of("select replace(name, '_', '') from queue", "replace(name, '_', '')", null),
                Arguments.of("select rtrim(name) from queue", "rtrim(name)", null),
                Arguments.of("select (select count(*) from queue where queueDepthMessages > 0.6 * maximumQueueDepthMessages) from broker", "select count(*) from queue where queueDepthMessages>0.6*maximumQueueDepthMessages", null),
                Arguments.of("select substring(name, 2) from queue", "substring(name, 2)", null),
                Arguments.of("select substring(name, 2, 5) from queue", "substring(name, 2, 5)", null),
                Arguments.of("select sum(queueDepthMessages) from queue", "sum(queueDepthMessages)", null),
                Arguments.of("select trim(name) from queue", "trim(name)", null),
                Arguments.of("select upper(name) from queue", "upper(name)", null)
        );
    }
}
