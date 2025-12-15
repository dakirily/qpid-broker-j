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
package org.apache.qpid.server.query.engine.evaluator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;

/**
 * Tests designed to verify the {@link QueryEvaluator} functionality
 */
public class QueryEvaluatorTest
{
    @Test()
    public void createWithNullBroker()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> new QueryEvaluator(null));
        assertEquals(Errors.EVALUATION.BROKER_NOT_SUPPLIED, exception.getMessage());
    }

    @Test()
    public void createWithNullQuerySettings()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> new QueryEvaluator(null, null, null));
        assertEquals(Errors.EVALUATION.DEFAULT_QUERY_SETTINGS_NOT_SUPPLIED, exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("nullSqlExecutions")
    public void executeNullSql(final Consumer<QueryEvaluator> invocation)
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> {
            QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
            invocation.accept(evaluator);
        });
        assertEquals(Errors.EVALUATION.QUERY_NOT_SUPPLIED, exception.getMessage());
    }

    private static Stream<Consumer<QueryEvaluator>> nullSqlExecutions()
    {
        return Stream.of(
                evaluator -> evaluator.execute(null),
                evaluator -> evaluator.execute(null, new QuerySettings())
        );
    }

    @ParameterizedTest
    @MethodSource("emptySqlExecutions")
    public void executeEmptySql(final Consumer<QueryEvaluator> invocation)
    {
        QueryValidationException exception = assertThrows(QueryValidationException.class, () -> {
            QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
            invocation.accept(evaluator);
        });
        assertEquals(Errors.VALIDATION.QUERY_EMPTY, exception.getMessage());
    }

    private static Stream<Consumer<QueryEvaluator>> emptySqlExecutions()
    {
        return Stream.of(
                evaluator -> evaluator.execute(""),
                evaluator -> evaluator.execute("", new QuerySettings())
        );
    }

    @Test()
    public void executeWithNullQuerySettings()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> {
            QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
            evaluator.execute("select 1 + 1", null);
        });
        assertEquals(Errors.EVALUATION.QUERY_SETTINGS_NOT_SUPPLIED, exception.getMessage());
    }

    @Test()
    public void evaluateWithNullQuery()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> {
            QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
            evaluator.evaluate(null);
        });
        assertEquals(Errors.EVALUATION.QUERY_NOT_SUPPLIED, exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = { "\n", "\r", "\r\n" })
    public void multiLineQuery(final String delimiter)
    {
        final QueryEvaluator evaluator = new QueryEvaluator(null, new QuerySettings(), TestBroker.createBroker());
        final String query = "select * " + delimiter +
                "from queue " + delimiter +
                "where name = 'QUEUE_1'";
        final List<Map<String, Object>> result = evaluator.execute(query).getResults();
        assertEquals(1, result.size());
    }
}

