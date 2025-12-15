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
package org.apache.qpid.server.query.engine.parsing.expression.function.string;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryEvaluationException;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;

/**
 * Tests designed to verify the {@link ReplaceExpression} functionality
 */
public class ReplaceExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void noArguments()
    {
        String query = "select replace() as result";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Function 'REPLACE' requires 3 parameters", exception.getMessage());
    }

    @Test()
    public void oneArgument()
    {
        String query = "select replace('test') as result";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Function 'REPLACE' requires 3 parameters", exception.getMessage());
    }

    @Test()
    public void twoArgument()
    {
        String query = "select replace('test', 'e') as result";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Function 'REPLACE' requires 3 parameters", exception.getMessage());
    }

    @Test()
    public void fourArgument()
    {
        String query = "select replace('test', 'e', '', '') as result";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Function 'REPLACE' requires 3 parameters", exception.getMessage());
    }

    @Test()
    public void threeArguments()
    {
        String query = "select replace('test', 't', '') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("es", result.get(0).get("result"));

        query = "select replace('test', 'e', '') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("tst", result.get(0).get("result"));

        query = "select replace('test', 'a', 'b') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).get("result"));

        query = "select replace('test 123', ' ', '_') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("test_123", result.get(0).get("result"));

        query = "select replace('test', 'test', 'x') as result";
        result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("x", result.get(0).get("result"));
    }

    @Test()
    public void firstArgumentNull()
    {
        String query = "select replace(null, 'n', '') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertNull(result.get(0).get("result"));
    }

    @Test()
    public void secondArgumentNull()
    {
        String query = "select replace('test', null, '') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).get("result"));
    }

    @Test()
    public void thirdArgumentNull()
    {
        String query = "select replace('test', 't', null) as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("es", result.get(0).get("result"));
    }

    @Test()
    public void secondArgumentEmptyString()
    {
        String query = "select replace('test', '', 'a') as result";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).get("result"));
    }

    @Test()
    public void firstArgumentInvalid()
    {
        String query = "select replace(statistics, 'n', '') as result from queue";
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Parameter of function 'REPLACE' invalid (parameter type: HashMap)", exception.getMessage());
    }

    @Test()
    public void secondArgumentInvalid()
    {
        String query = "select replace(name, statistics, '') as result from queue";
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Parameter of function 'REPLACE' invalid (parameter type: HashMap)", exception.getMessage());
    }

    @Test()
    public void thirdArgumentInvalid()
    {
        String query = "select replace(name, 'a', statistics) as result from queue";
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Parameter of function 'REPLACE' invalid (parameter type: HashMap)", exception.getMessage());
    }
}

