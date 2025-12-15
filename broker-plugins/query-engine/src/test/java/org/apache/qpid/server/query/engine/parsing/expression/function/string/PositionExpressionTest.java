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
 * Tests designed to verify the public class {@link PositionExpression} functionality
 */
public class PositionExpressionTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void oneArgument()
    {
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> {
            String query = "select position('TEST') as result";
            _queryEvaluator.execute(query);
        });
        assertEquals("Function 'POSITION' requires at least 2 parameters", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("twoArgumentQueries")
    public void twoArguments(final String query, final int expectedPosition)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expectedPosition, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("twoArgumentInQueries")
    public void twoArgumentsIn(final String query, final int expectedPosition)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expectedPosition, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("threeArgumentQueries")
    public void threeArguments(final String query, final int expectedPosition)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expectedPosition, result.get(0).get("result"));
    }

    @ParameterizedTest
    @MethodSource("threeArgumentInQueries")
    public void threeArgumentsIn(final String query, final int expectedPosition)
    {
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals(expectedPosition, result.get(0).get("result"));
    }

    @Test()
    public void extractValueBetweenTwoStrings()
    {
        String query = "select substring('request_be.XX_XXX.C7', position('.', 'request_be.XX_XXX.C7') + 1, len('request_be.XX_XXX.C7') - position('.', 'request_be.XX_XXX.C7') - (len('request_be.XX_XXX.C7') - position('.', 'request_be.XX_XXX.C7', position('.', 'request_be.XX_XXX.C7') + 1) + 1)) as account";
        List<Map<String, Object>> result = _queryEvaluator.execute(query).getResults();
        assertEquals(1, result.size());
        assertEquals("XX_XXX", result.get(0).get("account"));
    }

    @Test()
    public void fourArguments()
    {
        String query = "select position('test', 1, 1, 1) as result";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Function 'POSITION' requires maximum 3 parameters", exception.getMessage());
    }

    @Test()
    public void noArguments()
    {
        String query = "select position() as result";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Function 'POSITION' requires at least 2 parameters", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("firstArgumentInvalidQueries")
    public void firstArgumentInvalid(final String query, final String expectedMessage)
    {
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test()
    public void secondArgumentInvalid()
    {
        String query = "select position('x', statistics) as result from queue";

        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Parameter of function 'POSITION' invalid (parameter type: HashMap)", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("thirdArgumentInvalidQueries")
    public void thirdArgumentInvalid(final String query)
    {
        QueryEvaluationException exception = assertThrows(QueryEvaluationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Function 'POSITION' requires argument 3 to be an integer greater than 0", exception.getMessage());
    }

    private static Stream<Arguments> twoArgumentQueries()
    {
        return Stream.of(
                Arguments.of("select position(4, '123') as result", 0),
                Arguments.of("select position(3, '123') as result", 3),
                Arguments.of("select position('world', 'hello world') as result", 7)
        );
    }

    private static Stream<Arguments> twoArgumentInQueries()
    {
        return Stream.of(
                Arguments.of("select position(4 in '123') as result", 0),
                Arguments.of("select position(3 in '123') as result", 3),
                Arguments.of("select position('world' in 'hello world') as result", 7)
        );
    }

    private static Stream<Arguments> threeArgumentQueries()
    {
        return Stream.of(
                Arguments.of("select position('.', 'X.X.X.X') as result", 2),
                Arguments.of("select position('X', 'X.X.X.X', 1) as result", 1),
                Arguments.of("select position('.', 'X.X.X.X', 2) as result", 2),
                Arguments.of("select position('.', 'X.X.X.X', 3) as result", 4),
                Arguments.of("select position('.', 'X.X.X.X', 4) as result", 4),
                Arguments.of("select position('.', 'X.X.X.X', 5) as result", 6)
        );
    }

    private static Stream<Arguments> threeArgumentInQueries()
    {
        return Stream.of(
                Arguments.of("select position('.' in 'X.X.X.X') as result", 2),
                Arguments.of("select position('.' in 'X.X.X.X', 2) as result", 2),
                Arguments.of("select position('.' in 'X.X.X.X', 3) as result", 4),
                Arguments.of("select position('.' in 'X.X.X.X', 4) as result", 4),
                Arguments.of("select position('.' in 'X.X.X.X', 5) as result", 6)
        );
    }

    private static Stream<Arguments> firstArgumentInvalidQueries()
    {
        return Stream.of(
                Arguments.of("select position('', 'test') as result", "Function 'POSITION' requires argument 1 to be a non-empty string"),
                Arguments.of("select position(null, 'test') as result", "Parameter of function 'POSITION' invalid (parameter type: null)")
        );
    }

    private static Stream<String> thirdArgumentInvalidQueries()
    {
        return Stream.of(
                "select position('x', 'test', 'x') as result from queue",
                "select position('x', 'test', -1) as result from queue",
                "select position('x', 'test', null) as result from queue"
        );
    }
}
