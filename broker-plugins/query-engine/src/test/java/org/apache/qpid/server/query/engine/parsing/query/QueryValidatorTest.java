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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.qpid.server.query.engine.TestBroker;
import org.apache.qpid.server.query.engine.evaluator.QueryEvaluator;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;

/**
 * Tests designed to verify the query validation functionality
 */
public class QueryValidatorTest
{
    private final QueryEvaluator _queryEvaluator = new QueryEvaluator(TestBroker.createBroker());

    @Test()
    public void selectWithoutFields()
    {
        String query = "select";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Missing expression", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("unknownPropertyQueries")
    public void selectUnknownProperty(final String query)
    {
        QueryValidationException exception = assertThrows(QueryValidationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Keyword 'FROM' not found where expected", exception.getMessage());
    }

    @Test()
    public void emptyFrom()
    {
        String query = "select 1 from ";
        QueryParsingException exception = assertThrows(QueryParsingException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Missing domain name", exception.getMessage());
    }

    @Test()
    public void multipleDomains()
    {
        String query = "select 1 from queue,exchange";
        QueryValidationException exception = assertThrows(QueryValidationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Querying from multiple domains not supported", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("joinQueries")
    public void join(final String query)
    {
        QueryValidationException exception = assertThrows(QueryValidationException.class, () -> _queryEvaluator.execute(query));
        assertEquals("Joins are not supported", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("missingGroupByQueries")
    public void missingGroupByItems(final String query, final String expectedMessage)
    {
        QueryValidationException exception = assertThrows(QueryValidationException.class, () -> _queryEvaluator.execute(query));
        assertEquals(expectedMessage, exception.getMessage());
    }

    private static Stream<Arguments> missingGroupByQueries()
    {
        return Stream.of(
                Arguments.of("select count(*), overflowPolicy from queue",
                        "Not a single-group group function: projections [overflowPolicy] should be included in GROUP BY clause"),
                Arguments.of("select count(*), overflowPolicy, expiryPolicy from queue",
                        "Not a single-group group function: projections [overflowPolicy, expiryPolicy] should be included in GROUP BY clause"),
                Arguments.of("select count(*), overflowPolicy, expiryPolicy from queue group by overflowPolicy",
                        "Not a single-group group function: projections [expiryPolicy] should be included in GROUP BY clause")
        );
    }

    private static Stream<Arguments> joinQueries()
    {
        return Stream.of(
                Arguments.of("select 1 from queue q join exchange"),
                Arguments.of("select 1 from queue q join exchange e on (q.name = e.name)")
        );
    }

    private static Stream<Arguments> unknownPropertyQueries()
    {
        return Stream.of(
                Arguments.of("select unknownProperty"),
                Arguments.of("select 1+1, current_timestamp(), unknownProperty")
        );
    }
}
