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
package org.apache.qpid.server.query.engine.parsing.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryValidationException;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;

/**
 * Tests designed to verify the {@link ConditionalExpressionFactory} functionality
 */
public class ConditionalExpressionFactoryTest
{
    @BeforeEach()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public void caseWithNullConditions()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ConditionalExpressionFactory.caseExpression(null, new ArrayList<>()));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSIONS_NULL, exception.getMessage());
    }

    @Test()
    public void caseWithNullOutcomes()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ConditionalExpressionFactory.caseExpression(new ArrayList<>(), null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSIONS_NULL, exception.getMessage());
    }

    @Test()
    public void caseWithEmptyConditions()
    {
        QueryValidationException exception = assertThrows(QueryValidationException.class, () -> ConditionalExpressionFactory.caseExpression(new ArrayList<>(), Arrays.asList(ConstantExpression.of(1))));
        assertEquals(Errors.VALIDATION.CASE_CONDITIONS_EMPTY, exception.getMessage());
    }

    @Test()
    public void caseWithEmptyOutcomes()
    {
        QueryValidationException exception = assertThrows(QueryValidationException.class, () -> ConditionalExpressionFactory.caseExpression(Collections.singletonList(ConstantExpression.of(1)), Collections.emptyList()));
        assertEquals(Errors.VALIDATION.CASE_OUTCOMES_EMPTY, exception.getMessage());
    }
}

