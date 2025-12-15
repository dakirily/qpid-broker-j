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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.BetweenExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.EqualExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.GreaterThanExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.GreaterThanOrEqualExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.InExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.IsNullExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.LessThanExpression;
import org.apache.qpid.server.query.engine.parsing.expression.comparison.LessThanOrEqualExpression;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;
import org.apache.qpid.server.query.engine.parsing.expression.logic.NotExpression;
import org.apache.qpid.server.query.engine.parsing.query.SelectExpression;

/**
 * Tests designed to verify the {@link ComparisonExpressionFactory} functionality
 */
public class ComparisonExpressionFactoryTest
{
    @BeforeEach()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public void betweenWithNullLeft()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.betweenExpression(null, null, ConstantExpression.of(1), ConstantExpression.of(2)));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public void betweenWithNullLow()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.betweenExpression(null, ConstantExpression.of(1), null, ConstantExpression.of(2)));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public void betweenWithNullHigh()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.betweenExpression(null, ConstantExpression.of(1), ConstantExpression.of(2), null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T> void between()
    {
       ExpressionNode<T, Boolean> expression = ComparisonExpressionFactory.betweenExpression(
            null,
            ConstantExpression.of(1),
            ConstantExpression.of(2), ConstantExpression.of(3));
       assertEquals(BetweenExpression.class, expression.getClass());
    }

    @Test()
    public void equalWithNullLeft()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.equalExpression(null, ConstantExpression.of(1)));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public void equalWithNullRight()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.equalExpression(ConstantExpression.of(1), null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T> void equal()
    {
        Predicate<T> expression = ComparisonExpressionFactory.equalExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(EqualExpression.class, expression.getClass());
    }

    @Test()
    public void notEqualWithNullLeft()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.notEqualExpression(null, ConstantExpression.of(1)));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public void notEqualWithNullRight()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.notEqualExpression(ConstantExpression.of(1), null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T> void notEqual()
    {
        Predicate<T> expression = ComparisonExpressionFactory.notEqualExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(NotExpression.class, expression.getClass());
    }

    @Test()
    public void greaterThanWithNullLeft()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.greaterThanExpression(null, ConstantExpression.of(1)));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public void greaterThanWithNullRight()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.greaterThanExpression(ConstantExpression.of(1), null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T> void greaterThan()
    {
        Predicate<T> expression = ComparisonExpressionFactory.greaterThanExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(GreaterThanExpression.class, expression.getClass());
    }

    @Test()
    public void greaterThanOrEqualWithNullLeft()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.greaterThanOrEqualExpression(null, ConstantExpression.of(1)));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public void greaterThanOrEqualWithNullRight()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.greaterThanOrEqualExpression(ConstantExpression.of(1), null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T> void greaterThanOrEqual()
    {
        Predicate<T> expression = ComparisonExpressionFactory.greaterThanOrEqualExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(GreaterThanOrEqualExpression.class, expression.getClass());
    }

    @Test()
    public void lessThanWithNullLeft()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.lessThanExpression(null, ConstantExpression.of(1)));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public void lessThanWithNullRight()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.lessThanExpression(ConstantExpression.of(1), null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T> void lessThan()
    {
        Predicate<T> expression = ComparisonExpressionFactory.lessThanExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(LessThanExpression.class, expression.getClass());
    }

    @Test()
    public void lessThanOrEqualWithNullLeft()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.lessThanOrEqualExpression(null, ConstantExpression.of(1)));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public void lessThanOrEqualWithNullRight()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.lessThanOrEqualExpression(ConstantExpression.of(1), null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T> void lessThanOrEqual()
    {
        Predicate<T> expression = ComparisonExpressionFactory.lessThanOrEqualExpression(
                ConstantExpression.of(1),
                ConstantExpression.of(2));
        assertEquals(LessThanOrEqualExpression.class, expression.getClass());
    }

    @Test()
    public void inWithNullLeft()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.inExpression(null,new ArrayList<>()));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());

        exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.inExpression(null, new SelectExpression<>()));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T, R> void inWithNullRight()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.inExpression(ConstantExpression.of(null), (List<ExpressionNode<T, R>>) null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSIONS_NULL, exception.getMessage());

        exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.inExpression(ConstantExpression.of(null), (SelectExpression<T, R>) null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T> void in()
    {
        Predicate<T> expression = ComparisonExpressionFactory.inExpression(
                ConstantExpression.of(1),
                new ArrayList<>());
        assertEquals(InExpression.class, expression.getClass());
    }

    @Test()
    public void isNullWithNullExpression()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.isNullExpression(null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T> void isNull()
    {
        Predicate<T> expression = ComparisonExpressionFactory.isNullExpression(ConstantExpression.of(1));
        assertEquals(IsNullExpression.class, expression.getClass());
    }

    @Test()
    public void isNotNullWithNullExpression()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.isNotNullExpression(null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    public <T> void isNotNull()
    {
        Predicate<T> expression = ComparisonExpressionFactory.isNotNullExpression(ConstantExpression.of(1));
        assertEquals(NotExpression.class, expression.getClass());
    }

    @Test()
    public void likeWithNullExpression()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> ComparisonExpressionFactory.likeExpression(null, "", ""));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }
}

