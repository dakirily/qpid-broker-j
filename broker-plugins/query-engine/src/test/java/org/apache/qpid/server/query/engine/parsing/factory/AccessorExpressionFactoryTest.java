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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.query.engine.evaluator.EvaluationContext;
import org.apache.qpid.server.query.engine.evaluator.EvaluationContextHolder;
import org.apache.qpid.server.query.engine.evaluator.settings.QuerySettings;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.parsing.expression.ExpressionNode;
import org.apache.qpid.server.query.engine.parsing.expression.accessor.ChainedObjectAccessor;
import org.apache.qpid.server.query.engine.parsing.expression.accessor.DelegatingCollectionAccessorExpression;
import org.apache.qpid.server.query.engine.parsing.expression.accessor.DelegatingObjectAccessor;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;

/**
 * Tests designed to verify the {@link AccessorExpressionFactory} functionality
 */
public class AccessorExpressionFactoryTest
{
    @BeforeEach()
    public void setUp()
    {
        EvaluationContext ctx = EvaluationContextHolder.getEvaluationContext();
        ctx.put(EvaluationContext.QUERY_DEPTH, new AtomicInteger(0));
        ctx.put(EvaluationContext.QUERY_SETTINGS, new QuerySettings());
    }

    @Test()
    public void delegatingWithNullProperty()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> AccessorExpressionFactory.delegating("", null));
        assertEquals(Errors.VALIDATION.PROPERTY_NAME_NULL, exception.getMessage());
    }

    @Test()
    public <T, R> void delegating()
    {
        ExpressionNode<T, R> expression = AccessorExpressionFactory.delegating("", "test");
        assertEquals(DelegatingObjectAccessor.class, expression.getClass());
    }

    @Test()
    public void chainedWithNullExpression()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> AccessorExpressionFactory.chained("", null, null));
        assertEquals(Errors.VALIDATION.CHILD_EXPRESSION_NULL, exception.getMessage());
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <T, R> void chained()
    {
        ExpressionNode<T, R> expression = (ExpressionNode<T, R>) AccessorExpressionFactory.chained("", ConstantExpression.of("test"), Arrays.asList(ConstantExpression.of("test")));
        assertEquals(ChainedObjectAccessor.class, expression.getClass());

        expression = (ExpressionNode<T, R>) AccessorExpressionFactory.chained("", LiteralExpressionFactory.string("'test'"), null);
        assertEquals(DelegatingObjectAccessor.class, expression.getClass());
    }

    @Test()
    public void collectionWithNullProperty()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> AccessorExpressionFactory.collection("", null, null));
        assertEquals(Errors.VALIDATION.PROPERTY_NAME_NULL, exception.getMessage());
    }

    @Test()
    public void collectionWithNullIndex()
    {
        NullPointerException exception = assertThrows(NullPointerException.class, () -> AccessorExpressionFactory.collection("", "test", null));
        assertEquals(Errors.VALIDATION.INDEX_NULL, exception.getMessage());
    }

    @Test()
    @SuppressWarnings("unchecked")
    public <T, R> void collection()
    {
        ExpressionNode<T, R> expression = AccessorExpressionFactory.collection("", "test", (ExpressionNode<T, R>)ConstantExpression.of(1));
        assertEquals(DelegatingCollectionAccessorExpression.class, expression.getClass());
    }
}

