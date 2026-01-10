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

package org.apache.qpid.server.security;

import javax.security.auth.Subject;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Lightweight replacement for java.security.AccessController/Subject.doAs().
 * <br>
 * Maintains the current {@link Subject} in a thread local so code that previously relied on the
 * SecurityManager can still retrieve the active subject.
 */
public final class SubjectExecutionContext
{
    private static final ThreadLocal<Subject> CURRENT = new ThreadLocal<>();

    private SubjectExecutionContext()
    {
        // utility class has private constructor
    }

    public static Subject currentSubject()
    {
        return CURRENT.get();
    }

    public static <T> T withSubject(final Subject subject, final Callable<T> action) throws Exception
    {
        final Subject original = CURRENT.get();
        try
        {
            setSubject(subject);
            return action.call();
        }
        finally
        {
            restoreSubject(original);
        }
    }

    public static <T> T withSubjectUnchecked(final Subject subject, final Callable<T> action)
    {
        final Subject original = CURRENT.get();
        try
        {
            setSubject(subject);
            return action.call();
        }
        catch (RuntimeException | Error e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new SubjectActionException(e);
        }
        finally
        {
            restoreSubject(original);
        }
    }

    public static void withSubject(final Subject subject, final Runnable runnable)
    {
        final Subject original = CURRENT.get();
        try
        {
            setSubject(subject);
            runnable.run();
        }
        finally
        {
            restoreSubject(original);
        }
    }

    public static Throwable unwrapSubjectActionException(final Throwable throwable)
    {
        return ((throwable instanceof SubjectActionException sae && sae.getCause() != null)
                ? sae.getCause() : throwable);
    }

    public static <T extends Exception> T unwrapSubjectActionException(final Throwable throwable,
                                                                       final Class<T> desiredType,
                                                                       final Function<Throwable, T> function)
    {
        if (desiredType.isInstance(throwable))
        {
            return desiredType.cast(throwable);
        }

        final Throwable cause = throwable.getCause();

        if (desiredType.isInstance(cause))
        {
            return desiredType.cast(cause);
        }

        return function.apply(throwable);
    }

    private static void setSubject(final Subject subject)
    {
        if (subject == null)
        {
            CURRENT.remove();
        }
        else
        {
            CURRENT.set(subject);
        }
    }

    private static void restoreSubject(final Subject subject)
    {
        if (subject == null)
        {
            CURRENT.remove();
        }
        else
        {
            CURRENT.set(subject);
        }
    }
}
