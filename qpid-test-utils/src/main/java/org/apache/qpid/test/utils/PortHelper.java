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

package org.apache.qpid.test.utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread safe helper that hands out free TCP ports.
 *
 * <p>All ports that have ever been issued are remembered in a single
 * process wide set (<em>no two threads can receive the same port</em>).
 * In addition, each thread gets its own private record of the ports
 * it has obtained, so that {@link #waitUntilAllocatedPortsAreFree()}
 * blocks only for ports requested by <em>that</em> thread.</p>
 */
public class PortHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PortHelper.class);

    public static final int START_PORT_NUMBER = 10_000;
    public static final int MIN_PORT_NUMBER   = 1;
    public static final int MAX_PORT_NUMBER   = 49_151;

    private static final int DEFAULT_TIMEOUT_MILLIS = 30_000;

    /** All ports handed out by any thread (thread safe). */
    private final Set<Integer> allocatedPorts = ConcurrentHashMap.newKeySet();

    /** Ports handed out to the <em>current</em> thread. */
    private final ThreadLocal<Set<Integer>> threadAllocatedPorts = ThreadLocal.withInitial(HashSet::new);

    /** Highest port number issued so far (atomic for racy reads/writes). */
    private final AtomicInteger highestIssuedPort = new AtomicInteger(-1);

    /**
     * Return the next free port {@code fromPort}.  Thread safe.
     *
     * @throws IllegalArgumentException if {@code fromPort} is outside {@code [MIN_PORT_NUMBER, MAX_PORT_NUMBER]}.
     * @throws NoSuchElementException   if no free port exists in the range.
     */
    public int getNextAvailable(final int fromPort)
    {
        if (fromPort < MIN_PORT_NUMBER || fromPort > MAX_PORT_NUMBER)
        {
            throw new IllegalArgumentException("Invalid start port: " + fromPort);
        }

        for (int p = fromPort; p <= MAX_PORT_NUMBER; p++)
        {
            if (!isPortAvailable(p)) {
                continue;                         // the OS is using it
            }

            /* -------- critical section -------- */
            synchronized (allocatedPorts)
            {
                if (allocatedPorts.contains(p))
                {
                    continue;                     // another thread grabbed it
                }
                allocatedPorts.add(p);
                threadAllocatedPorts.get().add(p);
                final int port = p;
                highestIssuedPort.updateAndGet(v -> Math.max(v, port));
                return p;
            }
        }
        throw new NoSuchElementException("Could not find an available port ≥ " + fromPort);
    }

    /** Convenience wrapper that starts the scan at the highest port handed out so far (or {@link #START_PORT_NUMBER}). */
    public int getNextAvailable()
    {
        final int start = highestIssuedPort.get() < 0 ? START_PORT_NUMBER : highestIssuedPort.get() + 1;
        return getNextAvailable(start);
    }

    /** Block until every port obtained by <em>this</em> thread is free again. */
    public void waitUntilAllocatedPortsAreFree()
    {
        waitUntilPortsAreFree(new ArrayList<>(threadAllocatedPorts.get()));
    }

    /** Block until <strong>all</strong> ports in {@code ports} are free. */
    private void waitUntilPortsAreFree(final List<Integer> ports)
    {
        LOGGER.debug("Checking if ports {} are free…", ports);
        final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try
        {
            final CompletableFuture<Void>[] futures = new CompletableFuture[ports.size()];
            for (int i = 0; i < ports.size(); i ++)
            {
                final int port = ports.get(i);
                futures[i] = port > 0
                        ? CompletableFuture.runAsync(() -> waitUntilPortIsFree(port), executor)
                        : CompletableFuture.runAsync(() -> {}, executor);
            }
            CompletableFuture.allOf(futures).join();
        }
        finally
        {
            executor.shutdown();
        }
        LOGGER.debug("Ports {} are free", ports);
    }

    private void waitUntilPortIsFree(int port)
    {
        final int timeout = DEFAULT_TIMEOUT_MILLIS;
        final long deadline = System.currentTimeMillis() + timeout;
        boolean previouslyBusy = false;

        while (System.currentTimeMillis() < deadline)
        {
            if (isPortAvailable(port))
            {
                if (previouslyBusy)
                {
                    LOGGER.debug("Port {} is now available", port);
                }
                return;
            }
            previouslyBusy = true;
            try
            {
                Thread.sleep(500);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }

        throw new RuntimeException("Timed out after " + timeout + " ms waiting for port " + port + " to become available");
    }

    /** True iff the OS allows us to bind a {@link ServerSocket} to {@code port}. */
    public boolean isPortAvailable(final int port)
    {
        try (final ServerSocket s = new ServerSocket())
        {
            s.setReuseAddress(true);
            s.bind(new InetSocketAddress(port));
            return true;
        }
        catch (final IOException e)
        {
            LOGGER.debug("Port {} is not free", port);
            return false;
        }
    }
}
