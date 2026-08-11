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
package org.apache.qpid.server.protocol.v1_0;

import org.apache.qpid.server.protocol.v1_0.constants.Symbols;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CapabilitiesAwareTest
{
    @Test
    void getCapabilities()
    {
        final Source source = new Source();
        assertNull(source.getCapabilities());

        final Attach attach = new Attach();
        assertNotNull(attach.getCapabilities());
        assertInstanceOf(Symbol[].class, attach.getCapabilities());
        assertEquals(0, attach.getCapabilities().length);
    }

    @Test
    void getOutcomes()
    {
        final Source source = new Source();
        assertNull(source.getOutcomes());

        final Attach attach = new Attach();
        assertNotNull(attach.getOutcomes());
        assertInstanceOf(Symbol[].class, attach.getOutcomes());
        assertEquals(0, attach.getOutcomes().length);
    }

    @Test
    void hasOutcomeWithNullOutcomes()
    {
        final Source source = new Source();
        assertFalse(source.hasOutcome(Symbols.AMQP_ACCEPTED));
    }

    @Test
    void hasNullOutcomeWithNullOutcomes()
    {
        final Source source = new Source();
        assertFalse(source.hasOutcome(null));
    }

    @Test
    void hasNullOutcome()
    {
        final Source source = new Source();
        source.setOutcomes(Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE);
        assertFalse(source.hasOutcome(null));
    }

    @Test
    void hasOutcome()
    {
        final Source source = new Source();
        source.setOutcomes(Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE);
        assertTrue(source.hasOutcome(Symbols.AMQP_ACCEPTED));
    }

    @Test
    void hasOutcomeWithNullElements()
    {
        final Source source = new Source();
        source.setOutcomes(null, Symbols.AMQP_ATTACH, null, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE);
        assertTrue(source.hasOutcome(Symbols.AMQP_ACCEPTED));
    }

    @Test
    void doesNotHaveOutcome()
    {
        final Source source = new Source();
        source.setOutcomes(Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE);
        assertFalse(source.hasOutcome(Symbols.AMQP_BEGIN));
    }

    @Test
    void hasCapabilityWithNullCapabilities()
    {
        final Source source = new Source();
        assertFalse(source.hasCapability(Symbols.AMQP_ACCEPTED));
    }

    @Test
    void hasNullCapabilityWithNullCapabilities()
    {
        final Source source = new Source();
        assertFalse(source.hasCapability(null));
    }

    @Test
    void hasNullCapability()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertFalse(source.hasCapability(null));
    }

    @Test
    void hasCapability()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.hasCapability(Symbols.AMQP_ACCEPTED));
    }

    @Test
    void hasCapabilityWithNullElements()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { null, Symbols.AMQP_ATTACH, null, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.hasCapability(Symbols.AMQP_ACCEPTED));
    }

    @Test
    void doesNotHaveCapability()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertFalse(source.hasCapability(Symbols.AMQP_BEGIN));
    }

    @Test
    void hasTwoCapabilities()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.hasCapabilities(Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED));
        assertTrue(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH));
        assertTrue(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED));
    }

    @Test
    void hasTwoCapabilitiesWithNullElements()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { null, Symbols.AMQP_ATTACH, null, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.hasCapabilities(Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED));
        assertTrue(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH));
        assertTrue(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED));
    }

    @Test
    void doesNotHaveTwoCapabilities()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertFalse(source.hasCapabilities(Symbols.AMQP_BEGIN, Symbols.AMQP_CLOSE));
        assertFalse(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_BEGIN));
    }

    @Test
    void hasTwoSameCapabilities()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.hasCapabilities(Symbols.AMQP_ACCEPTED, Symbols.AMQP_ACCEPTED));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ATTACH, Symbols.AMQP_ATTACH));
        assertTrue(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_CLOSE));
    }

    @Test
    void hasThreeCapabilities()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.hasCapabilities(Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE));
        assertTrue(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH));
        assertTrue(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED));
    }

    @Test
    void hasThreeCapabilitiesWithNullElements()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { null, Symbols.AMQP_ATTACH, null, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.hasCapabilities(Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH));
        assertTrue(source.hasCapabilities(Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE));
        assertTrue(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH));
        assertTrue(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED));
    }

    @Test
    void doesNotHaveThreeCapabilities()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertFalse(source.hasCapabilities(Symbols.AMQP_BEGIN, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE));
        assertFalse(source.hasCapabilities(Symbols.AMQP_CLOSE, Symbols.AMQP_BEGIN, Symbols.AMQP_ATTACH));
    }

    @Test
    void containsWithNullCapabilities()
    {
        final Source source = new Source();
        assertFalse(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED));
    }

    @Test
    void containsNullCapabilityWithNullCapabilities()
    {
        final Source source = new Source();
        assertFalse(source.contains(source.getCapabilities(), null));
    }

    @Test
    void containsNullCapability()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertFalse(source.contains(source.getCapabilities(), null));
    }

    @Test
    void containsCapability()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED));
    }

    @Test
    void containsCapabilityWithNullElements()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { null, Symbols.AMQP_ATTACH, null, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED));
    }

    @Test
    void doesNotContainCapability()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertFalse(source.contains(source.getCapabilities(), Symbols.AMQP_BEGIN));
    }

    @Test
    void containsTwoCapabilities()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED));
    }

    @Test
    void containsTwoCapabilitiesWithNullElements()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { null, Symbols.AMQP_ATTACH, null, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED));
    }

    @Test
    void doesNotContainTwoCapabilities()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertFalse(source.contains(source.getCapabilities(), Symbols.AMQP_BEGIN, Symbols.AMQP_CLOSE));
        assertFalse(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_BEGIN));
    }

    @Test
    void containsTwoSameCapabilities()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED, Symbols.AMQP_ACCEPTED));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ATTACH, Symbols.AMQP_ATTACH));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_CLOSE));
    }

    @Test
    void containsThreeCapabilities()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED));
    }

    @Test
    void containsThreeCapabilitiesWithNullElements()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { null, Symbols.AMQP_ATTACH, null, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH, Symbols.AMQP_CLOSE));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_ACCEPTED, Symbols.AMQP_ATTACH));
        assertTrue(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED));
    }

    @Test
    void doesNotContainThreeCapabilities()
    {
        final Source source = new Source();
        source.setCapabilities(new Symbol[] { Symbols.AMQP_ATTACH, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE });
        assertFalse(source.contains(source.getCapabilities(), Symbols.AMQP_BEGIN, Symbols.AMQP_ACCEPTED, Symbols.AMQP_CLOSE));
        assertFalse(source.contains(source.getCapabilities(), Symbols.AMQP_CLOSE, Symbols.AMQP_BEGIN, Symbols.AMQP_ATTACH));
    }
}
