/*
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
 */

package org.apache.qpid.server.store.berkeleydb.tuple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.test.utils.UnitTestBase;

class MapBindingTest extends UnitTestBase
{
    private static final MapBinding BINDING = MapBinding.getInstance();
    private static final Map<String, Object> MAP = Map.of("key", "value", "flag", Boolean.TRUE);

    @Test
    void testEntryToObjectReadsJson()
    {
        assertEquals(Map.of("key", "value"), BINDING.entryToObject(jsonTuple("{\"key\":\"value\"}")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{", "[]", "\"x\""})
    void testEntryToObjectRejectsInvalidMap(final String json)
    {
        assertThrows(StoreException.class, () -> BINDING.entryToObject(jsonTuple(json)));
    }

    @Test
    void testObjectToEntryWritesJson()
    {
        final TupleOutput output = new TupleOutput();
        BINDING.objectToEntry(Map.of("key", "value"), output);

        final ObjectMapper mapper = ConfiguredObjectJacksonModule.newObjectMapper(true);
        final TupleInput input = new TupleInput(output.toByteArray());
        assertEquals(mapper.readTree("{\"key\":\"value\"}"), mapper.readTree(input.readString()));
    }

    @Test
    void testObjectToEntryRejectsSelfReferentialMap()
    {
        final Map<String, Object> map = new HashMap<>();
        map.put("self", map);

        assertThrows(StoreException.class, () -> BINDING.objectToEntry(map, new TupleOutput()));
    }

    @Test
    void testRoundTripThroughTuple()
    {
        final TupleOutput output = new TupleOutput();
        BINDING.objectToEntry(MAP, output);

        assertEquals(MAP, BINDING.entryToObject(new TupleInput(output.toByteArray())));
    }

    @Test
    void testRoundTripThroughDatabaseEntry()
    {
        final DatabaseEntry entry = new DatabaseEntry();
        BINDING.objectToEntry(MAP, entry);

        assertEquals(MAP, BINDING.entryToObject(entry));
    }

    @Test
    void testEntryToObjectWithNonZeroOffset()
    {
        final DatabaseEntry entry = new DatabaseEntry();
        BINDING.objectToEntry(MAP, entry);

        final int offset = 7;
        final int size = entry.getSize();
        final byte[] backing = new byte[offset + size + 4];
        Arrays.fill(backing, (byte) 0x5a);
        System.arraycopy(entry.getData(), entry.getOffset(), backing, offset, size);

        assertEquals(MAP, BINDING.entryToObject(new DatabaseEntry(backing, offset, size)));
    }

    @Test
    void testRoundTripWithNestedStructures()
    {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("string", "value");
        map.put("intVal", 42);
        map.put("longVal", Integer.MAX_VALUE + 1L);
        map.put("doubleVal", 1.5d);
        map.put("boolVal", Boolean.FALSE);
        map.put("nullVal", null);
        map.put("nestedMap", Map.of("nestedString", "nested", "nestedInt", 7));
        map.put("list", Arrays.asList("a", 1, Integer.MAX_VALUE + 2L, Boolean.TRUE, null, List.of("x", "y")));
        map.put("emptyMap", Map.of());
        map.put("emptyList", List.of());

        final DatabaseEntry entry = new DatabaseEntry();
        BINDING.objectToEntry(map, entry);

        assertEquals(map, BINDING.entryToObject(entry));
    }

    private static TupleInput jsonTuple(final String json)
    {
        final TupleOutput output = new TupleOutput();
        output.writeString(json);
        return new TupleInput(output.toByteArray());
    }
}
