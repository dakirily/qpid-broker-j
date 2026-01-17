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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.store.StoreException;

class MapBindingTest
{
    private MapBinding mapBinding;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp()
    {
        mapBinding = MapBinding.getInstance();
        objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(true);
    }

    @Test
    void testEntryToObject() throws Exception
    {
        final TupleInput input = mock(TupleInput.class);
        final Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("key", "value");

        when(input.readString()).thenReturn(objectMapper.writeValueAsString(expectedMap));

        final Map<String, Object> result = mapBinding.entryToObject(input);

        assertEquals(expectedMap, result);
    }

    @Test
    void testEntryToObject_JsonProcessingException()
    {
        final TupleInput input = mock(TupleInput.class);
        when(input.readString()).thenReturn("invalid json");

        assertThrows(StoreException.class, () -> mapBinding.entryToObject(input));
    }

    @Test
    void testEntryToObject_ValidJsonArray_throwsStoreException()
    {
        final TupleInput input = mock(TupleInput.class);
        when(input.readString()).thenReturn("[]");

        assertThrows(StoreException.class, () -> mapBinding.entryToObject(input));
    }

    @Test
    void testEntryToObject_ValidJsonString_throwsStoreException()
    {
        final TupleInput input = mock(TupleInput.class);
        when(input.readString()).thenReturn("\"x\"");

        assertThrows(StoreException.class, () -> mapBinding.entryToObject(input));
    }

    @Test
    void testObjectToEntry() throws Exception
    {
        final TupleOutput output = mock(TupleOutput.class);
        final Map<String, Object> map = new HashMap<>();
        map.put("key", "value");

        mapBinding.objectToEntry(map, output);

        verify(output).writeString(objectMapper.writeValueAsString(map));
    }

    @Test
    void testObjectToEntry_SelfReferentialMap_throwsStoreException()
    {
        final TupleOutput output = new TupleOutput();
        final Map<String, Object> map = new HashMap<>();
        map.put("self", map);

        assertThrows(StoreException.class, () -> mapBinding.objectToEntry(map, output));
    }

    @Test
    void testRoundTrip_ThroughTupleOutputAndInput_preservesMap()
    {
        final Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("key", "value");
        expectedMap.put("flag", Boolean.TRUE);

        final TupleOutput output = new TupleOutput();
        mapBinding.objectToEntry(expectedMap, output);

        final TupleInput input = new TupleInput(output.toByteArray());
        final Map<String, Object> result = mapBinding.entryToObject(input);

        assertEquals(expectedMap, result);
    }

    @Test
    void testRoundTrip_ThroughDatabaseEntry_preservesMap()
    {
        final Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("key", "value");
        expectedMap.put("flag", Boolean.TRUE);

        final DatabaseEntry entry = new DatabaseEntry();
        mapBinding.objectToEntry(expectedMap, entry);

        final Map<String, Object> result = mapBinding.entryToObject(entry);

        assertEquals(expectedMap, result);
    }

    @Test
    void testRoundTrip_ThroughDatabaseEntryWithOffset_preservesMap()
    {
        final Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("key", "value");
        expectedMap.put("flag", Boolean.TRUE);

        final DatabaseEntry entry = new DatabaseEntry();
        mapBinding.objectToEntry(expectedMap, entry);

        final byte[] data = entry.getData();
        final int offset = 7;
        final byte[] backing = new byte[data.length + 11];
        Arrays.fill(backing, (byte) 0x5A);
        System.arraycopy(data, 0, backing, offset, data.length);
        entry.setData(backing, offset, data.length);

        final Map<String, Object> result = mapBinding.entryToObject(entry);

        assertEquals(expectedMap, result);
    }

    @Test
    void testRoundTrip_ComplexMap_preservesNestedStructures()
    {
        final Map<String, Object> expectedMap = createComplexMap();

        final DatabaseEntry entry = new DatabaseEntry();
        mapBinding.objectToEntry(expectedMap, entry);

        final Map<String, Object> result = mapBinding.entryToObject(entry);

        assertEquals(expectedMap, result);
    }

    private static Map<String, Object> createComplexMap()
    {
        final Map<String, Object> map = new LinkedHashMap<>();

        map.put("string", "value");
        map.put("intVal", 42);
        map.put("longVal", ((long) Integer.MAX_VALUE) + 1);
        map.put("doubleVal", 1.5d);
        map.put("boolVal", Boolean.FALSE);
        map.put("nullVal", null);

        final Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("nestedString", "nested");
        nested.put("nestedInt", 7);
        map.put("nestedMap", nested);

        final List<Object> list = new ArrayList<>();
        list.add("a");
        list.add(1);
        list.add(((long) Integer.MAX_VALUE) + 2);
        list.add(Boolean.TRUE);
        list.add(null);
        list.add(Arrays.asList("x", "y"));
        map.put("list", list);

        map.put("emptyMap", new LinkedHashMap<>());
        map.put("emptyList", new ArrayList<>());

        return map;
    }
}
