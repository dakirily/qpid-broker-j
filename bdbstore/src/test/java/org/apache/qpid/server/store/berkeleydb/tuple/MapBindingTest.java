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

import java.util.HashMap;
import java.util.Map;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
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
    void testObjectToEntry() throws Exception
    {
        final TupleOutput output = mock(TupleOutput.class);
        final Map<String, Object> map = new HashMap<>();
        map.put("key", "value");

        mapBinding.objectToEntry(map, output);

        verify(output).writeString(objectMapper.writeValueAsString(map));
    }
}
