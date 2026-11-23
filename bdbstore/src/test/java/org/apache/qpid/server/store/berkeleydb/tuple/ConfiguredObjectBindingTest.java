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
package org.apache.qpid.server.store.berkeleydb.tuple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConfiguredObjectBindingTest extends UnitTestBase
{
    private static final UUID OBJECT_ID = UUID.fromString("5e4feab0-d0c5-42ac-866c-2e0cd4057907");
    private static final String OBJECT_TYPE = "dummyType";
    private static final Map<String, Object> ATTRIBUTES = Map.of("dummy", "attributes");
    private static final ConfiguredObjectRecord OBJECT =
            new ConfiguredObjectRecordImpl(OBJECT_ID, OBJECT_TYPE, ATTRIBUTES);
    private static final ConfiguredObjectBinding WRITER = ConfiguredObjectBinding.getInstance();
    private static final ConfiguredObjectBinding READER = new ConfiguredObjectBinding(OBJECT_ID);

    @Test
    public void testEntryToObjectReadsIdFromKey()
    {
        assertRecord(READER.entryToObject(configuredObjectTuple("{\"dummy\":\"attributes\"}")), ATTRIBUTES);
    }

    @Test
    public void testObjectToEntryAndEntryToObject()
    {
        final TupleOutput output = new TupleOutput();
        WRITER.objectToEntry(OBJECT, output);

        final byte[] bytes = output.toByteArray();
        assertEquals(OBJECT_TYPE, new TupleInput(bytes).readString());
        assertRecord(READER.entryToObject(new TupleInput(bytes)), ATTRIBUTES);
    }

    @Test
    public void testRoundTripWithComplexAttributes()
    {
        final Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("string", "value");
        attributes.put("int", 123);
        attributes.put("long", Integer.MAX_VALUE + 42L);
        attributes.put("double", 1.25d);
        attributes.put("bool", Boolean.TRUE);
        attributes.put("nullValue", null);
        attributes.put("map", Map.of("nestedInt", 7, "nestedString", "nested"));
        attributes.put("list", Arrays.asList(1, "x", Boolean.FALSE, null));
        attributes.put("emptyMap", Map.of());
        attributes.put("emptyList", List.of());

        final ConfiguredObjectRecord object = new ConfiguredObjectRecordImpl(OBJECT_ID, OBJECT_TYPE, attributes);
        final TupleOutput output = new TupleOutput();
        WRITER.objectToEntry(object, output);

        final ConfiguredObjectRecord storedObject = READER.entryToObject(new TupleInput(output.toByteArray()));
        assertRecord(storedObject, attributes);
        assertThrows(UnsupportedOperationException.class, () -> storedObject.getAttributes().put("x", "y"));
    }

    @Test
    public void testRoundTripThroughDatabaseEntry()
    {
        final DatabaseEntry entry = new DatabaseEntry();
        WRITER.objectToEntry(OBJECT, entry);

        assertRecord(READER.entryToObject(entry), ATTRIBUTES);
    }

    @Test
    public void testEntryToObjectWithNonZeroOffset()
    {
        final DatabaseEntry entry = new DatabaseEntry();
        WRITER.objectToEntry(OBJECT, entry);

        final int offset = 7;
        final int size = entry.getSize();
        final byte[] backing = new byte[offset + size + 3];
        Arrays.fill(backing, (byte) 0x5a);
        System.arraycopy(entry.getData(), entry.getOffset(), backing, offset, size);

        assertRecord(READER.entryToObject(new DatabaseEntry(backing, offset, size)), ATTRIBUTES);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{", "[]", "\"x\""})
    public void testEntryToObjectRejectsInvalidAttributes(final String json)
    {
        assertThrows(StoreException.class, () -> READER.entryToObject(configuredObjectTuple(json)));
    }

    @Test
    public void testEntryToObjectRejectsNullAttributes()
    {
        // JSON null reaches record construction as a null attributes map.
        assertThrows(NullPointerException.class, () -> READER.entryToObject(configuredObjectTuple("null")));
    }

    @Test
    public void testObjectToEntryRejectsSelfReferentialAttributes()
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("self", attributes);
        final ConfiguredObjectRecord object = new ConfiguredObjectRecordImpl(OBJECT_ID, OBJECT_TYPE, attributes);

        assertThrows(StoreException.class, () -> WRITER.objectToEntry(object, new TupleOutput()));
    }

    private static TupleInput configuredObjectTuple(final String attributesJson)
    {
        final TupleOutput output = new TupleOutput();
        output.writeString(OBJECT_TYPE);
        output.writeString(attributesJson);
        return new TupleInput(output.toByteArray());
    }

    private static void assertRecord(final ConfiguredObjectRecord record, final Map<String, Object> attributes)
    {
        assertEquals(OBJECT_ID, record.getId(), "Unexpected id");
        assertEquals(OBJECT_TYPE, record.getType(), "Unexpected type");
        assertEquals(attributes, record.getAttributes(), "Unexpected attributes");
    }
}
