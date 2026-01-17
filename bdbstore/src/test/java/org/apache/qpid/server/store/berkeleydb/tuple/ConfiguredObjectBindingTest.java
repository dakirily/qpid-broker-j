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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.ConfiguredObjectRecordImpl;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConfiguredObjectBindingTest extends UnitTestBase
{
    private static final Map<String, Object> DUMMY_ATTRIBUTES_MAP =
            Collections.singletonMap("dummy", "attributes");

    private static final String DUMMY_TYPE_STRING = "dummyType";

    private ConfiguredObjectRecord _object;
    private UUID _objectId;

    private ConfiguredObjectBinding _configuredObjectBinding;

    @BeforeEach
    public void setUp() throws Exception
    {
        _configuredObjectBinding = ConfiguredObjectBinding.getInstance();
        _objectId = UUIDGenerator.generateRandomUUID();
        _object = new ConfiguredObjectRecordImpl(_objectId, DUMMY_TYPE_STRING, DUMMY_ATTRIBUTES_MAP);
    }

    @Test
    public void testObjectToEntryAndEntryToObject_usingUuidBindingPreservesIdTypeAndAttributes()
    {
        TupleOutput tupleOutput = new TupleOutput();

        // In the configuration store, UUID is normally derived from the key, not the value.
        _configuredObjectBinding.objectToEntry(_object, tupleOutput);

        byte[] entryAsBytes = tupleOutput.getBufferBytes();
        TupleInput tupleInput = new TupleInput(entryAsBytes);

        ConfiguredObjectBinding readerBinding = new ConfiguredObjectBinding(_objectId);
        ConfiguredObjectRecord storedObject = readerBinding.entryToObject(tupleInput);

        assertEquals(_objectId, storedObject.getId(), "Unexpected id");
        assertEquals(DUMMY_ATTRIBUTES_MAP, storedObject.getAttributes(), "Unexpected attributes");
        assertEquals(DUMMY_TYPE_STRING, storedObject.getType(), "Unexpected type");
    }

    @Test
    public void testRoundTrip_complexAttributes()
    {
        Map<String, Object> complexAttributes = new LinkedHashMap<>();
        complexAttributes.put("string", "value");
        complexAttributes.put("int", 123);
        complexAttributes.put("long", ((long) Integer.MAX_VALUE) + 42L);
        complexAttributes.put("double", 1.25d);
        complexAttributes.put("bool", Boolean.TRUE);
        complexAttributes.put("nullValue", null);

        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("nestedInt", 7);
        nested.put("nestedString", "nested");
        complexAttributes.put("map", nested);

        List<Object> list = new ArrayList<>();
        list.add(1);
        list.add("x");
        list.add(Boolean.FALSE);
        list.add(null);
        complexAttributes.put("list", list);

        complexAttributes.put("emptyMap", new HashMap<>());
        complexAttributes.put("emptyList", new ArrayList<>());

        UUID id = UUIDGenerator.generateRandomUUID();
        ConfiguredObjectRecord object = new ConfiguredObjectRecordImpl(id, DUMMY_TYPE_STRING, complexAttributes);

        TupleOutput out = new TupleOutput();
        _configuredObjectBinding.objectToEntry(object, out);

        TupleInput in = new TupleInput(out.getBufferBytes());
        ConfiguredObjectRecord storedObject = new ConfiguredObjectBinding(id).entryToObject(in);

        assertEquals(id, storedObject.getId(), "Unexpected id");
        assertEquals(DUMMY_TYPE_STRING, storedObject.getType(), "Unexpected type");
        assertEquals(complexAttributes, storedObject.getAttributes(), "Unexpected attributes");

        // Attributes are expected to be immutable.
        assertThrows(UnsupportedOperationException.class,
                () -> storedObject.getAttributes().put("x", "y"),
                "Attributes map must be unmodifiable");
    }

    @Test
    public void testRoundTrip_viaDatabaseEntry_preservesIdTypeAndAttributes()
    {
        DatabaseEntry entry = new DatabaseEntry();

        _configuredObjectBinding.objectToEntry(_object, entry);

        ConfiguredObjectBinding readerBinding = new ConfiguredObjectBinding(_objectId);
        ConfiguredObjectRecord storedObject = readerBinding.entryToObject(entry);

        assertEquals(_objectId, storedObject.getId(), "Unexpected id");
        assertEquals(DUMMY_TYPE_STRING, storedObject.getType(), "Unexpected type");
        assertEquals(DUMMY_ATTRIBUTES_MAP, storedObject.getAttributes(), "Unexpected attributes");
    }

    @Test
    public void testEntryToObject_databaseEntryWithOffset_preservesIdTypeAndAttributes()
    {
        DatabaseEntry entry = new DatabaseEntry();
        _configuredObjectBinding.objectToEntry(_object, entry);

        final byte[] data = entry.getData();
        assertNotNull(data, "Entry data must not be null");

        final int offset = 7;
        final byte[] backing = new byte[data.length + offset + 3];
        System.arraycopy(data, 0, backing, offset, data.length);

        DatabaseEntry entryWithOffset = new DatabaseEntry();
        entryWithOffset.setData(backing, offset, data.length);

        ConfiguredObjectBinding readerBinding = new ConfiguredObjectBinding(_objectId);
        ConfiguredObjectRecord storedObject = readerBinding.entryToObject(entryWithOffset);

        assertEquals(_objectId, storedObject.getId(), "Unexpected id");
        assertEquals(DUMMY_TYPE_STRING, storedObject.getType(), "Unexpected type");
        assertEquals(DUMMY_ATTRIBUTES_MAP, storedObject.getAttributes(), "Unexpected attributes");
    }

    @Test
    public void entryToObject_invalidJson_throwsStoreException()
    {
        TupleOutput out = new TupleOutput();
        out.writeString(DUMMY_TYPE_STRING);
        out.writeString("{");

        TupleInput in = new TupleInput(out.getBufferBytes());

        assertThrows(StoreException.class,
                () -> new ConfiguredObjectBinding(_objectId).entryToObject(in),
                "Expected StoreException for invalid JSON");
    }

    @Test
    public void entryToObject_jsonArray_throwsStoreException()
    {
        TupleOutput out = new TupleOutput();
        out.writeString(DUMMY_TYPE_STRING);
        out.writeString("[]");

        TupleInput in = new TupleInput(out.getBufferBytes());

        assertThrows(StoreException.class,
                () -> new ConfiguredObjectBinding(_objectId).entryToObject(in),
                "Expected StoreException for non-object JSON");
    }

    @Test
    public void entryToObject_jsonString_throwsStoreException()
    {
        TupleOutput out = new TupleOutput();
        out.writeString(DUMMY_TYPE_STRING);
        out.writeString("\"x\"");

        TupleInput in = new TupleInput(out.getBufferBytes());

        assertThrows(StoreException.class,
                () -> new ConfiguredObjectBinding(_objectId).entryToObject(in),
                "Expected StoreException for non-object JSON");
    }

    @Test
    public void entryToObject_jsonNull_throwsNullPointerException()
    {
        // This documents current behaviour: "null" JSON yields a null map and fails in record construction.
        TupleOutput out = new TupleOutput();
        out.writeString(DUMMY_TYPE_STRING);
        out.writeString("null");

        TupleInput in = new TupleInput(out.getBufferBytes());

        assertThrows(NullPointerException.class,
                () -> new ConfiguredObjectBinding(_objectId).entryToObject(in),
                "Expected NullPointerException for null attributes JSON");
    }

    @Test
    public void objectToEntry_selfReferentialAttributes_throwsStoreException()
    {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("dummy", "attributes");
        attributes.put("self", attributes);

        ConfiguredObjectRecord record = new ConfiguredObjectRecordImpl(UUIDGenerator.generateRandomUUID(),
                DUMMY_TYPE_STRING,
                attributes);

        TupleOutput out = new TupleOutput();

        assertThrows(StoreException.class,
                () -> _configuredObjectBinding.objectToEntry(record, out),
                "Expected StoreException for self-referential attributes");
    }
}
