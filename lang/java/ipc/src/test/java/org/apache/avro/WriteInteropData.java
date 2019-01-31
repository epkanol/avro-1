/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class WriteInteropData {

    private static String JAPANESE_HIRAGANA_LETTER_DO = "\u3069";
    private static String UNICODE_LOBSTER = "\uD83E\uDD9E";
    private static File WRITE_DIR = new File("/tmp/java");
    private static File READ_DIR = new File("/tmp/python");
    
    static {
        WRITE_DIR.mkdirs();
        READ_DIR.mkdirs();
    }
    
    static class TestData<T> {
        String file;
        Consumer<Interop> applier;
        TestData(String file, Consumer<Interop> fcn) { 
            this.file = file; 
            this.applier = fcn; 
        }
        @Override
        public String toString() {
            return file;
        }
    }
    
    static class StringConsumer implements Consumer<Interop>{
        String value;
        StringConsumer(String val) { value = val; }
        @Override
        public void accept(Interop obj) {
            obj.setStringField(value);
        }
    }

    static class UnionConsumer implements Consumer<Interop>{
        Object value;
        UnionConsumer(Object val) { value = val; }
        @Override
        public void accept(Interop obj) {
            obj.setUnionField(value);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Parameters(name = "{0}")
    public static Collection<TestData> data() {
        return Arrays.asList(
            new TestData("string", new StringConsumer("åke")),
            new TestData("japanese", new StringConsumer(String.format("%ske", JAPANESE_HIRAGANA_LETTER_DO))),
            new TestData("lobster", new StringConsumer(String.format("%ske", UNICODE_LOBSTER))),
            new TestData("ds", new StringConsumer("ddddd")),
            new TestData("map", new Consumer<Interop>() {

                @Override
                public void accept(Interop t) {
                    Map<String, Foo> map = new HashMap<>();
                    String res = "";
                    for (int i = 0; i < 5; i++) {
                        map.put("key" + i, new Foo(res));
                        res += "a";
                    }
                    t.setMapField(map);
                }
            }), 
            new TestData("unionAsBooleanTrue", new UnionConsumer(Boolean.TRUE)),
            new TestData("unionAsBooleanFalse", new UnionConsumer(Boolean.FALSE)),
            new TestData("unionAsDoubleZero", new UnionConsumer(Double.valueOf(0.0))),
            new TestData("unionAsDoubleOne", new UnionConsumer(Double.valueOf(1.0))),
            new TestData("unionAsBytes", new UnionConsumer(Arrays.asList(ByteBuffer.wrap(new byte[] {1,2,3}), ByteBuffer.wrap(new byte[] {4,5}), ByteBuffer.wrap(new byte[] {6}))))
                );
    };

    private void write(String filename, Interop... obj) throws IOException 
    {
        File file = new File(WRITE_DIR, String.format("%s.avro", filename));
        
        DataFileWriter<Interop> writer = null;
        try {
             writer = new DataFileWriter<Interop>(
            // GenericDatumWriter does not work, gets error with enums, due to AVRO-997
                     new SpecificDatumWriter<Interop>(Interop.SCHEMA$))
                     
                     .create(Interop.SCHEMA$, file);
            for (Interop o: obj){
                writer.append(o);
            }
        } finally {
            if (writer != null) {
                writer.close();      
            }
        }
    }    
    
    private Collection<Interop> read(String filename) throws IOException {
        File file = new File(READ_DIR, String.format("%s.avro", filename));
        List<Interop> list = new LinkedList<>();
        
        DataFileReader<Interop> reader= null;
        try {
             reader= new DataFileReader<Interop>(file,
                     new SpecificDatumReader<Interop>());
             while(reader.hasNext()) {
                 Interop obj = reader.next();
                 list.add(obj);
             }
             return list;
        } finally {
            if (reader != null) {
                reader.close();      
            }
        }
    }

    
    private Interop newInterop() {
        Interop o = new Interop();
        o.setStringField("");
        o.setBytesField(ByteBuffer.allocate(0));
        o.setArrayField(Collections.<Double>emptyList());
        o.setMapField(Collections.<String, Foo>emptyMap());
        o.setUnionField(Boolean.FALSE);
        o.setEnumField(Kind.A);
        o.setFixedField(new MD5(new byte[16]));
        o.setRecordField(new Node("root", Collections.<Node>emptyList()));
        return o;
    }
    
    @Parameter
    public TestData data;
    
    @Test
    public void write() throws IOException {
        Interop obj = newInterop();
        data.applier.accept(obj);
        write(data.file, obj);
    }
    
    @Test
    public void read() throws IOException {
        Interop expected = newInterop();
        data.applier.accept(expected);
        Collection<Interop> read = read(data.file);
        for(Interop obj : read) {
            assertInterop(expected, obj);
        }
    }

    private void assertInterop(Interop expected, Interop actual) {
        assertEquals(expected.arrayField, actual.arrayField);
        assertEquals(expected.boolField, actual.boolField);
        assertEquals(expected.boolField, actual.boolField);
        assertEquals(expected.bytesField, actual.bytesField);
        assertEquals(expected.doubleField, actual.doubleField, 1.0e-9);
        assertEquals(expected.enumField, actual.enumField);
        assertEquals(expected.fixedField, actual.fixedField);
        assertEquals(expected.floatField, actual.floatField, 1.0e-9);
        assertEquals(expected.intField, actual.intField);
        assertEquals(expected.longField, actual.longField);
        assertEquals(expected.mapField, actual.mapField);
        assertEquals(expected.nullField, actual.nullField);
        assertEquals(expected.recordField, actual.recordField);
        assertEquals(expected.stringField, actual.stringField);
        // python incorrectly encodes False as double 0.0 when both boolean and double is in union
        assertEquals(expected.unionField, actual.unionField);
        //assertEquals(expected, actual);
    }

}
