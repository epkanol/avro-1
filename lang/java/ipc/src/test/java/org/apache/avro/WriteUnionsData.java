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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.JUnitSoftAssertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class WriteUnionsData {

    private static final Integer INT_ZERO = Integer.valueOf(0);
    private static final Integer INT_ONE = Integer.valueOf(1);
    private static final Long LONG_ZERO = Long.valueOf(0);
    private static final Long LONG_ONE = Long.valueOf(1);
    private static final Float FLOAT_ZERO = Float.valueOf(0);
    private static final Float FLOAT_ONE = Float.valueOf(1);
    private static final Double DOUBLE_ZERO = Double.valueOf(0);
    private static final Double DOUBLE_ONE = Double.valueOf(1);
    private static File WRITE_DIR = new File("/tmp/java/unions");
    private static File READ_DIR = new File("/tmp/python/unions");
    private static File FASTAVRO_DIR = new File("/tmp/fastavro/unions");
    
    @Rule
    public JUnitSoftAssertions softly = new JUnitSoftAssertions();
    
    static {
        WRITE_DIR.mkdirs();
        READ_DIR.mkdirs();
        FASTAVRO_DIR.mkdirs();
    }
    
    static class TestData<T> {
        String file;
        Consumer<Unions> modifier;
        Function<Unions, Object> extractor;
        TestData(String file, Consumer<Unions> modifier, Function<Unions, Object> extractor) { 
            this.file = file; 
            this.modifier = modifier; 
            this.extractor = extractor;
        }
        @Override
        public String toString() {
            return file;
        }
    }
    
    static class UnionConsumer implements Consumer<Unions>{
        Object value;
        private BiConsumer<Unions, Object> function;
        UnionConsumer(Object val, BiConsumer<Unions, Object> fcn) { value = val; function = fcn;}
        @Override
        public void accept(Unions obj) {
            function.accept(obj, value);
        }
    }

    static class BooleanConsumer implements Consumer<Unions>{
        Boolean value;
        private BiConsumer<Unions, Boolean> function;
        BooleanConsumer(Boolean val, BiConsumer<Unions, Boolean> fcn) { value = val; function = fcn;}
        @Override
        public void accept(Unions obj) {
            function.accept(obj, value);
        }
    }
    
    static class MySupplier implements Function<Unions, Object> {
        private Function<Unions, Object> function;
        MySupplier(Class<Unions> v,Function<Unions, Object> fcn) {
            this.function = fcn;
        }
        @Override
        public Object apply(Unions value) {
            return function.apply(value);
        }
        
    }
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Parameters(name = "{0}")
    public static Collection<TestData> data() {
        return Arrays.asList(
            new TestData("intOrLongZero", new UnionConsumer(INT_ZERO, (o,v) -> o.setIntOrLong(v)), new MySupplier(Unions.class, u -> u.getIntOrLong())),
            new TestData("intOrLongOne", new UnionConsumer(INT_ONE, (o,v) -> o.setIntOrLong(v)), new MySupplier(Unions.class, u -> u.getIntOrLong())),
            new TestData("intOrLongZeroLong", new UnionConsumer(LONG_ZERO, (o,v) -> o.setIntOrLong(v)), new MySupplier(Unions.class, u -> u.getIntOrLong())),
            new TestData("intOrLongOneLong", new UnionConsumer(LONG_ONE, (o,v) -> o.setIntOrLong(v)), new MySupplier(Unions.class, u -> u.getIntOrLong())),
            new TestData("longOrIntZero", new UnionConsumer(INT_ZERO, (o,v) -> o.setLongOrInt(v)), new MySupplier(Unions.class, u -> u.getLongOrInt())),
            new TestData("longOrIntOne", new UnionConsumer(INT_ONE, (o,v) -> o.setLongOrInt(v)), new MySupplier(Unions.class, u -> u.getLongOrInt())),
            new TestData("longOrIntZeroLong", new UnionConsumer(LONG_ZERO, (o,v) -> o.setLongOrInt(v)), new MySupplier(Unions.class, u -> u.getLongOrInt())),
            new TestData("longOrIntOneLong", new UnionConsumer(LONG_ONE, (o,v) -> o.setLongOrInt(v)), new MySupplier(Unions.class, u -> u.getLongOrInt())),
            new TestData("intOrLongNullableNull", new UnionConsumer(null, (o,v) -> o.setIntOrLongNullable(v)), new MySupplier(Unions.class, u -> u.getIntOrLongNullable())),
            new TestData("intOrLongNullableZero", new UnionConsumer(INT_ZERO, (o,v) -> o.setIntOrLongNullable(v)), new MySupplier(Unions.class, u -> u.getIntOrLongNullable())),
            new TestData("intOrLongNullableZeroLong", new UnionConsumer(LONG_ZERO, (o,v) -> o.setIntOrLongNullable(v)), new MySupplier(Unions.class, u -> u.getIntOrLongNullable())),
            new TestData("longOrFloatOrDoubleZero", new UnionConsumer(LONG_ZERO, (o,v) -> o.setLongOrFloatOrDouble(v)), new MySupplier(Unions.class, u -> u.getLongOrFloatOrDouble())),
            new TestData("longOrFloatOrDoubleZeroFloat", new UnionConsumer(FLOAT_ZERO, (o,v) -> o.setLongOrFloatOrDouble(v)), new MySupplier(Unions.class, u -> u.getLongOrFloatOrDouble())),
            new TestData("longOrFloatOrDoubleZeroDouble", new UnionConsumer(DOUBLE_ZERO, (o,v) -> o.setLongOrFloatOrDouble(v)), new MySupplier(Unions.class, u -> u.getLongOrFloatOrDouble())),

            new TestData("floatOrDoubleZero", new UnionConsumer(FLOAT_ZERO, (o,v) -> o.setFloatOrDouble(v)), new MySupplier(Unions.class, u -> u.getFloatOrDouble())),
            new TestData("floatOrDoubleOne", new UnionConsumer(FLOAT_ONE, (o,v) -> o.setFloatOrDouble(v)), new MySupplier(Unions.class, u -> u.getFloatOrDouble())),
            new TestData("floatOrDoubleZeroDouble", new UnionConsumer(DOUBLE_ZERO, (o,v) -> o.setFloatOrDouble(v)), new MySupplier(Unions.class, u -> u.getFloatOrDouble())),
            new TestData("floatOrDoubleOneDouble", new UnionConsumer(DOUBLE_ONE, (o,v) -> o.setFloatOrDouble(v)), new MySupplier(Unions.class, u -> u.getFloatOrDouble())),
            new TestData("booleanNullableFalse", new BooleanConsumer(Boolean.FALSE, (o,v) -> o.setBooleanNullable(v)), new MySupplier(Unions.class, u -> u.getBooleanNullable())),
            new TestData("booleanNullableTrue", new BooleanConsumer(Boolean.TRUE, (o,v) -> o.setBooleanNullable(v)), new MySupplier(Unions.class, u -> u.getBooleanNullable())),
            new TestData("booleanNullableNull", new BooleanConsumer(null, (o,v) -> o.setBooleanNullable(v)), new MySupplier(Unions.class, u -> u.getBooleanNullable())),
            new TestData("nullableBooleanFalse", new BooleanConsumer(Boolean.FALSE, (o,v) -> o.setNullableBoolean(v)), new MySupplier(Unions.class, u -> u.getNullableBoolean())),
            new TestData("nullableBooleanTrue", new BooleanConsumer(Boolean.TRUE, (o,v) -> o.setNullableBoolean(v)), new MySupplier(Unions.class, u -> u.getNullableBoolean())),
            new TestData("nullableBooleanNull", new BooleanConsumer(null, (o,v) -> o.setNullableBoolean(v)), new MySupplier(Unions.class, u -> u.getNullableBoolean()))
                );
    };

    private void write(String filename, Unions... obj) throws IOException 
    {
        File file = new File(WRITE_DIR, String.format("%s.avro", filename));
        
        DataFileWriter<Unions> writer = null;
        try {
             writer = new DataFileWriter<Unions>(
            // GenericDatumWriter does not work, gets error with enums, due to AVRO-997
                     new SpecificDatumWriter<Unions>(Unions.SCHEMA$))
                     
                     .create(Unions.SCHEMA$, file);
            for (Unions o: obj){
                writer.append(o);
            }
        } finally {
            if (writer != null) {
                writer.close();      
            }
        }
    }    
    
    private Collection<Unions> read(File dir, String filename) throws IOException {
        File file = new File(dir, String.format("%s.avro", filename));
        List<Unions> list = new LinkedList<>();
        
        DataFileReader<Unions> reader= null;
        try {
             reader= new DataFileReader<Unions>(file,
                     new SpecificDatumReader<Unions>());
             while(reader.hasNext()) {
                 Unions obj = reader.next();
                 list.add(obj);
             }
             return list;
        } finally {
            if (reader != null) {
                reader.close();      
            }
        }
    }

    
    private Unions newUnions() {
        Unions o = new Unions();
        o.setIntOrLong(INT_ZERO);
        o.setLongOrInt(Long.valueOf(0));
        o.setLongOrFloatOrDouble(Long.valueOf(0));
        o.setFloatOrDouble(Float.valueOf(0));
        o.setBoolOrIntOrLongOrFloatOrDouble(Boolean.FALSE);
        return o;
    }
    
    @Parameter
    public TestData data;
    
    @Test
    public void write() throws IOException {
        Unions obj = newUnions();
        data.modifier.accept(obj);
        write(data.file, obj);
    }
    
    @Test
    public void readFromPythonAvro() throws IOException {
        readFromDir(READ_DIR);
    }

    private void readFromDir(File dir) throws IOException {
        Unions expected = newUnions();
        data.modifier.accept(expected);
        Collection<Unions> read = read(dir, data.file);
        for(Unions obj : read) {
            Object expectedValue = data.extractor.apply(expected);
            Object foundValue = data.extractor.apply(obj);
            Assertions.assertThat(foundValue).as("hard failure").isEqualTo(expectedValue);
            softlyAssertUnions(expected, obj);
        }
    }

    @Test
    public void readFromFastAvro() throws IOException {
        readFromDir(FASTAVRO_DIR);
    }

    private void softlyAssertUnions(Unions expected, Unions actual) {
        softly.assertThat(actual.booleanNullable).isEqualTo(expected.booleanNullable);
        softly.assertThat(actual.boolOrIntOrLongOrFloatOrDouble).isEqualTo(expected.boolOrIntOrLongOrFloatOrDouble);
        softly.assertThat(actual.boolOrIntOrLongOrFloatOrDoubleNullable).isEqualTo(expected.boolOrIntOrLongOrFloatOrDoubleNullable);
        softly.assertThat(actual.floatOrDouble).isEqualTo(expected.floatOrDouble);
        softly.assertThat(actual.intOrLong).isEqualTo(expected.intOrLong);
        softly.assertThat(actual.intOrLongNullable).isEqualTo(expected.intOrLongNullable);
        softly.assertThat(actual.longOrFloatOrDouble).isEqualTo(expected.longOrFloatOrDouble);
        softly.assertThat(actual.longOrInt).isEqualTo(expected.longOrInt);
        softly.assertThat(actual.longOrIntNullable).isEqualTo(expected.longOrIntNullable);
        softly.assertThat(actual.nullableBoolean).isEqualTo(expected.nullableBoolean);
    }

}
