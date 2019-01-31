#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import tempfile
import unittest

from avro import datafile
from avro import io
from avro import schema


def get_unions_schema():
  test_dir = os.path.dirname(os.path.abspath(__file__))
  schema_json_path = os.path.join(test_dir, 'unions.avsc')
  with open(schema_json_path, 'r') as f:
    schema_json = f.read()
  return schema.Parse(schema_json)


UNIONS_SCHEMA = get_unions_schema()

class TestDataFileUnions(unittest.TestCase):
  def __default_java_datum(self):
      datum = {
          'intOrLong': 0,
          'longOrInt': 0,
          'intOrLongNullable': None,
          'longOrIntNullable': None,
          'longOrFloatOrDouble': 0,
          'floatOrDouble': 0.0,
          'boolOrIntOrLongOrFloatOrDouble': False,
          'boolOrIntOrLongOrFloatOrDoubleNullable': None,
          'booleanNullable': None,
          'nullableBoolean': None,

      }
      return datum

  parameters = [
      ('intOrLong', 0, 'intOrLongZero'),
      ('intOrLong', 1, 'intOrLongOne'),
      ('intOrLong', 0, 'intOrLongZeroLong'),
      ('intOrLong', 1, 'intOrLongOneLong'),
      ('longOrInt', 0, 'longOrIntZero'),
      ('longOrInt', 1, 'longOrIntOne'),
      ('longOrInt', 0, 'longOrIntZeroLong'),
      ('longOrInt', 1, 'longOrIntOneLong'),
      ('intOrLongNullable', None, 'intOrLongNullableNull'),
      ('intOrLongNullable', 0, 'intOrLongNullableZero'),
      ('intOrLongNullable', 0, 'intOrLongNullableZeroLong'),
      ('longOrFloatOrDouble', 0, 'longOrFloatOrDoubleZero'),
      ('longOrFloatOrDouble', 0.0, 'longOrFloatOrDoubleZeroFloat'),
      ('longOrFloatOrDouble', 0.0, 'longOrFloatOrDoubleZeroDouble'),
      ('floatOrDouble', 0.0, 'floatOrDoubleZero'),
      ('floatOrDouble', 1.0, 'floatOrDoubleOne'),
      ('floatOrDouble', 0.0, 'floatOrDoubleZeroDouble'),
      ('floatOrDouble', 1.0, 'floatOrDoubleOneDouble'),
      ('booleanNullable', None, 'booleanNullableNull'),
      ('booleanNullable', False, 'booleanNullableFalse'),
      ('booleanNullable', True, 'booleanNullableTrue'),
      ('nullableBoolean', None, 'nullableBooleanNull'),
      ('nullableBoolean', False, 'nullableBooleanFalse'),
      ('nullableBoolean', True, 'nullableBooleanTrue'),

  ]

  def test_read_from_java(self):
      for p in self.parameters:
          with self.subTest(p, params=p):
              self.__validate_file(p)

  def test_write_to_python(self):
      for p in self.parameters:
          with self.subTest(p, params=p):
              self.__generate_file(p)


  def __validate_file(self, p):
      param = p[0]
      if callable(p[1]):
          value = p[1]()
      else:
          value = p[1]
      file = p[2]
      datum_reader = io.DatumReader()
      java_datum = self.__default_java_datum()
      java_datum[param] = value
      with open("/tmp/java/unions/" + file + ".avro", 'rb') as reader:
        dfr = datafile.DataFileReader(reader, datum_reader)
        for datum in dfr:
          self.assertEqual(java_datum, datum)

  def __generate_file(self, p):
      param = p[0]
      if callable(p[1]):
          value = p[1]()
      else:
          value = p[1]
      file = p[2]
      datum_writer = io.DatumWriter()
      java_datum = self.__default_java_datum()
      java_datum[param] = value
      with open("/tmp/python/unions/" + file + ".avro", 'wb') as writer:
        df = datafile.DataFileWriter(writer, datum_writer, UNIONS_SCHEMA)
        df.append(java_datum)
        df.flush()


if __name__ == '__main__':
    raise Exception('Use run_tests.py')
