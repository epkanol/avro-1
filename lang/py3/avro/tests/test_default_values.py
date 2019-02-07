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
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import binascii
import io
import logging
import sys
import unittest

from avro import io as avro_io
from avro import schema

SCHEMA_ABC = schema.Parse("""
{
  "type": "record",
  "name": "Test",
  "fields": [
      {"name": "a", "type": ["null", "string"], "default": "abc"}
  ]
}
""")

SCHEMA_DEF = schema.Parse("""
{
  "type": "record",
  "name": "Test",
  "fields": [
      {"name": "a", "type": ["null", "string"], "default": "def"}
  ]
}
""")


def avro_hexlify(reader):
  """Return the hex value, as a string, of a binary-encoded int or long."""
  bytes = []
  current_byte = reader.read(1)
  bytes.append(binascii.hexlify(current_byte).decode())
  while (ord(current_byte) & 0x80) != 0:
    current_byte = reader.read(1)
    bytes.append(binascii.hexlify(current_byte).decode())
  return ' '.join(bytes)


def write_datum(datum, writer_schema):
  writer = io.BytesIO()
  encoder = avro_io.BinaryEncoder(writer)
  datum_writer = avro_io.DatumWriter(writer_schema)
  datum_writer.write(datum, encoder)
  return writer, encoder, datum_writer


def read_datum(buffer, writer_schema, reader_schema=None):
  reader = io.BytesIO(buffer.getvalue())
  decoder = avro_io.BinaryDecoder(reader)
  datum_reader = avro_io.DatumReader(writer_schema, reader_schema)
  return datum_reader.read(decoder)


def check_binary_encoding(number_type):
  logging.debug('Testing binary encoding for type %s', number_type)
  correct = 0
  for datum, hex_encoding in BINARY_ENCODINGS:
    logging.debug('Datum: %d', datum)
    logging.debug('Correct Encoding: %s', hex_encoding)

    writer_schema = schema.Parse('"%s"' % number_type.lower())
    writer, encoder, datum_writer = write_datum(datum, writer_schema)
    writer.seek(0)
    hex_val = avro_hexlify(writer)

    logging.debug('Read Encoding: %s', hex_val)
    if hex_encoding == hex_val: correct += 1
  return correct


def check_skip_number(number_type):
  logging.debug('Testing skip number for %s', number_type)
  correct = 0
  for value_to_skip, hex_encoding in BINARY_ENCODINGS:
    VALUE_TO_READ = 6253
    logging.debug('Value to Skip: %d', value_to_skip)

    # write the value to skip and a known value
    writer_schema = schema.Parse('"%s"' % number_type.lower())
    writer, encoder, datum_writer = write_datum(value_to_skip, writer_schema)
    datum_writer.write(VALUE_TO_READ, encoder)

    # skip the value
    reader = io.BytesIO(writer.getvalue())
    decoder = avro_io.BinaryDecoder(reader)
    decoder.skip_long()

    # read data from string buffer
    datum_reader = avro_io.DatumReader(writer_schema)
    read_value = datum_reader.read(decoder)

    logging.debug('Read Value: %d', read_value)
    if read_value == VALUE_TO_READ: correct += 1
  return correct


# ------------------------------------------------------------------------------


class TestDefaultValueSerialization(unittest.TestCase):

  def testDefaultValue(self):
    writer_schema = SCHEMA_ABC
    reader_schema = SCHEMA_ABC
    datum_to_write = { }
    writer, encoder, datum_writer = write_datum(datum_to_write, writer_schema)
    datum_read = read_datum(writer, writer_schema, reader_schema)
    # it could be argued that it would be nice to be able to read out default values
    # from values if they are present in the schema
    #self.assertEqual("abc", datum_read["a"])
    self.assertIsNone(datum_read["a"])

  def testDefaultValueSerializeWithOtherDefault(self):
    writer_schema = SCHEMA_ABC
    reader_abc = SCHEMA_ABC
    reader_def = SCHEMA_DEF

    datum_to_write = { }
    writer, encoder, datum_writer = write_datum(datum_to_write, writer_schema)
    datum_read = read_datum(writer, writer_schema, reader_abc)
    writer_again, encoder_again, datum_writer_again = write_datum(datum_read, writer_schema)
    datum_read = read_datum(writer_again, writer_schema, reader_def)
    # read it with schema with new default value, should show the new default
    #self.assertEqual("def", datum_read["a"])
    self.assertIsNone(datum_read["a"])


if __name__ == '__main__':
  raise Exception('Use run_tests.py')
