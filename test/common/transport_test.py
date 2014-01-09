# Copyright 2014 TellApart, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for taba.common.transport
"""

import testing_bootstrap
testing_bootstrap.Bootstrap()

from cStringIO import StringIO
import unittest

import mox

from taba.common import transport
class TransportTestCase(mox.MoxTestBase):
  """Test cases for the transport module.
  """

  #########################################################
  # Encode
  #########################################################

  def testEncodeEventTuple(self):
    """Test that event tuples are encoded correctly.
    """
    def _check_tuple(event_tuple, expected):
      encoded = transport.EncodeEventTuple(event_tuple)
      self.assertEqual(expected, encoded)

    _check_tuple(
        transport.MakeEventTuple('typeA', 1234567890, 321),
        '["typeA", 1234567890, "321"]')

    _check_tuple(
        transport.MakeEventTuple('typeA', 1234567890, "string_val"),
        '["typeA", 1234567890, "\\"string_val\\""]')

    _check_tuple(
        transport.MakeEventTuple('typeA', 1234567890, {"key": "val"}),
        '["typeA", 1234567890, "{\\"key\\": \\"val\\"}"]')

  def testEncodeMultiClient(self):
    """Test that multi-client bundles are encoded correctly.
    """
    data = {
        'client1': {
            'name1': [
                transport.MakeEventTuple('typeA', 1234567890, 321),
                transport.MakeEventTuple('typeA', 1234567890, 654),
                transport.MakeEventTuple('typeA', 1234567890, 987)],
            'name2': [
                transport.MakeEventTuple('typeB', 1234567890, "str1"),
                transport.MakeEventTuple('typeB', 1234567890, "str1"),
                transport.MakeEventTuple('typeB', 1234567890, "str2")]},
        'client2': {
            'name2': [
                transport.MakeEventTuple('typeB', 1234567890, "str3"),
                transport.MakeEventTuple('typeB', 1234567890, "str4")],
            'name3': [
                transport.MakeEventTuple('typeC', 1234567890, 123.45)]}}

    # Since the encoding relies on dicts, the actual ordering of the parts of
    # the serialized body aren't stable. So instead of comparing against an
    # expected serialization, we check that each independent section exists as
    # expected.
    preamble_bundle = '\n'.join([
        '1',
        ''])

    c1n1_bundle = '\n'.join([
        '',
        'name1',
        '["typeA", 1234567890, "321"]',
        '["typeA", 1234567890, "654"]',
        '["typeA", 1234567890, "987"]',
        ''])

    c1n2_bundle = '\n'.join([
        '',
        'name2',
        '["typeB", 1234567890, "\\"str1\\""]',
        '["typeB", 1234567890, "\\"str1\\""]',
        '["typeB", 1234567890, "\\"str2\\""]',
        ''])

    c2n2_bundle = '\n'.join([
        '',
        'name2',
        '["typeB", 1234567890, "\\"str3\\""]',
        '["typeB", 1234567890, "\\"str4\\""]',
        ''])

    c2n3_bundle = '\n'.join([
        '',
        'name3',
        '["typeC", 1234567890, "123.45"]',
        ''])

    actual = transport.EncodeMultiClient(data, encode_events=True)

    self.assertTrue(preamble_bundle in actual)
    self.assertTrue(c1n1_bundle in actual)
    self.assertTrue(c1n2_bundle in actual)
    self.assertTrue(c2n2_bundle in actual)
    self.assertTrue(c2n3_bundle in actual)

  def testEncodeMultiClientNoEncodeEvent(self):
    """Test that multi-client bundles are encoded correctly when the individual
    events aren't encoded/decoded.
    """
    data = {
        'client1': {
            'name1': [
                "encoded_event_1",
                "encoded_event_2",
                "encoded_event_3",],
            'name2': [
                "encoded_event_4",
                "encoded_event_5",],},
        'client2': {
            'name2': [
                "encoded_event_6",
                "encoded_event_7",
                "encoded_event_8",],
            'name3': [
                "encoded_event_9",]}}

    preamble_bundle = '\n'.join([
        '1',
        ''])

    c1n1_bundle = '\n'.join([
        '',
        'name1',
        'encoded_event_1',
        'encoded_event_2',
        'encoded_event_3',
        ''])

    c1n2_bundle = '\n'.join([
        '',
        'name2',
        'encoded_event_4',
        'encoded_event_5',
        ''])

    c2n2_bundle = '\n'.join([
        '',
        'name2',
        'encoded_event_6',
        'encoded_event_7',
        'encoded_event_8',
        ''])

    c2n3_bundle = '\n'.join([
        '',
        'name3',
        'encoded_event_9',
        ''])

    actual = transport.EncodeMultiClient(data, encode_events=False)

    self.assertTrue(preamble_bundle in actual)
    self.assertTrue(c1n1_bundle in actual)
    self.assertTrue(c1n2_bundle in actual)
    self.assertTrue(c2n2_bundle in actual)
    self.assertTrue(c2n3_bundle in actual)

  #########################################################
  # Decode
  #########################################################

  def testEventBundle(self):
    """Test functionality of the EventBundle class.
    """
    data = {
        'client1': {
            'name1': [
                transport.MakeEventTuple('typeA', 1234567890, 321),
                transport.MakeEventTuple('typeA', 1234567890, 654),
                transport.MakeEventTuple('typeA', 1234567890, 987)],
            'name2': [
                transport.MakeEventTuple('typeB', 1234567890, "str1"),
                transport.MakeEventTuple('typeB', 1234567890, "str1"),
                transport.MakeEventTuple('typeB', 1234567890, "str2")]},
        'client2': {
            'name2': [
                transport.MakeEventTuple('typeB', 1234567890, "str3"),
                transport.MakeEventTuple('typeB', 1234567890, "str4")],
            'name3': [
                transport.MakeEventTuple('typeC', 1234567890, 123.45)]}}

    serialized = transport.EncodeMultiClient(data)
    bundle_iter = transport.DecodeStreaming(StringIO(serialized))

    for bundle in bundle_iter:
      str(bundle)   # Test __repr__

      key = (bundle.client, bundle.name)
      if key == ('client1', 'name1'):
        self.assertEqual(bundle.tab_type, 'typeA')
        self.assertEqual(bundle.num_events, 3)

      elif key == ('client1', 'name2'):
        self.assertEqual(bundle.tab_type, 'typeB')
        self.assertEqual(bundle.num_events, 3)

      elif key == ('client2', 'name2'):
        self.assertEqual(bundle.tab_type, 'typeB')
        self.assertEqual(bundle.num_events, 2)

      elif key == ('client2', 'name3'):
        self.assertEqual(bundle.tab_type, 'typeC')
        self.assertEqual(bundle.num_events, 1)

      else:
        self.fail('Got unexpected bundle %s' % str(key))

  def testEventBundleLarge(self):
    """Test that the maximum number of events in an EventBundle is respected.
    """
    data = {'client1': {'name1': [
        transport.MakeEventTuple('typeA', 1234567890, 321)
        for _ in xrange(75000)]}}

    serialized = transport.EncodeMultiClient(data)
    bundle_iter = transport.DecodeStreaming(StringIO(serialized))

    bundle = bundle_iter.next()
    self.assertEqual(bundle.client, 'client1')
    self.assertEqual(bundle.name, 'name1')
    self.assertEqual(bundle.tab_type, 'typeA')
    self.assertEqual(bundle.num_events, 50000)

    bundle = bundle_iter.next()
    self.assertEqual(bundle.client, 'client1')
    self.assertEqual(bundle.name, 'name1')
    self.assertEqual(bundle.tab_type, 'typeA')
    self.assertEqual(bundle.num_events, 25000)

    self.assertRaises(StopIteration, bundle_iter.next)

  #########################################################
  # End-to-End
  #########################################################

  def testSerializeDeserialize(self):
    """End to end test of serialize/deserialize.
    """
    data = {
        'client1': {
            'name1': [
                transport.MakeEventTuple('typeA', 1234567890, 321),
                transport.MakeEventTuple('typeA', 1234567890, 654),
                transport.MakeEventTuple('typeA', 1234567890, 987)],
            'name2': [
                transport.MakeEventTuple('typeB', 1234567890, "str1"),
                transport.MakeEventTuple('typeB', 1234567890, "str1"),
                transport.MakeEventTuple('typeB', 1234567890, "str2")]},
        'client2': {
            'name2': [
                transport.MakeEventTuple('typeB', 1234567890, "str3"),
                transport.MakeEventTuple('typeB', 1234567890, "str4")],
            'name3': [
                transport.MakeEventTuple('typeC', 1234567890, 123.45)]}}

    serialized = transport.EncodeMultiClient(data, encode_events=True)
    deserialized = transport.Decode(StringIO(serialized), decode_events=True)

    self.assertEqual(data, deserialized)

  def testSerializeDeserializeNoDecodeEvents(self):
    """End to end test of serialize/deserialize when individual events aren't
    encoded/decoded.
    """
    data = {
        'client1': {
            'name1': [
                '["typeA", 1234567890, "321"]',
                '["typeA", 1234567890, "321"]',
                '["typeA", 1234567890, "321"]',
                ],
            'name2': [
                '["typeA", 1234567890, "321"]',
                '["typeA", 1234567890, "321"]',]},
        'client2': {
            'name2': [
                '["typeA", 1234567890, "321"]',
                '["typeA", 1234567890, "321"]',],
            'name3': [
                '["typeA", 1234567890, "321"]',]}}

    serialized = transport.EncodeMultiClient(data, encode_events=False)
    deserialized = transport.Decode(StringIO(serialized), decode_events=False)

    self.assertEqual(data, deserialized)

  def testSerializeDeserializeLargeBundle(self):
    """End to end test of serialize/deserialize for a very large bundle of
    events. Should trip both the events per block and events per bundle limits.
    """
    data = {
        'client1': {
            'name1': [
                transport.MakeEventTuple('typeA', 1234567890, 321)
                for _ in xrange(100000)],
            'name2': [
                transport.MakeEventTuple('typeB', 1234567890, "str1"),
                transport.MakeEventTuple('typeB', 1234567890, "str1"),
                transport.MakeEventTuple('typeB', 1234567890, "str2")]},
        'client2': {
            'name2': [
                transport.MakeEventTuple('typeB', 1234567890, "str3"),
                transport.MakeEventTuple('typeB', 1234567890, "str4")],
            'name3': [
                transport.MakeEventTuple('typeC', 1234567890, 123.45)]}}

    serialized = transport.EncodeMultiClient(data, encode_events=True)
    deserialized = transport.Decode(StringIO(serialized), decode_events=True)

    self.assertEqual(data, deserialized)

if __name__ == '__main__':
  unittest.main()
