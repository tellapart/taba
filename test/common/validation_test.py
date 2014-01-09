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

"""Tests for taba.common.validation
"""

import unittest

from taba.common import validation

class ValidationTestCase(unittest.TestCase):

  def testValid(self):
    """Baseline test that valid values are allowed as Names and Clients.
    """
    values = [
        'aValidValue01_./-']

    for value in values:
      valid_name = validation.IsNameValid(value)
      self.assertTrue(valid_name)

      valid_client = validation.IsClientValid(value)
      self.assertTrue(valid_client)

  def testInvalid(self):
    """Test that strings containing invalid characters are invalid.
    """
    values = [
        'a value with spaces',
        'invalid&char1',
        'invalid|char2',
        "invalid'char3"]

    for value in values:
      valid_name = validation.IsNameValid(value)
      self.assertFalse(valid_name)

      valid_client = validation.IsClientValid(value)
      self.assertFalse(valid_client)

  def testUnicodeValidation(self):
    """Test that strings containing unicode characters are invalid.
    """
    values = [
        u'aUnicodeString\u2248\u2248\u2248\u2248\u2606']

    for value in values:
      valid_name = validation.IsNameValid(value)
      self.assertFalse(valid_name)

      valid_client = validation.IsClientValid(value)
      self.assertFalse(valid_client)

  def testSanitize(self):
    """Test that Sanitize replaces invalid characters properly.
    """
    before_after = [
        ('a value with spaces', 'a_value_with_spaces'),
        ('invalid&char1', 'invalid_char1'),
        ('invalid|char2', 'invalid_char2'),
        ("invalid'char3", 'invalid_char3'),
        (u'aUnicodeStr\u2248\u2248\u2248\u2248\u2606', 'aUnicodeStr_____')]

    for value, expected in before_after:
      sanitized = validation.Sanitize(value)
      self.assertEqual(expected, sanitized)

if __name__ == '__main__':
  unittest.main()
