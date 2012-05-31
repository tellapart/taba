"""
Copyright 2012 TellApart, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

------------------------------------------------------------

Helper class to load and run correct test suites.
"""

import optparse
import unittest2 as unittest

_AVAILABLE_TEST_TYPES = [ 'small', 'medium', 'big' ]

class TestSuiteManager(object):
  """This class allows us to run python tests based on the test type user wants to run.
  """
  def __init__(self, test_cases=None, child_test_suites=None):
    """Construct a new test suite manager which has the given test cases and child
      modules loaded.

    Args:
      test_cases - A dict containing an array of test cases mapped to their
          corresponding test types.
      child_test_suites - A list of child modules with TestSuiteManager, hence
          allowing us to recursively load the test suites.
    """
    self._test_cases = test_cases
    self._child_test_cases = child_test_suites
    self._options = None
    self._args = None
    self._loadTestsFromTestCase = unittest.TestLoader().loadTestsFromTestCase
    self._GetCommandLineOptions()

  def GetTestSuite(self, test_type=None):
    """This gets all the test cases that need to be run for the given test suite type.

    Args:
      test_type - Array of type of tests that we want to run.

    Returns:
      A TestSuite object which contains all the test cases that should be run for
      the given test type.
    """
    if test_type is None:
      test_type = self.GetTestType()
    test_suite = unittest.TestSuite()
    if self._test_cases:
      for suite_type, test_cases in self._test_cases.iteritems():
        if suite_type in test_type:
          test_suite.addTests(unittest.TestSuite([self._loadTestsFromTestCase(tc)
                              for tc in test_cases]))

    if self._child_test_cases:
      for child in self._child_test_cases:
        test_suite.addTests(child.TEST_SUITE_MANAGER.GetTestSuite(test_type))

    return test_suite

  def _GetCommandLineOptions(self):
    """Parse and store command-line options.
    """
    parser = optparse.OptionParser()

    parser.add_option('--test_suite', action='store', dest='test_suite',
        help=('The test suite that needs to be run (one of: %s)' % ', '.join(
          _AVAILABLE_TEST_TYPES + ['all'])))

    self._options, self._args = parser.parse_args()

  def GetTestType(self):
    """Get the types of tests we need to run.

    Returns:
      An array of the test types that are to be run based on the user input.
    """
    default_type = [ 'small' ]
    if not self._options.test_suite:
      return default_type
    if self._options.test_suite == 'all':
      return _AVAILABLE_TEST_TYPES
    if self._options.test_suite in _AVAILABLE_TEST_TYPES:
      return [self._options.test_suite]

    raise ValueError('Illegal test type: %s' % self._options.test_suite)

  def RunTests(self, test_type=None):
    """Runs the complete test suite.

    This runs the entire test suite including the children test suites.

    Args:
      test_type - The type of tests we want to run.
    """
    test_suite = self.GetTestSuite(test_type)
    unittest.TextTestRunner().run(test_suite)

