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

Defines a test suite encompassing all test cases.

To run all tests, invoke like this:
  python -m 'tellapart.all_tests'
"""

from gevent import monkey
monkey.patch_all()

from tellapart.testutil import test_suite_manager

import tellapart.storage.all_tests as storage_tests
import tellapart.taba.all_tests as taba_tests

_ALL_TEST_CASES = {
}

_CHILD_TEST_SUITES = [
  storage_tests,
  taba_tests,
]

TEST_SUITE_MANAGER = test_suite_manager.TestSuiteManager(_ALL_TEST_CASES,
    _CHILD_TEST_SUITES)

def main():
  TEST_SUITE_MANAGER.RunTests()

if __name__ == '__main__':
  main()

