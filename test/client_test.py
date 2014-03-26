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
import requests

"""Tests for the Taba Client.
"""

import unittest

import mox

from taba import client

class TabaClientTestCase(mox.MoxTestBase):

  def testFlush(self):
    """Test _Flush()"""
    taba_client = client.TabaClient('c1', 1, 'http://localhost:8280/post')

    name, tab_type, value = 'name', 'type', 4

    body = '\n'.join([
        '1', '', 'c1', '', 'name', '["type", 1234567890, "4"]',
        '', '', ''])

    resp = requests.Response()
    resp.status_code = 200

    self.mox.StubOutWithMock(client.time, 'time')
    client.time.time().MultipleTimes().AndReturn(1234567890)

    self.mox.StubOutWithMock(taba_client.session, 'post')

    taba_client.session.post('http://localhost:8280/post', body).AndReturn(resp)

    self.mox.ReplayAll()

    taba_client.RecordEvent(name, tab_type, value)
    taba_client._Flush()

    self.mox.VerifyAll()

if __name__ == '__main__':
  unittest.main()
