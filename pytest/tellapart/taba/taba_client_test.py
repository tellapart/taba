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

Tests for the Taba Agent.
"""

import mox

from tellapart.taba.util import misc_util
from tellapart.taba import taba_client
from tellapart.taba import taba_event

class TabaClientTestCase(mox.MoxTestBase):

  def testSerDe(self):
    """Test SerializeEvent() and DeserializeEvent()"""
    event = taba_client.TabaEvent('name', 'type', 4, 1234567890)
    ser = taba_client.SerializeEvent(event)

    self.assertTrue(ser, ('name', 'type', 4, 1234567890))

    dser = taba_event.DeserializeEvent(ser)

    self.assertEqual(dser.name, event.name)
    self.assertEqual(dser.type, event.type)
    self.assertEqual(dser.value, event.value)
    self.assertEqual(dser.timestamp, event.timestamp)

  def testFlush(self):
    """Test _Flush()"""
    client = taba_client.TabaClient('c1', 1, 'http://localhost:8280/post')

    taba = misc_util.Bunch(name='taba_name', type='taba_type')
    value = 4

    body = '\n'.join(['c1', '["taba_name", "taba_type", "4", 1234567890]', ''])

    self.mox.StubOutWithMock(taba_client.time, 'time')
    taba_client.time.time().AndReturn(1234567890)

    self.mox.StubOutWithMock(
        taba_client.misc_util,
        'GenericFetchFromUrlToString')

    taba_client.misc_util.GenericFetchFromUrlToString(
        'http://localhost:8280/post',
        post_data=body)\
        .AndReturn(misc_util.Bunch(status_code=200))

    self.mox.ReplayAll()

    client.RecordEvent(taba.name, taba.type, value)
    client._Flush()

    self.mox.VerifyAll()
