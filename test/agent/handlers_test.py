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

"""Tests for taba.agent.handlers
"""

import testing_bootstrap
testing_bootstrap.Bootstrap()

from collections import defaultdict

import mox
import webtest

from taba.agent import handlers
from taba.common import transport
from taba.third_party import bottle

class AgentHandlersTestCase(mox.MoxTestBase):
  """Test cases for bottle request handlers in taba.agent.handlers
  """

  def setUp(self):
    """Override."""
    super(AgentHandlersTestCase, self).setUp()
    self.app = webtest.TestApp(bottle.app())

  def tearDown(self):
    """Override."""
    super(AgentHandlersTestCase, self).tearDown()

  def testGetStatusText(self):
    """Test /status with default accept.
    """
    self.mox.StubOutWithMock(handlers, 'global_taba_agent')
    handlers._GLOBAL_TABA_AGENT.Status().AndReturn({
      'all_events': 100,
      'buffered_events': 10,
      'buffer_pct': 0.10,
      'queued_events': 20,
      'queued_requests': 3,
      'pending_events': 50,
      'pending_requests': 5,
      'url_stats': {
          'shards': 128,
          'total_events': 100,
          'buffered_events': 10,
          'queued_events': 20,
          'queued_requests': 30,
          'pending_events': 40,
          'pending_requests': 5,}, })

    self.mox.ReplayAll()

    self.app.get('/status')

  def testGetStatusJson(self):
    """Test /status with default application/json accept.
    """
    self.mox.StubOutWithMock(handlers, 'global_taba_agent')
    handlers._GLOBAL_TABA_AGENT.Status().AndReturn({
      'all_events': 100,
      'buffered_events': 10,
      'buffer_pct': 0.10,
      'queued_events': 20,
      'queued_requests': 3,
      'pending_events': 50,
      'pending_requests': 5,
      'url_stats': {
          'shards': 128,
          'total_events': 100,
          'buffered_events': 10,
          'queued_events': 20,
          'queued_requests': 30,
          'pending_events': 40,
          'pending_requests': 5,}, })

    self.mox.ReplayAll()

    response = self.app.get('/status', headers={'HTTP_ACCEPT': 'application/json'})
    response.json

  def testPost(self):
    """Test basic test for /post.
    """
    self.mox.StubOutWithMock(handlers, 'global_taba_agent')

    expected_events = defaultdict(list)
    expected_events['name1'] = ['["typeA", 1234567890, "12.34"]',]
    handlers._GLOBAL_TABA_AGENT.Buffer('client1', expected_events)

    postdata = transport.Encode(
        'client1',
        {'name1': [transport.MakeEventTuple('typeA', 1234567890, 12.34)]})

    self.mox.ReplayAll()

    self.app.post('/post', params=postdata)
