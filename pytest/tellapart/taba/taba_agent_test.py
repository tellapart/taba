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

from cStringIO import StringIO
import zlib

import mox

from tellapart.taba.util import misc_util
from tellapart.taba.agent import taba_agent
from tellapart.taba.agent import taba_agent_handlers

class TabaAgentTestCase(mox.MoxTestBase):

  def testBufferNormal(self):
    """Test Buffer() under normal conditions"""
    agent = taba_agent.TabaAgent()
    agent.max_buffer_size = 10
    agent.max_request_events = 100
    agent.max_pending_reqs = 1
    agent.server_requests_pending = [0]
    agent.server_event_url = ''
    agent.dummy_mode = False

    events_1 = ["mock_event_1", "mock_event_2", "mock_event_3"]
    events_2 = ["mock_event_5", "mock_event_6"]

    agent.Buffer("client_1", events_1)
    agent.Buffer("client_2", events_2)

    self.assertEqual(agent._CurrentBufferSize(), 5)
    self.assertEqual(
        agent.buffer,
        {"client_1": events_1, "client_2": events_2})

  def testBufferAlmostFull(self):
    """Test Buffer() when it's almost and can't store an entire request"""
    agent = taba_agent.TabaAgent()
    agent.max_buffer_size = 4
    agent.max_request_events = 100
    agent.max_pending_reqs = 1
    agent.server_requests_pending = [0]
    agent.server_event_url = ''
    agent.dummy_mode = False

    events_1 = ["mock_event_1", "mock_event_2", "mock_event_3"]
    events_2 = ["mock_event_5", "mock_event_6"]

    agent.Buffer("client_1", events_1)
    agent.Buffer("client_2", events_2)

    self.assertEqual(agent._CurrentBufferSize(), 4)
    self.assertEqual(
        agent.buffer,
        {"client_1": events_1, "client_2": ["mock_event_5"]})

  def testBufferFull(self):
    """Test Buffer() when it's completely full"""
    agent = taba_agent.TabaAgent()
    agent.max_buffer_size = 3
    agent.max_request_events = 100
    agent.max_pending_reqs = 1
    agent.server_requests_pending = [0]
    agent.server_event_url = ''
    agent.dummy_mode = False

    events_1 = ["mock_event_1", "mock_event_2", "mock_event_3"]
    events_2 = ["mock_event_5", "mock_event_6"]

    agent.Buffer("client_1", events_1)
    agent.Buffer("client_2", events_2)

    self.assertEqual(agent._CurrentBufferSize(), 3)
    self.assertEqual(agent.buffer, {"client_1": events_1})

  def testFlushNormal(self):
    """Test Flush() under normal conditions"""
    agent = taba_agent.TabaAgent()
    agent.max_buffer_size = 10
    agent.max_request_events = 100
    agent.max_pending_reqs = 1
    agent.server_requests_pending = [0]
    agent.server_event_urls = ['']
    agent.dummy_mode = False
    agent.request_workers.poll_timeout_secs = 2
    agent.request_workers.Start()

    events_1 = ['e1', 'e2']
    events_2 = ['e3']

    self.mox.StubOutWithMock(
        taba_agent.misc_util, 'GenericFetchFromUrlToString')

    body1 = '\n'.join(['c2', 'e3']) + '\n'
    request1 = zlib.compress(body1)
    response1 = misc_util.Bunch(status_code=200)

    body2 = '\n'.join(['c1', 'e1', 'e2']) + '\n'
    request2 = zlib.compress(body2)
    response2 = misc_util.Bunch(status_code=200)

    taba_agent.misc_util.GenericFetchFromUrlToString(
        '', post_data=request1) \
        .AndReturn(response1)

    taba_agent.misc_util.GenericFetchFromUrlToString(
        '', post_data=request2) \
        .AndReturn(response2)

    self.mox.ReplayAll()

    agent.Buffer('c1', events_1)
    agent.Buffer('c2', events_2)
    self.assertEqual(agent._CurrentBufferSize(), 3)

    agent.Flush()
    agent.request_workers.Finish()
    agent.request_workers.Wait()
    self.assertEqual(agent._CurrentBufferSize(), 0)

    self.mox.VerifyAll()

  def testFlushOversized(self):
    """Test Flush() when the buffer has too many events per request"""
    agent = taba_agent.TabaAgent()
    agent.max_buffer_size = 10
    agent.max_request_events = 2
    agent.max_pending_reqs = 1
    agent.server_requests_pending = [0]
    agent.server_event_urls = ['']
    agent.dummy_mode = False
    agent.request_workers.poll_timeout_secs = 2
    agent.request_workers.Start()

    events_1 = ['e1', 'e2', 'e3']

    self.mox.StubOutWithMock(
        taba_agent.misc_util, 'GenericFetchFromUrlToString')

    body1 = '\n'.join(['c1', 'e1', 'e2']) + '\n'
    request1 = zlib.compress(body1)
    response1 = misc_util.Bunch(status_code=200)

    body2 = '\n'.join(['c1', 'e3']) + '\n'
    request2 = zlib.compress(body2)
    response2 = misc_util.Bunch(status_code=200)

    taba_agent.misc_util.GenericFetchFromUrlToString(
        '', post_data=request1) \
        .AndReturn(response1)

    taba_agent.misc_util.GenericFetchFromUrlToString(
        '', post_data=request2) \
        .AndReturn(response2)

    self.mox.ReplayAll()

    agent.Buffer('c1', events_1)
    self.assertEqual(agent._CurrentBufferSize(), 3)

    agent.Flush()
    agent.request_workers.Finish()
    agent.request_workers.Wait()
    self.assertEqual(agent._CurrentBufferSize(), 0)

    self.mox.VerifyAll()

  def testFlushFail(self):
    """Test Flush() when the request fails"""
    agent = taba_agent.TabaAgent()
    agent.max_buffer_size = 10
    agent.max_request_events = 100
    agent.max_pending_reqs = 1
    agent.server_requests_pending = [0]
    agent.server_event_urls = ['']
    agent.dummy_mode = False
    agent.request_workers.poll_timeout_secs = 2
    agent.request_workers.Start()

    events_1 = ['e1', 'e2']

    self.mox.StubOutWithMock(
        taba_agent.misc_util, 'GenericFetchFromUrlToString')

    body1 = '\n'.join(['c1', 'e1', 'e2']) + '\n'
    request1 = zlib.compress(body1)
    response1 = misc_util.Bunch(status_code=400)
    response2 = misc_util.Bunch(status_code=200)

    taba_agent.misc_util.GenericFetchFromUrlToString(
        '', post_data=request1) \
        .AndReturn(response1)

    taba_agent.misc_util.GenericFetchFromUrlToString(
        '', post_data=request1) \
        .AndReturn(response2)

    self.mox.ReplayAll()

    agent.Buffer('c1', events_1)
    self.assertEqual(agent._CurrentBufferSize(), 2)

    agent.Flush()
    agent.request_workers.Finish()
    agent.request_workers.Wait()
    self.assertEqual(agent._CurrentBufferSize(), 0)

    self.mox.VerifyAll()

  def testHandlePostNormal(self):
    """Test HandlePost under normal conditions"""
    body = '\n'.join(['c1', 'e1', 'e2']) + '\n'
    request = misc_util.Bunch(raw={
        'request_body_bytes': StringIO(body)})

    self.mox.StubOutWithMock(taba_agent_handlers.global_taba_agent, 'Buffer')
    taba_agent_handlers.global_taba_agent.Buffer('c1', ['e1', 'e2'])

    self.mox.ReplayAll()
    taba_agent_handlers.HandlePost(request)
    self.mox.VerifyAll()

  def testHandlePostNoEvents(self):
    """Test HandlePost when no events are posted"""
    body = '\n'.join(['c1', '']) + '\n'
    request = misc_util.Bunch(raw={
        'request_body_bytes': StringIO(body)})

    self.mox.StubOutWithMock(taba_agent_handlers, 'juno')
    taba_agent_handlers.juno.status(400)
    taba_agent_handlers.juno.append(mox.IgnoreArg())

    self.mox.ReplayAll()
    taba_agent_handlers.HandlePost(request)
    self.mox.VerifyAll()

  def testHandlePostInvalid(self):
    """Test HandlePost with a garbage request"""
    body = 'asdfasef23waeqyeagerwv34rasef23rasdfg'
    request = misc_util.Bunch(raw={
        'request_body_bytes': StringIO(body)})

    self.mox.StubOutWithMock(taba_agent_handlers, 'juno')
    taba_agent_handlers.juno.status(400)
    taba_agent_handlers.juno.append(mox.IgnoreArg())

    self.mox.ReplayAll()
    taba_agent_handlers.HandlePost(request)
    self.mox.VerifyAll()
