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

Taba Client class and singleton instance. Acts as an interface for recording
Taba Events from a client application to a Taba Agent.
"""

import time

import cjson

from tellapart.taba.taba_event import SerializeEvent
from tellapart.taba.taba_event import TabaEvent
from tellapart.taba.taba_type import TabaType
from tellapart.taba.util import misc_util
from tellapart.taba.util import thread_util

LOG = misc_util.MakeStreamLogger(__name__)

client = None

class TabaClient(object):
  """Class for maintaining and flushing a buffer of Taba Events. This buffer
  is designed to be as small and simple as possible, relying on the Taba Agent
  to handle sophisticated buffering and connections.
  """
  def __init__(self, client_id, flush_period, event_post_url, dummy_mode=False):
    """
    Args:
      client_id - The Client ID string for this Taba Client. This value should
          be unique within the Taba deployment, and should be durable between
          restarts of the same logical client.
    """
    self.flush_period = flush_period
    self.event_post_url = event_post_url
    self.dummy_mode = dummy_mode

    self.buffer = []
    self.client_id = client_id
    self.failures = 0

  def _Flush(self):
    """Serialize and flush the internal buffer of Taba Events to the Taba Agent.
    """
    num_events = len(self.buffer)
    if num_events == 0:
      return

    body_lines = []
    body_lines.append(self.client_id)
    body_lines.extend([cjson.encode(SerializeEvent(e)) for e in self.buffer])

    self.buffer = []

    if self.dummy_mode:
      return

    response = misc_util.GenericFetchFromUrlToString(
        self.event_post_url,
        post_data='\n'.join(body_lines) + '\n')

    if response.status_code != 200:
      LOG.error('Failed to flush %d Taba buffer to Agent' % num_events)
      self.failures += 1

  def Initialize(self):
    """Start periodically flushing the Taba Event buffer.
    """
    thread_util.ScheduleOperationWithPeriod(
        self.flush_period,
        self._Flush)

  def RecordEvent(self, name, type, value):
    """Create and buffer a Taba Event.

    Args:
      name - Taba Name to post the event to.
      type - Taba Type of the Taba Name.
      value - The value being recorded.
    """
    now = time.time()
    self.buffer.append(TabaEvent(name, type, value, now))

  def State(self):
    """Return a JSON encoded dictionary of the Taba Client state. The returned
    value has the fields:
        failures - The number of flush events that failed.
        buffer_size - The number of Taba Events currently in the buffer.
    """
    state = {
        'failures' : self.failures,
        'buffer_size' : len(self.buffer)}
    return cjson.encode(state)

class Taba(object):
  """Proxy object for posting events to a specific Taba Name (Tab)"""

  def __init__(self, name, type=TabaType.COUNTER_GROUP):
    """
    Args:
      name - Taba Name string to post Events to.
      type - TabaType of this Tab to record Events to.
    """
    self.name = name
    self.type = type

  def RecordValue(self, *args):
    """Post an Event to this Tab.

    Args:
      *args - List of arguments to pass to this event.
    """
    RecordValue(self.name, args, self.type)

#----------------------------------------------------------
# Proxy functions to the global Taba Client
#----------------------------------------------------------

def RecordValue(name, value_tuple=(1,), type=TabaType.COUNTER_GROUP):
  """Record an Event to a Taba Name.

  Args:
    name - The Taba Name to post the Event to.
    value_tuple - The tuple of values being recorded.
    type - TabaType of the Tab.
  """
  global client
  if not client:
    return
  else:
    client.RecordEvent(name, type, value_tuple)

def Initialize(client_id, event_post_url, flush_period=1, dummy_mode=False):
  """Initialize the singleton Taba Client.

  Args:
    client_id - The Client ID string for this Taba Client. This value should be
        unique in the Taba deployment, and should be durable between restarts
        of the same logical client.
  """
  global client
  client = TabaClient(client_id, flush_period, event_post_url, dummy_mode)
  client.Initialize()

def Flush():
  """Force the Taba Client to flush it's buffer to the Agent.
  """
  global client
  if not client:
    return
  else:
    client._Flush()

def GetStatus():
  """Return a JSON encoded dictionary of the Taba Client state. The returned
  value has the fields:
      failures - The number of flush events that failed
      buffer_size - The number of Taba Events currently in the buffer
  """
  global client
  if not client:
    status = "Uninitialized"
  else:
    status = client.State()

  return status
