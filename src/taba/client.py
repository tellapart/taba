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

"""Taba Client class and singleton instance. Acts as an interface for recording
Events from a client application to a Taba Agent.
"""

from collections import defaultdict
import logging
import time
import traceback

import cjson
import requests

from taba.common import transport
from taba.common import validation
from taba.handlers.tab_type import TabType
from taba.util import thread_util

LOG = logging.getLogger(__name__)

_CLIENT = None

class TabaClient(object):
  """Class for maintaining and flushing a buffer of Events. This buffer is
  designed to be as small and simple as possible, relying on the Taba Agent to
  handle sophisticated buffering and connections.
  """

  def __init__(self, client_id, flush_period, event_post_url, dummy_mode=False):
    """
    Args:
      client_id - The Client ID string for this Client. This value should be
          unique within the Taba deployment, and should be durable between
          restarts of the same logical client.
    """
    self.flush_period = flush_period
    self.event_post_url = event_post_url
    self.dummy_mode = dummy_mode

    self.buffer = defaultdict(list)
    self.buffer_size = 0
    self.client_id = client_id
    self.failures = 0

    self.session = requests.session()

  def _Flush(self):
    """Serialize and flush the internal buffer of Events to the Taba Agent.
    """
    try:
      num_events = self.buffer_size
      if num_events == 0:
        return

      flush_buffer = self.buffer
      self.buffer = defaultdict(list)
      self.buffer_size = 0

      body = transport.Encode(self.client_id, flush_buffer)

      if self.dummy_mode:
        return

      response = self.session.post(self.event_post_url, body)

      if response.status_code != 200:
        LOG.error('Failed to flush %d Taba buffer to Agent' % num_events)
        self.failures += 1

    except Exception:
      LOG.error('Exception flushing Taba Client buffer.')
      LOG.error(traceback.format_exc())

  def Initialize(self):
    """Start periodically flushing the Event buffer.
    """
    thread_util.ScheduleOperationWithPeriod(
        self.flush_period,
        self._Flush)

  def RecordEvent(self, name, type, value):
    """Create and buffer an Event.

    Args:
      name - Tab Name to post the event to.
      type - Tab Type of the Tab Name.
      value - The value being recorded.
    """
    self.buffer[name].append(transport.MakeEventTuple(type, time.time(), value))
    self.buffer_size += 1

  def State(self):
    """Return a JSON encoded dictionary of the Taba Client state. The returned
    value has the fields:
        failures - The number of flush events that failed.
        buffer_size - The number of Events currently in the buffer.
    """
    state = {
        'failures' : self.failures,
        'buffer_size' : len(self.buffer_size)}
    return cjson.encode(state)

class Tab(object):
  """Proxy object for posting events to a specific Tab Name"""

  def __init__(self, name, type=TabType.COUNTER_GROUP):
    """
    Args:
      name - Tab Name string to post events to.
      type - Type of Tab to record events to.
    """
    self.name = name
    self.type = type

  def RecordValue(self, value):
    """Post an event to this Tab.

    Args:
      *args - List of arguments to pass to this Event.
    """
    RecordValue(self.name, value, self.type)

#----------------------------------------------------------
# Proxy functions to the global Taba Client
#----------------------------------------------------------

def RecordValue(name, value=1, type=TabType.COUNTER_GROUP):
  """Record an Event to a Tab Name.

  Args:
    name - The Tab Name to post the event to.
    value - The value being recorded.
    type - Tab Type of the Tab Name.
  """
  global _CLIENT
  if not _CLIENT:
    return
  else:

    if not validation.IsNameValid(name):
      LOG.error('Illegal Tab Name "%s"! Skipping.' % name)
      return

    _CLIENT.RecordEvent(name, type, value)

def Initialize(client_id, event_post_url, flush_period=1, dummy_mode=False):
  """Initialize the singleton Taba Client.

  Args:
    client_id - The Client ID string for this Taba Client. This value should be
        unique in the Taba deployment, and should be durable between restarts
        of the same logical client.
    flush_period - Period in seconds at which to flush buffered events.
    event_post_url - URL to which to POST events.
    dummy_mode - If true all Tabs are disabled.
  """
  global _CLIENT

  if not validation.IsClientValid(client_id):
    raise ValueError('Illegal Client ID "%s"' % client_id)

  _CLIENT = TabaClient(client_id, flush_period, event_post_url, dummy_mode)
  _CLIENT.Initialize()

def Flush():
  """Force the Taba Client to flush it's buffer to the Agent.
  """
  global _CLIENT
  if not _CLIENT:
    return
  else:
    _CLIENT._Flush()

def GetStatus():
  """Return a JSON encoded dictionary of the Taba Client state. The returned
  value has the fields:
      failures - The number of flush events that failed,
      buffer_size - The number of Events currently in the buffer.
  """
  global _CLIENT
  if not _CLIENT:
    status = "Uninitialized"
  else:
    status = _CLIENT.State()

  return status
