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

import logging

from taba.client.thread_engine import ThreadEngine
from taba.common import validation
from taba.handlers.tab_type import TabType

LOG = logging.getLogger(__name__)

# Global TabaClientEngine singleton.
_ENGINE = None

###########################################################
# Client / Engine Interface
###########################################################

def Initialize(
    client_id,
    event_post_url,
    flush_period=1,
    engine_class=ThreadEngine):
  """Initialize the singleton Taba Client.

  Args:
    client_id - The Client ID string for this Taba Client. This value should be
        unique in the Taba deployment, and should be durable between restarts
        of the same logical client.
    flush_period - Period in seconds at which to flush buffered events.
    event_post_url - URL to which to POST events.
    engine_class - TabaClientEngine class to use. Defaults to ThreadEngine,
        which uses threading for background flushing. Other options include the
        GeventEngine, which uses gevent greenlets instead of threads.
  """
  global _ENGINE
  if _ENGINE is not None:
    raise ValueError('TabaClientEngine is already initialized.')

  if not validation.IsClientValid(client_id):
    raise ValueError('Illegal Client ID "%s"' % client_id)

  _ENGINE = engine_class(client_id, event_post_url, flush_period)
  _ENGINE.Initialize()

def Stop():
  """Shut down and running Client Engine.
  """
  global _ENGINE
  if _ENGINE is not None:
    _ENGINE.Stop()

def GetStatus():
  """Retrieve a JSON encoded string of the Client Engine status.
  """
  global _ENGINE
  if _ENGINE is None:
    return '"Uninitialized"'
  else:
    return _ENGINE.Status()

###########################################################
# Event Recording Accessors
###########################################################

def RecordEvent(name, value, tab_type):
  """Raw accessor to record an Event to a Tab Name.

  Args:
    name - Tab Name to post the event to.
    value - Value being recorded.
    tab_type - Tab Type of the Tab Name.
  """
  global _ENGINE
  if _ENGINE is None:
    return

  if not validation.IsNameValid(name):
    LOG.error('Illegal Tab Name "%s"! Skipping.' % name)
    return

  _ENGINE.RecordEvent(name, tab_type, value)

def Counter(name, value=1):
  """Event recording wrapper for CounterGroup type Tabs.

  Args:
    name - Tab Name to post an Event to.
    value - Value being recorded.
  """
  RecordEvent(name, value, TabType.COUNTER_GROUP)

def Percentile(name, value=1):
  """Event recording wrapper for PercentileGroup type Tabs.

  Args:
    name - Tab Name to post an Event to.
    value - Value being recorded.
  """
  RecordEvent(name, value, TabType.PERCENTILE_GROUP)

def Gauge(name, value, expiration=None):
  """Event recording wrapper for Gauge and ExpiryGauge type Tabs.

  Args:
    name - Tab Name to post an Event to.
    value - Value being recorded.
    expiration - Optional epoch timestamp of when the value expires. If None,
        a Gauge type Tab will be recorded; otherwise an ExpiryGauge will be
        recorded.
  """
  if expiration is None:
    RecordEvent(name, value, TabType.GAUGE)
  else:
    RecordEvent(name, (value, expiration), TabType.EXPIRY_GAUGE)

def Buffer(name, value):
  """Event recording wrapper for Buffer type Tabs.

  Args:
    name - Tab Name to post an Event to.
    value - Value being recorded.
  """
  RecordEvent(name, value, TabType.BUFFER)

class Tab(object):
  """Proxy object for posting events to a specific Tab Name"""

  def __init__(self, name, tab_type=TabType.COUNTER_GROUP):
    """
    Args:
      name - Tab Name string to post events to.
      tab_type - Type of Tab to record events to.
    """
    self.name = name
    self.type = tab_type

  def Record(self, value):
    """Post an event to this Tab.

    Args:
      *args - List of arguments to pass to this Event.
    """
    RecordEvent(self.name, value, self.type)
