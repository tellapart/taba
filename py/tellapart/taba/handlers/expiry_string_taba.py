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

Taba Handler that accepts String inputs, and tracks the latest value set,
with an expiration timestamp.
"""

from collections import defaultdict
import time

from tellapart.taba.handlers.taba_handler import TabaHandler
from tellapart.taba.taba_event import TABA_EVENT_IDX_TIME
from tellapart.taba.taba_event import TABA_EVENT_IDX_VALUE

class ExpiryStringState(object):
  """Wrapper class for ExpiryStringTaba States.
  """
  def __init__(self, value, expiry):
    self.value = value
    self.expiry = expiry

class ExpiryStringTaba(TabaHandler):
  """Taba Handler that accepts String inputs, with an expiration timestamp.
  """
  CURRENT_VERSION = 1

  def NewState(self, client_id, name):
    """See base class definition.
    """
    return ExpiryStringState('', 0)

  def FoldEvents(self, state, events):
    """See base class definition.
    """
    # Sort the events by time and pick the latest one.
    events = sorted(events, key=lambda e: e[TABA_EVENT_IDX_TIME])
    event = events[-1]

    value = event[TABA_EVENT_IDX_VALUE]

    # Check the expiration date. If it is before current time, then discard
    # the event.
    now = time.time()
    if value[1] > now:
      state.expiry = value[1]
      state.value = str(value[0])

    return state

  def ProjectState(self, state):
    """See base class definition.
    """
    now = time.time()

    out = {'vals': []}
    if state.expiry > now:
      out['vals'] = [str(state.value)]

    return out

  def Aggregate(self, projections):
    """See base class definition.
    """
    aggregate = {'vals': [v for p in projections for v in p['vals']]}
    return aggregate

  def Render(self, name, projections):
    """See base class definition.
    """
    # Build a map of string value to their frequency.
    val_count_map = defaultdict(int)
    for projection in projections:
      for val in projection['vals']:
        val_count_map[val] += 1

    str_val = ', '.join([
        '(string: %s, frequency: %d)' % (k, v)
        for k, v in val_count_map.iteritems()])

    rendered = '%s\t{%s}' % (name, str_val)

    return [rendered]

  def Upgrade(self, state, version):
    """See base class definition.
    """
    if version == 1:
      return state

    else:
      raise ValueError('Unsupported state version %d' % version)

  def ShouldPrune(self, state):
    """See base class definition.
    """
    return time.time() > state.expiry
