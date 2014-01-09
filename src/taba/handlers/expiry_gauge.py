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

"""Tab Handler that accepts String inputs, and tracks the latest value set,
with an expiration timestamp.
"""

from collections import defaultdict
import time

import cjson

from taba.handlers.tab_handler import TabHandler

# Indexes into ExpiryGauge state elements.
STATE_IDX_VALUE = 0
STATE_IDX_EXPIRY = 1

class ExpiryGauge(TabHandler):
  """Tab Handler that accepts String inputs, with an expiration timestamp.
  """
  CURRENT_VERSION = 0

  def NewState(self, client_id, name):
    """See base class definition."""
    return {client_id: None}

  def FoldEvents(self, state, events):
    """See base class definition."""
    max_event = None

    # Pick the most recent event.
    for event in events:
      if max_event is None:
        max_event = event
      elif max_event.timestamp < event.timestamp:
        max_event = event

    element = max_event.payload

    # Check the expiration date. If it is before current time, then discard
    # the event.
    now = time.time()
    if element[STATE_IDX_EXPIRY] > now:
      for client in state:
        state[client] = element

    return state

  def Reduce(self, states):
    """See base class definition."""
    now = time.time()

    reduced = {}
    for state in states:
      for client, entry in state.iteritems():
        if not entry:
          continue
        expiry = entry[STATE_IDX_EXPIRY]

        if expiry > now and (
            client not in reduced or
            reduced[client][STATE_IDX_EXPIRY] < expiry):
          reduced[client] = entry

    return reduced

  def Render(self, state, accept):
    """See base class definition."""
    now = time.time()

    value_to_count = defaultdict(int)
    for entry in state.itervalues():
      if entry[STATE_IDX_EXPIRY] > now:
        value_to_count[str(entry[STATE_IDX_VALUE])] += 1

    return cjson.encode(value_to_count)

  def Upgrade(self, state, version):
    """See base class definition."""
    if version == 0:
      return state
    else:
      raise ValueError('Unknown version %s' % version)

  def ShouldPrune(self, state):
    """See base class definition."""
    now = time.time()
    for _, expiry in state.values():
      if expiry > now:
        return False
    return True
