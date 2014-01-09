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

"""Handler that keeps a buffer of values with expiry times.
"""

import itertools
import time

import cjson

from taba.handlers.tab_handler import TabHandler

# Limit for the number of events to buffer in a single state.
MAX_ELEMENTS = 1000

# Indexes into state elements.
STATE_IDX_VALUE = 0
STATE_IDX_TIME = 1
STATE_IDX_EXPIRY = 2

class Buffer(TabHandler):
  """Tab Handler that buffers values with expiry times.
  """
  CURRENT_VERSION = 0

  def NewState(self, client_id, name):
    """See base class definition."""
    return []

  def FoldEvents(self, state, events):
    """See base class definition."""
    now = time.time()

    for event in events:
      val, expiry = event.payload
      tm = event.timestamp
      if expiry > now:
        state.append((val, tm, expiry))

    state = _FilterState(state, now)
    return state

  def Reduce(self, states):
    """See base class definition."""
    state = itertools.chain(*states)
    state = _FilterState(state)
    return state

  def Render(self, state, accept):
    """See base class definition."""
    return cjson.encode([(val, tm) for val, tm, _ in state])

  def Upgrade(self, state, version):
    """See base class definition."""
    if version == 0:
      return state
    else:
      raise ValueError('Unknown version %s' % version)

  def ShouldPrune(self, state):
    """See base class definition."""
    state = _FilterState(state)
    return (len(state) == 0)

def _FilterState(state, now=None):
  """Remove expired items from a State, and truncate to the max allowed length.

  Args:
    state - State object.
    now - Optional UTC timestamp.

  Returns:
    Filtered State object.
  """
  if now is None:
    now = time.time()

  state = itertools.ifilter(lambda e: e[STATE_IDX_EXPIRY] > now, state)
  state = sorted(state, key=lambda e: e[STATE_IDX_EXPIRY], reverse=True)
  state = state[:MAX_ELEMENTS]

  return state
