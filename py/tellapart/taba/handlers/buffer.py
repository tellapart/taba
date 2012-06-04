# Copyright 2012 TellApart, Inc.
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

"""
Taba Handler that keeps a buffer of values with expiry times.
"""

import time

import cjson

from tellapart.taba.handlers.taba_handler import TabaHandler
from tellapart.taba.taba_event import TABA_EVENT_IDX_TIME
from tellapart.taba.taba_event import TABA_EVENT_IDX_VALUE

# Limit for the number of events to buffer in a single state.
MAX_ELEMENTS = 1000

class BufferTaba(TabaHandler):
  """Taba Handler that buffers values with expiry times.
  """
  CURRENT_VERSION = 1

  def NewState(self, client_id, name):
    """See base class definition.
    """
    return []

  def FoldEvents(self, state, events):
    """See base class definition.
    """
    # Prune expired values.
    now = time.time()
    out_elements = [
        (val, tm, expiry) for val, tm, expiry in state if expiry > now]

    # Add the new elements.
    for event in events:
      val, expiry = event[TABA_EVENT_IDX_VALUE]
      tm = event[TABA_EVENT_IDX_TIME]
      if expiry > now:
        out_elements.append((val, tm, expiry))

    out_elements = sorted(out_elements, key=lambda e: e[2], reverse=True)
    out_elements = out_elements[:MAX_ELEMENTS]

    return out_elements

  def ProjectState(self, state):
    """See base class definition.
    """
    # Remove the expired items and return the rest.
    now = time.time()
    output = [(val, tm) for val, tm, expiry in state if expiry > now]

    return {'vals': output}

  def Aggregate(self, projections):
    """See base class definition.
    """
    # Flatten all the projections into a single list.
    return {'vals': [(v, t) for p in projections for v, t in p['vals']]}

  def Render(self, name, projections):
    """See base class definition.
    """
    reduction = [(v, t) for p in projections for v, t in p['vals']]
    return ['%s\t%s' % (name, repr(reduction))]

  def Upgrade(self, state, version):
    """See base class definition.
    """
    if version == 0:
      return cjson.decode(state)

    elif version == 1:
      return state

    else:
      raise ValueError('Unsupported state version %d' % version)

  def ShouldPrune(self, state):
    """See base class definition.
    """
    # If any element is still active, don't prune.
    now = time.time()
    for _, _, expiry in state:
      if expiry > now:
        return False

    return True
