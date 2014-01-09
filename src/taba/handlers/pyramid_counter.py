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

"""Tab Handler that accepts numeric values, and tracks the count and total of
values within several specified windows (looking back from the present). Relies
on a Cython native implementation for serialization/deserialization and
calculations.
"""

import time

import cjson

from taba.handlers.tab_handler import TabHandler
from taba.handlers.tab_type import RenderMode

from taba.handlers.pyramid_counter_native import (
    NewStateBuffer,       #@UnresolvedImport
    PyramidCounterState,  #@UnresolvedImport
    SynchronizeCounters)  #@UnresolvedImport

class PyramidCounter(TabHandler):
  """Tab Handler that accepts numeric values, and tracks the count and total
  of values within several specified windows (looking back from the present).
  """
  CURRENT_VERSION = 0

  def NewState(self, client_id, name, interval_specs):
    """See base class definition.

    Args:
      interval_specs - List of Tuples of (Bucket Width Seconds, Bucket Count).
          Each element in the list corresponds to an interval to track. The
          Bucket Widths and Bucket Counts _must_ be integers.
    """
    return NewStateBuffer(
        interval_specs,
        time.time())

  def FoldEvents(self, state_buffer, events):
    """See base class definition."""
    state = PyramidCounterState(state_buffer)

    # Roll the buckets in the State to the current time. Discount the current
    # time by the cluster latency, so that short time-span buckets aren't
    # perpetually empty. However, make sure not to discount farther back than
    # the latest incoming event.
    now = time.time()
    effective_now = now - self.server.GetGroupLatency()
    effective_now = max(effective_now, max(e.timestamp for e in events))
    state.RotateBucketsIfNeeded(effective_now)

    # Fold the events in the the prepared state.
    state.FoldEvents(events)

    return state.Serialize()

  def Reduce(self, state_buffers):
    """See base class definition."""
    if len(state_buffers) == 0:
      return None
    elif len(state_buffers) == 1:
      return state_buffers[0]

    states = [PyramidCounterState(b) for b in state_buffers]

    # Roll the buckets in all the States to the the same current time. Discount
    # the current time by the cluster latency, so that short time-span buckets
    # aren't perpetually empty. The synchronization will also check whether any
    # of the states is already rolled past the effective time, and will use the
    # greatest value found.
    effective_now = time.time() - self.server.GetGroupLatency()
    SynchronizeCounters(states, effective_now)

    # Reduce the States together.
    base = states[0]
    base.Reduce(states[1:])

    return base.Serialize()

  def Render(self, state_buffer, accept):
    """See base class definition."""
    state = PyramidCounterState(state_buffer)
    state.RotateBucketsIfNeeded(time.time() - self.server.GetGroupLatency())

    all_data = state.Render()

    if accept == RenderMode.DETAIL:
      return all_data

    else:
      data = []
      for interval in all_data:
        count = sum([e[1] for e in interval])
        total = sum([e[2] for e in interval])
        data.append((count, total))

      if accept == RenderMode.PYTHON_OBJ:
        return data
      else:
        return cjson.encode(data)

  def Upgrade(self, state_buffer, version):
    """See base class definition."""
    if version == 0:
      return state_buffer
    else:
      raise ValueError('Unknown version %s' % version)

  def ShouldPrune(self, state_buffer):
    """See base class definition."""
    state = PyramidCounterState(state_buffer)
    state.RotateBucketsIfNeeded(time.time() - self.server.GetGroupLatency())

    data = state.Render()
    count = sum([e[1] for interval in data for e in interval])
    return (count == 0)
