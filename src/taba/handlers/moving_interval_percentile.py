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

"""Tab Handler that accepts numeric values, and tracks the count, total, and
percentile of values within a specified window (looking back from the present).
Relies on the DynamicPercentileCounter and its Cython native implementation.
"""

import math
import time

from taba.handlers import dynamic_percentile_counter_native   #@UnresolvedImport
from taba.handlers.dynamic_percentile_counter import DynamicPercentileCounter
from taba.handlers.tab_handler import TabHandler
from taba.handlers.tab_type import RenderMode

DYNAMIC_PERCENTILE_COUNTER = DynamicPercentileCounter()

class MovingIntervalPercentileState(object):
  """Container class for the State of the MovingIntervalPercentile Handler
  """

  __slots__ = [
      'start_time', 'bucket_width', 'num_buckets', 'samples', 'buckets']

  def __init__(self, start_time, bucket_width, num_buckets, samples, buckets):
    """
    Args:
      start_time - Epoch time of the start of the oldest buckets.
      bucket_width - Time slice width of each bucket, in seconds.
      num_buckets - The number of buckets to use.
      samples - The number of samples to keep per bucket.
      buckets - Array of sub-states (StateStruct objects, with
          DynamicPercentileCounter State objects as payload).
    """
    self.start_time = start_time
    self.bucket_width = bucket_width
    self.num_buckets = num_buckets
    self.samples = samples
    self.buckets = buckets

  @property
  def end_time(self):
    """
    Returns:
      Epoch time of the end of the last bucket.
    """
    return self.start_time + (self.num_buckets * self.bucket_width)

class MovingIntervalPercentile(TabHandler):
  """Tab Handler that accepts numeric values, and tracks the count, total, and
  percentile of values within a specified window (looking back from the
  present).  Relies on the DynamicPercentileCounter and its Cython native
  implementation.
  """
  CURRENT_VERSION = 0

  def NewState(self, client_id, name, bucket_width, num_buckets=5, samples=128):
    """See base class definition. (override)

    Args (new):
      bucket_width - Time slice width, in seconds, of each bucket.
          NOTE: It is _very strongly_ recommended that this be an integer,
          otherwise floating point errors will accumulate over time.
      num_buckets - Number of time slice buckets to use.
      samples - Number of samples values to keep per bucket.
    """
    from taba.server.model.state_manager import StateStruct

    now_floor = int(time.time() / bucket_width) * bucket_width
    start = now_floor - (bucket_width * (num_buckets - 1))

    buckets = [
        StateStruct(
            DYNAMIC_PERCENTILE_COUNTER.NewState(client_id, name, samples),
            DynamicPercentileCounter.CURRENT_VERSION,
            None)
        for _ in xrange(num_buckets)]

    return MovingIntervalPercentileState(
        start, bucket_width, num_buckets, samples, buckets)

  def FoldEvents(self, state, events):
    """See base class definition. (override)"""
    # Roll the buckets in the State to the current time. Discount the current
    # time by the cluster latency, so that short time-span buckets aren't
    # perpetually empty. However, make sure not to discount farther back than
    # the latest incoming event.
    now = time.time()
    effective_now = now - self.server.GetGroupLatency()
    effective_now = max(effective_now, max(e.timestamp for e in events))

    state = _RotateAndUpgradeIfNecessary(state, effective_now)

    # Group events by the bucket they belong in.
    events_by_bucket = [[] for _ in xrange(state.num_buckets)]
    for event in events:
      event_bucket = int(
          (event.timestamp - state.start_time) / state.bucket_width)
      if 0 <= event_bucket < state.num_buckets:
        events_by_bucket[event_bucket].append(event)

    # Apply each group of events to the appropriate state.
    for i, events in enumerate(events_by_bucket):
      state.buckets[i].payload = DYNAMIC_PERCENTILE_COUNTER.FoldEvents(
          state.buckets[i].payload,
          events)

    return state

  def Reduce(self, states):
    """See base class definition. (override)"""
    # Assert that the states to reduce have the same bucket specifications.
    for state in states[1:]:
      if (state.num_buckets != states[0].num_buckets or
          state.bucket_width != states[0].bucket_width):
        raise ValueError('Cannot reduce heterogeneous state specifications.')

    # Roll the buckets in the States to the current time. Discount the current
    # time by the cluster latency, so that short time-span buckets aren't
    # perpetually empty. If any of the States are already rolled past the
    # discounted time, use the most recent State as the baseline.
    now = time.time()
    effective_now = max(
        now - self.server.GetGroupLatency(),
        max(s.end_time for s in states))

    states = [
        _RotateAndUpgradeIfNecessary(state_buffer, effective_now)
        for state_buffer in states]

    # Reduce the states for each bucket together.
    for i in xrange(states[0].num_buckets):
      pct_states = [state.buckets[i].payload for state in states]
      states[0].buckets[i].payload = DYNAMIC_PERCENTILE_COUNTER.Reduce(pct_states)

    return states[0]

  def Render(self, state, accept):
    """See base class definition. (override)"""
    effective_now = time.time() - self.server.GetGroupLatency()
    state = _RotateAndUpgradeIfNecessary(state, effective_now)

    pct_states = [bucket.payload for bucket in state.buckets]
    combined_state = DYNAMIC_PERCENTILE_COUNTER.Reduce(pct_states)

    count, total, percentiles = \
        dynamic_percentile_counter_native.Render(combined_state)

    if accept == RenderMode.PYTHON_OBJ:
      return (count, total, percentiles)

    else:
      pct_str = ', '.join(['%.2f' % v for v in percentiles])
      return '{"count": %d, "total": %f, "pct": [%s]}' % (count, total, pct_str)

  def Upgrade(self, state, version):
    """See base class definition. (override)"""
    if version == 0:
      return state
    else:
      raise ValueError('Unknown version %s' % version)

  def ShouldPrune(self, state):
    """See base class definition. (override)"""
    count, total, _ = self.Render(state, RenderMode.PYTHON_OBJ)
    return count == 0 and total == 0

def _RotateAndUpgradeIfNecessary(state, effective_now):
  """Given a MovingIntervalPercentileState, upgrade all the sub-states, and
  rotate the buckets to the (effective) current time, if necessary.

  Args:
    state - MovingIntervalPercentileState object.
    effective_now - The effective current epoch time.

  Returns:
    Updated MovingIntervalPercentileState object.
  """
  from taba.server.model.state_manager import StateStruct

  # Upgrade the sub-states.
  state.buckets = [
      StateStruct(
          DYNAMIC_PERCENTILE_COUNTER.Upgrade(
              state_struct.payload,
              state_struct.version),
          DYNAMIC_PERCENTILE_COUNTER.CURRENT_VERSION,
          None)
      for state_struct in state.buckets]

  # Calculate whether bucket rotation is necessary.
  to_rotate = int(math.ceil(
      (effective_now - state.end_time) / state.bucket_width))

  if to_rotate > 0:
    new_buckets = [
        StateStruct(
            DYNAMIC_PERCENTILE_COUNTER.NewState('', '', state.samples),
            DynamicPercentileCounter.CURRENT_VERSION,
            None)
        for _ in xrange(min(to_rotate, state.num_buckets))]

    state.buckets = state.buckets[to_rotate:] + new_buckets
    state.start_time = state.start_time + state.bucket_width * to_rotate

    if len(state.buckets) > state.num_buckets:
      state.buckets = state.buckets[:state.num_buckets]

  return state
