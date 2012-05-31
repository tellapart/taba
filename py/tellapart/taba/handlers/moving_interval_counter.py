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

Taba Handler that accepts numeric values, and tracks the count and total of
values within a specified window (looking back from the present).
"""

import time

from tellapart.taba.handlers.taba_handler import TabaHandler
from tellapart.taba.taba_event import TABA_EVENT_IDX_VALUE, TABA_EVENT_IDX_TIME

from tellapart.taba.handlers.moving_interval_counter_native import \
    MakeMicStateBuffer, MovingIntervalCounterState

class MovingIntervalCounter(TabaHandler):
  """Taba Handler that accepts numeric values, and tracks the count and total of
  values within a specified window (looking back from the present).
  """
  CURRENT_VERSION = 1

  def NewState(self, client_id, name, interval_secs=60, num_buckets=1000):
    """See base class definition.
    """
    return MakeMicStateBuffer(interval_secs, num_buckets, time.time())

  def FoldEvents(self, state_ser, events):
    """See base class definition.
    """
    state = MovingIntervalCounterState(state_ser)

    effective_now = time.time() - self.server.GetAverageAppliedLatency()
    state.DiscardOldestBuckets(effective_now)

    for event in events:
      state.FoldSingleEvent(
          event[TABA_EVENT_IDX_TIME],
          event[TABA_EVENT_IDX_VALUE][0])

    return state.Serialize()

  def ProjectState(self, state_ser):
    """See base class definition.
    """
    state = MovingIntervalCounterState(state_ser)

    effective_now = time.time() - self.server.GetAverageAppliedLatency()
    state.DiscardOldestBuckets(effective_now)

    (count, value) = state.Project()

    return {
      'count': count,
      'total': value,
      'average': float(value) / count if count != 0 else 0}

  def Aggregate(self, projections):
    """See base class definition.
    """
    count = sum([p['count'] for p in projections])
    total = sum([p['total'] for p in projections])

    return {
        'count': count,
        'total': total,
        'average': float(total) / count if count != 0 else 0}

  def Render(self, name, projections):
    """See base class definition.
    """
    renders = [
        '%s\t%.2f\t%d' % (name, p['total'], p['count'])
        for p in projections]

    return renders

  def Upgrade(self, state_ser, version):
    """See base class definition.
    """
    if version == 0:
      return state_ser

    elif version == 1:
      return state_ser

    else:
      raise ValueError('Unsupported state version %d' % version)

  def ShouldPrune(self, state_ser):
    """See base class definition.
    """
    projection = self.ProjectState(state_ser)
    if projection['count'] == 0 and projection['total'] == 0:
      return True

    return False
