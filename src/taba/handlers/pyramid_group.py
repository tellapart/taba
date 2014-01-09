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

"""CounterGroup type handler that combines a PyramidCounter with a
PercentileCounter.
"""

import cjson

from taba.handlers.counter_group import CounterGroup
from taba.handlers.dynamic_percentile_counter import DynamicPercentileCounter
from taba.handlers.pyramid_counter import PyramidCounter
from taba.handlers.tab_type import RenderMode

PYRAMID_COUNTER = PyramidCounter()
PYRAMID_INTERVALS = [
  (1, 60),   # 1sec   x 60  => 1 minute
  (10, 60),  # 10sec  x 60  => 10 minute
  (30, 120), # 30sec  x 120 => 1 hour
  (900, 96)  # 900sec x 96  => 1 day
]
PYRAMID_LABELS = ['1m', '10m', '1h', '1d']

# Monkey-patch the ShouldPrune function of the percentile counter so that it
# doesn't prevent the group from being pruned. The state held in the
# MovingIntervalCounter's will determine whether or not to prune the group.
DYNAMIC_PERCENTILE_COUNTER = DynamicPercentileCounter()
DYNAMIC_PERCENTILE_COUNTER.ShouldPrune = lambda s: True

class PyramidGroup(CounterGroup):
  """CounterGroup Handler that tracks moving intervals over 4 periods (1m, 10m,
  1h, 1d), and a percentile counter.
  """
  CURRENT_VERSION = 0

  COUNTERS = [PYRAMID_COUNTER, DYNAMIC_PERCENTILE_COUNTER]

  def __init__(self):
    self._server = None

  @property
  def server(self):
    return self._server

  @server.setter
  def server(self, value):
    self._server = value
    PYRAMID_COUNTER.server = value
    DYNAMIC_PERCENTILE_COUNTER.server = value

  def NewState(self, client_id, name):
    states = [
        PYRAMID_COUNTER.NewState(client_id, name, PYRAMID_INTERVALS),
        DYNAMIC_PERCENTILE_COUNTER.NewState(client_id, name, 200),]

    return CounterGroup.NewState(self, client_id, name, states)

  def Render(self, group_state, accept):
    """Custom group render function to help translate the PyramidCounter
    intervals.
    """
    pyr_state, pct_state = self._Unpack(group_state)

    pct_render = DYNAMIC_PERCENTILE_COUNTER.Render(pct_state.payload, accept)

    if accept == RenderMode.DETAIL:
      return cjson.encode(PYRAMID_COUNTER.Render(pyr_state.payload, accept))

    else:
      pyr_data = PYRAMID_COUNTER.Render(
          pyr_state.payload, RenderMode.PYTHON_OBJ)

      pyr_str = ', '.join([
          '"%s": {"count": %d, "total": %.2f, "average": %.2f}' % (
              l, d[0], d[1], d[1] / d[0] if d[0] else 0)
          for l, d in zip(PYRAMID_LABELS, pyr_data)])

      return '{%s, "pct": %s}' % (pyr_str, pct_render)

  def Upgrade(self, group_state, version):
    """See base class definition."""
    if version == 0:
      return group_state
    else:
      raise ValueError('Unknown version %s' % version)
