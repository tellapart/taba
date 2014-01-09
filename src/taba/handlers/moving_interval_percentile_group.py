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

"""CounterGroup Handler that tracks moving interval percentiles over 4 periods
(1m, 10m, 1h, 1d).
"""

from taba.handlers.counter_group import CounterGroup
from taba.handlers.moving_interval_percentile import MovingIntervalPercentile

MOVING_INTERVAL_PERCENTILE = MovingIntervalPercentile()

class MovingIntervalPercentileGroup(CounterGroup):
  """CounterGroup Handler that tracks moving interval percentiles over 4
  periods (1m, 10m, 1h, 1d).
  """
  CURRENT_VERSION = 0

  COUNTERS = [
      MOVING_INTERVAL_PERCENTILE,
      MOVING_INTERVAL_PERCENTILE,
      MOVING_INTERVAL_PERCENTILE,
      MOVING_INTERVAL_PERCENTILE]

  LABELS = ['1m', '10m', '1h', '1d']

  def __init__(self):
    self._server = None

  @property
  def server(self):
    return self._server

  @server.setter
  def server(self, value):
    self._server = value
    MOVING_INTERVAL_PERCENTILE.server = value

  def NewState(self, client_id, name):
    # 1m @ 15s, 10m @ 2.5m, 1h @ 15m, 1d @ 6h
    states = [
        MOVING_INTERVAL_PERCENTILE.NewState(client_id, name, 15, 4, 64),
        MOVING_INTERVAL_PERCENTILE.NewState(client_id, name, 150, 4, 128),
        MOVING_INTERVAL_PERCENTILE.NewState(client_id, name, 900, 4, 128),
        MOVING_INTERVAL_PERCENTILE.NewState(client_id, name, 21600, 4, 128), ]
    return CounterGroup.NewState(self, client_id, name, states)

  def Upgrade(self, group_state, version):
    """See base class definition."""
    if version == 0:
      return group_state
    else:
      raise ValueError('Unknown version %s' % version)
