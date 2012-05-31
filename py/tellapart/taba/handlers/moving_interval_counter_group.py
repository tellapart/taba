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

CounterGroup Handler that tracks moving intervals over 4 periods (1m, 10m,
1h, 1d), and a percentile counter.
"""

from tellapart.taba.handlers.counter_group import CounterGroup
from tellapart.taba.handlers.moving_interval_counter import \
    MovingIntervalCounter
from tellapart.taba.handlers.percentile_counter import PercentileCounter

MOVING_INTERVAL_COUNTER = MovingIntervalCounter()
PERCENTILE_COUNTER = PercentileCounter()

# Monkey-patch the ShouldPrune function of the percentile counter so that it
# doesn't prevent the group from being pruned. The state held in the
# MovingIntervalCounter's will determine whether or not to prune the group.
PERCENTILE_COUNTER.ShouldPrune = lambda s: True

class MovingIntervalCounterGroup(CounterGroup):
  """CounterGroup Handler that tracks moving intervals over 4 periods (1m, 10m,
  1h, 1d), and a percentile counter.
  """

  COUNTERS = [
      MOVING_INTERVAL_COUNTER,
      MOVING_INTERVAL_COUNTER,
      MOVING_INTERVAL_COUNTER,
      MOVING_INTERVAL_COUNTER,
      PERCENTILE_COUNTER]

  LABELS = ['1m', '10m', '1h', '1d', 'pct']

  def __init__(self):
    self._server = None

  @property
  def server(self):
    return self._server

  @server.setter
  def server(self, value):
    self._server = value
    MOVING_INTERVAL_COUNTER.server = value
    PERCENTILE_COUNTER.server = value

  def NewState(self, client_id, name):
    states = [
        MOVING_INTERVAL_COUNTER.NewState(client_id, name, 60, 60), # 1m @ 1s
        MOVING_INTERVAL_COUNTER.NewState(client_id, name, 600, 60), # 10m @ 10s
        MOVING_INTERVAL_COUNTER.NewState(client_id, name, 3600, 120), # 1h @ 30s
        MOVING_INTERVAL_COUNTER.NewState(client_id, name, 86400, 288), # 1d @ 5m
        PERCENTILE_COUNTER.NewState(client_id, name),]

    return CounterGroup.NewState(self, client_id, name, states)
